# pa_server 技术说明书

## 基于 io_uring 的主从 Proactor 高并发 TCP 服务器

---

## 目录

1. [架构概览](#1-架构概览)
2. [核心枚举与类型定义](#2-核心枚举与类型定义)
3. [类详解](#3-类详解)
   - [netAddr](#31-netaddr)
   - [Socket](#32-socket)
   - [Buffer](#33-buffer)
   - [Any](#34-any)
   - [Connection](#35-connection)
   - [Proactor](#36-proactor)
   - [SlavePool](#37-slavepool)
   - [TcpServer](#38-tcpserver)
4. [类协作关系](#4-类协作关系)
5. [完整数据流](#5-完整数据流)
6. [与 like-muduo 原版的对照](#6-与-like-muduo-原版的对照)

---

## 1. 架构概览

### 1.1 并发模型：主从 Proactor

```
┌─────────────────────────────────────────────────────────┐
│                    TcpServer                             │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Master Proactor (主线程)              │   │
│  │                                                   │   │
│  │  io_uring ─┬─ async accept ──► 新连接 fd          │   │
│  │            ├─ timerfd read ──► 时间轮 tick         │   │
│  │            └─ eventfd read ──► 跨线程唤醒          │   │
│  │                                                   │   │
│  │  全局连接表 (_connections)                         │   │
│  │  时间轮 (_timerWheel)                              │   │
│  └──────────────┬───────────────────────────────────┘   │
│                 │ 轮询分配新连接                          │
│                 ▼                                        │
│  ┌──────────────────────────────────────────────────┐   │
│  │                  SlavePool                        │   │
│  │                                                   │   │
│  │  ┌──────────┐  ┌──────────┐       ┌──────────┐   │   │
│  │  │ Slave[0] │  │ Slave[1] │  ...  │ Slave[N] │   │   │
│  │  │ io_uring │  │ io_uring │       │ io_uring │   │   │
│  │  │ 线程 #1  │  │ 线程 #2  │       │ 线程 #N  │   │   │
│  │  │          │  │          │       │          │   │   │
│  │  │ recv/send│  │ recv/send│       │ recv/send│   │   │
│  │  │ close    │  │ close    │       │ close    │   │   │
│  │  └──────────┘  └──────────┘       └──────────┘   │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### 1.2 与 epoll Reactor 的本质区别

| 维度 | epoll Reactor (原版) | io_uring Proactor (新版) |
|------|---------------------|--------------------------|
| **I/O 模式** | 同步非阻塞：epoll 通知 → 用户态执行 recv/send | 真异步：提交 SQE → 内核完成 → 收割 CQE |
| **事件注册** | 持久注册（EPOLL_CTL_ADD 一次，持续触发） | 一次性提交（每个 SQE 对应一次操作，完成后重新提交） |
| **数据就绪** | epoll_wait 返回"fd 可读/可写" | CQE.res 直接包含已读/已写的字节数 |
| **唤醒机制** | eventfd + epoll | eventfd + io_uring prep_read |
| **缓冲区** | 收到通知后同步读写 | 提交 SQE 时指定缓冲区，CQE 到达时数据已在缓冲区 |

---

## 2. 核心枚举与类型定义

### 2.1 `OpType` — io_uring 操作类型标签

```cpp
enum class OpType : uint64_t {
    NONE    = 0,
    ACCEPT  = 1,   // Master: 异步 accept
    RECV    = 2,   // Slave:  异步接收
    SEND    = 3,   // Slave:  异步发送
    CLOSE   = 4,   // Slave:  异步关闭 fd
    EVENTFD = 5,   // 跨线程唤醒
    TIMER   = 6,   // Master: 时间轮 tick
    TIMEOUT = 7,   // 预留：io_uring 原生超时
};
```

**用途**：编码到每个 io_uring SQE 的 `user_data` 字段中，收割 CQE 时解码以确定"这个完成事件属于哪种操作"。

**编码方式**：
```
user_data (64位) = (connId << 8) | (uint8_t)OpType
                   ────────────   ──────────────
                   高56位：连接ID   低8位：操作类型
```

- `connId = 0`：内部操作（ACCEPT、EVENTFD、TIMER），不属于具体连接
- `connId >= 1`：用户连接，收割时通过此 ID 查找对应 `Connection` 对象

**辅助函数**：

| 函数 | 作用 |
|------|------|
| `MakeUserData(connId, op)` | 将 connId 和 OpType 编码为 64 位整数 |
| `GetConnId(data)` | 从 64 位整数中提取 connId（`data >> 8`） |
| `GetOpType(data)` | 从 64 位整数中提取 OpType（`data & 0xFF`） |
| `ToUserPtr(data)` | `uint64_t → void*`（liburing 2.1 的 user_data 是 void*） |
| `FromUserPtr(ptr)` | `void* → uint64_t`（反向转换） |

### 2.2 `ConnStat` — 连接状态机

```cpp
enum class ConnStat : int {
    CONNECTING    = 0,  // 连接刚建立，尚未开始读
    CONNECTED     = 1,  // 正常通信中
    DISCONNECTING = 2,  // 正在关闭（先发完缓冲数据）
    DISCONNECTED  = 3,  // 已断开
};
```

**状态迁移**：
```
CONNECTING ──Established()──► CONNECTED ──Shutdown()──► DISCONNECTING
                                  │                          │
                                  │                          │
                                  └──────Release()───────────┴──► DISCONNECTED
```

---

## 3. 类详解

### 3.1 `netAddr`

> 移植自 `like-muduo/source/inet_addr.hpp`，封装 IPv4 地址。

| 字段 | 类型 | 作用 |
|------|------|------|
| `_addr` | `struct sockaddr_in` | 内核使用的 socket 地址结构体 |
| `_ip` | `std::string` | 点分十进制 IP 字符串（如 "192.168.1.1"） |
| `_port` | `uint16_t` | 主机字节序端口号 |

| 关键方法 | 作用 |
|----------|------|
| `operator&()` | 返回 `sockaddr*`，直接传给 `bind()`/`connect()` |
| `size()` | 返回 `sizeof(sockaddr_in)`，配合 `operator&()` 使用 |
| `name()` | 返回 `"ip:port"` 格式的可读字符串 |

**协作关系**：被 `Socket::Bind()` 使用，创建监听地址。

---

### 3.2 `Socket`

> 移植自 `like-muduo/source/server.hpp`，封装 TCP socket 文件描述符及其操作。

| 字段 | 类型 | 作用 |
|------|------|------|
| `_fd` | `int` | 文件描述符，-1 表示无效 |

| 关键方法 | 作用 |
|----------|------|
| `Create()` | 调用 `socket()` 创建 TCP socket 并设为非阻塞 |
| `Bind(ip, port)` | 调用 `bind()` 绑定地址 |
| `Listen(backlog)` | 调用 `listen()` 开始监听 |
| `SetNoBlock()` | `fcntl(fd, F_SETFL, O_NONBLOCK)` |
| `ReuseAddress()` | `setsockopt(SO_REUSEADDR \| SO_REUSEPORT)` |
| `BuildServer(port)` | 一站式：Create → ReuseAddress → Bind → Listen |
| `Close()` | 关闭 fd |

**协作关系**：
- `TcpServer` 持有一个 `_listenSock`，通过 `BuildServer()` 初始化
- 在 `SubmitMasterAccept()` 中，`_listenSock.Fd()` 被传给 `io_uring_prep_accept`

---

### 3.3 `Buffer`

> 移植自 `like-muduo/source/server.hpp`，读写分离的字节缓冲区。

| 字段 | 类型 | 作用 |
|------|------|------|
| `_buffer` | `std::vector<char>` | 底层存储，动态扩容 |
| `_read_idx` | `int` | 读指针（已消费数据的末尾） |
| `_write_idx` | `int` | 写指针（已写入数据的末尾） |

**内部布局**：
```
[  已消费  |  可读数据  |   空闲空间   ]
0         _read_idx   _write_idx    Capacity()
           ─────────               ─────────
           PreIdle()               PostIdle()
           ─────────────────
            ReadableSize()
```

| 关键方法 | 作用 |
|----------|------|
| `GetWritePos()` | 返回可写入位置的指针（传给 `io_uring_prep_recv` 的缓冲区） |
| `GetReadPos()` | 返回可读位置的指针（传给 `io_uring_prep_send` 的缓冲区） |
| `MoveWriteOffset(n)` | 推进写指针（recv 完成后调用） |
| `MoveReadOffset(n)` | 推进读指针（send 完成后调用） |
| `WriteAndPush(data, len)` | 先 EnsureWrite 保证空间，再写入并推进写指针 |
| `ReadAndPop(d, len)` | 读取数据并推进读指针 |
| `EnsureWrite(len)` | 保证有 len 字节可写空间（必要时扩容或整理碎片） |

**协作关系**：
- `Connection` 持有 `_in_buffer` 和 `_out_buffer`
- 异步 recv 时，`_in_buffer.GetWritePos()` 作为 io_uring 的接收缓冲区
- 异步 send 时，`_out_buffer.GetReadPos()` 作为 io_uring 的发送缓冲区

---

### 3.4 `Any`

> 移植自 `like-muduo/source/server.hpp`，类型擦除上下文容器。

| 字段 | 类型 | 作用 |
|------|------|------|
| `_dummy` | `holder*` | 指向堆上 placeholder<T> 的基类指针 |

**内部结构**：
- `holder`：抽象基类，提供 `clone()` 和 `type()` 接口
- `placeholder<T>`：模板子类，存储实际值 `T _val`

| 关键方法 | 作用 |
|----------|------|
| `get<T>()` | 安全提取：类型匹配返回 `T*`，否则返回 `nullptr` |
| `swap(other)` | 交换两个 Any 的内容 |
| `operator=(const Any&)` | 拷贝赋值（通过 copy-and-swap 惯用法） |

**协作关系**：`Connection` 持有 `_context` 成员，允许用户在连接上挂载任意类型的业务上下文。

---

### 3.5 `Connection`

> TCP 连接的核心抽象。每个连接归属于一个 Slave Proactor，所有 I/O 操作通过该 Slave 的 io_uring 异步完成。

#### 3.5.1 字段详解

| 字段 | 类型 | 作用 |
|------|------|------|
| `_id` | `uint64_t` | 全局唯一连接 ID（同时复用为 io_uring user_data 中的 connId） |
| `_sockfd` | `int` | 连接的 socket 文件描述符 |
| `_slave` | `Proactor*` | 所属 Slave Proactor（裸指针，生命周期由 SlavePool 保证） |
| `_status` | `ConnStat` | 连接状态机当前状态 |
| `_reading` | `bool` | **是否有在途 recv**。防止对同一 fd 重复提交 recv SQE |
| `_writing` | `bool` | **是否有在途 send**。防止对同一 fd 重复提交 send SQE |
| `_releaseOrNot` | `bool` | 是否启用空闲超时自动释放 |
| `_timeout` | `int` | 空闲超时秒数（传给时间轮） |
| `_in_buffer` | `Buffer` | 接收缓冲区：io_uring recv 的数据直接写入此处 |
| `_out_buffer` | `Buffer` | 发送缓冲区：用户 Send 的数据先写入此处，再提交 send SQE |
| `_context` | `Any` | 用户自定义上下文（协议升级、会话状态等） |

#### 3.5.2 回调字段

| 字段 | 类型签名 | 触发时机 |
|------|---------|---------|
| `_message_cb` | `void(PtrConnection, Buffer*)` | recv 完成后有数据可读 |
| `_connected_cb` | `void(PtrConnection)` | Established 后连接就绪 |
| `_closed_cb` | `void(PtrConnection)` | 连接关闭（用户回调） |
| `_any_cb` | `void(PtrConnection)` | 每次事件后（预留） |
| `_srv_closed_cb` | `void(PtrConnection)` | 连接关闭（服务器内部回调，用于从全局表移除） |

#### 3.5.3 关键方法

**公共接口（线程安全）**：

| 方法 | 调用线程 | 实际执行线程 | 作用 |
|------|---------|-------------|------|
| `Established()` | 任意（Master） | Slave Proactor | accept 完成后初始化连接，提交首次 recv |
| `Send(data, len)` | 任意 | Slave Proactor | 写入发送缓冲区，提交 send SQE |
| `Shutdown()` | 任意 | Slave Proactor | 优雅关闭：先发完 _out_buffer 再释放 |
| `Release()` | 任意 | Slave Proactor | 立即关闭：不等缓冲区，直接提交 close SQE |
| `EnableInactiveRelease(sec)` | 任意 | Slave Proactor | 启用空闲超时 |
| `CancelInactiveRelease()` | 任意 | Slave Proactor | 取消空闲超时 |

**CQE 回调（Slave Proactor 线程中调用）**：

| 方法 | 参数含义 | 后续动作 |
|------|---------|---------|
| `OnRecvComplete(n)` | n>0 已读字节，n=0 对端关闭，n<0 错误码 | 处理数据 → 再次 SubmitRecv（Proactor 模式关键） |
| `OnSendComplete(n)` | n>0 已发字节，n<0 错误码 | 若有剩余数据继续发，否则若 DISCONNECTING 则释放 |
| `OnCloseComplete(n)` | close 返回值 | 从 Slave 连接表移除，触发用户 closed 回调 |

**为什么需要 `_reading` / `_writing` 标志**：

这是 Proactor 模型与 Reactor 模型的关键差异之一。epoll 模式下，`EPOLLIN` 注册一次后持续生效；io_uring 模式下，每次 `io_uring_prep_recv` 是一次性操作，完成后必须重新提交。若不加标志防护，可能对同一 fd 提交多个并发的 recv SQE，导致数据乱序。

---

### 3.6 `Proactor`

> **io_uring 事件循环**。取代原版的 `EventLoop + Epoller + Channel` 三层结构。

#### 3.6.1 字段详解

| 字段 | 类型 | 作用 |
|------|------|------|
| `_ring` | `io_uring` | io_uring 实例（内核共享的 SQ/CQ 环形缓冲区） |
| `_eventFd` | `int` | eventfd 文件描述符（**跨线程唤醒**的关键） |
| `_eventFdBuf` | `uint64_t` | eventfd read 的缓冲区（成员变量保证生命周期覆盖 SQE 在途期） |
| `_running` | `std::atomic<bool>` | 控制 Run() 循环退出 |
| `_tid` | `std::thread::id` | 拥有此 Proactor 的线程 ID |
| `_taskMtx` | `std::mutex` | 保护 `_pendingTasks` 的互斥锁 |
| `_pendingTasks` | `std::vector<Task>` | 跨线程投递的待执行任务 |
| `_cqHandler` | `CQHandler` | CQE 处理回调（Master 设为 TcpServer::OnMasterCQE，Slave 设为连接路由） |
| `_conns` | `ConnMap` | **Slave 专用**：connId → Connection 的映射表（CQE 到达时快速查找连接） |
| `_globalConnId` | `static atomic<uint64_t>` | 全局自增连接 ID 生成器 |

#### 3.6.2 关键方法

**生命周期**：

| 方法 | 作用 |
|------|------|
| `Start()` | 记录当前线程 ID，进入 `Run()` 事件循环（**阻塞**） |
| `Stop()` | 设置 `_running = false`，写 eventfd 唤醒 `Run()` |

**跨线程任务**：

| 方法 | 调用场景 | 工作流程 |
|------|---------|---------|
| `PostTask(task)` | 任意线程调用 | `lock → push → unlock → write(eventfd, 1)` |

**io_uring I/O 提交**（必须在 Proactor 线程中调用）：

| 方法 | 底层调用 | 使用方 |
|------|---------|--------|
| `SubmitAccept(fd, connId)` | `io_uring_prep_accept` | Master |
| `SubmitRecv(fd, buf, len, connId)` | `io_uring_prep_recv` | Slave（Connection） |
| `SubmitSend(fd, buf, len, connId)` | `io_uring_prep_send` | Slave（Connection） |
| `SubmitClose(fd, connId)` | `io_uring_prep_close` | Slave（Connection） |
| `SubmitRead(fd, buf, len, connId, op)` | `io_uring_prep_read` | eventfd + timerfd |

每个 `SubmitXxx` 方法遵循相同模式：
```
1. GetSQE() → 获取一个空闲 SQE
2. io_uring_prep_xxx() → 填充 SQE
3. io_uring_sqe_set_data() → 标记 user_data（含 OpType + connId）
4. io_uring_submit() → 通知内核
```

**`GetSQE()` 的队列满处理**：
```
io_uring_get_sqe(&_ring) 返回 nullptr?
  ├── 否 → 直接返回 SQE
  └── 是 → io_uring_submit() + io_uring_wait_cqe() 收割一个 CQE
           → 重试 io_uring_get_sqe()
```

**CQE 处理**：

`SetCQHandler(h)` 设置的回调在 `Run()` 循环中被调用，但 **`EVENTFD` 类型的 CQE 由 `Run()` 自行处理**（不交给 `_cqHandler`）：

```
Run() 循环收割 CQE:
  if op == EVENTFD:
    ProcessTasks()        // 执行积压的跨线程任务
    SubmitRead(eventfd)   // 重新注册 eventfd（一次性操作）
  else:
    _cqHandler(userData, res)  // 委托给 Master/Slave 回调
```

**连接管理（Slave 专用）**：

| 方法 | 作用 |
|------|------|
| `AddConnection(id, conn)` | 将连接加入 `_conns`（Master 分发时调用） |
| `RemoveConnection(id)` | 从 `_conns` 移除（close 完成后调用） |
| `GetConnection(id)` | 按 ID 查找连接（CQE 处理时调用） |

#### 3.6.3 `Run()` 事件循环

```
1. SubmitRead(eventFd)              ── 注册跨线程唤醒
2. while (_running):
3.     io_uring_wait_cqe(&_ring)    ── 等待至少一个 CQE（阻塞）
4.     io_uring_for_each_cqe:
5.         解码 user_data → OpType + connId
6.         if EVENTFD: ProcessTasks() + 重新 SubmitRead(eventFd)
7.         else:       调用 _cqHandler
8.     io_uring_cq_advance           ── 标记 CQE 已处理
```

**eventfd 作为唤醒机制**：
```
任意线程                    Proactor 线程
  │                            │
  ├─ PostTask(task)            │
  │   ├─ lock → push task      │
  │   └─ write(eventfd, 1) ───► io_uring prep_read 完成
  │                            ├─ Run() 收割 CQE (op=EVENTFD)
  │                            ├─ ProcessTasks() 执行 task
  │                            └─ SubmitRead(eventfd) 重新注册
```

---

### 3.7 `SlavePool`

> **从 Proactor 线程池**。取代原版的 `LoopThreadPool`。

| 字段 | 类型 | 作用 |
|------|------|------|
| `_count` | `int` | Slave 线程数量 |
| `_nextIdx` | `int` | 轮询索引（Next() 方法使用） |
| `_mtx` | `std::mutex` | 保护 `_nextIdx`（多线程访问） |
| `_slaves` | `std::vector<Proactor*>` | Proactor 实例数组（堆分配） |
| `_threads` | `std::vector<std::thread>` | 工作线程数组 |

**每个 Slave 线程的启动过程**：
```
SlavePool::Start()
  for i in 0.._count:
    创建线程，在线程函数中:
      1. slave = _slaves[i]
      2. slave->SetCQHandler(lambda):  设置 CQE 路由回调
         - 解码 OpType
         - slave->GetConnection(cid) 查找 Connection
         - 路由到 conn->OnRecvComplete / OnSendComplete / OnCloseComplete
      3. slave->Start()  ← 进入 Proactor::Run() 事件循环（阻塞）
```

**轮询分配**：
```
Next():
  lock → idx = _nextIdx → _nextIdx = (idx + 1) % _count → unlock
  return _slaves[idx]
```

---

### 3.8 `TcpServer`

> **服务器主控类**。持有 Master Proactor、SlavePool、监听 socket、时间轮和全局连接表。

#### 3.8.1 字段详解

| 字段 | 类型 | 作用 |
|------|------|------|
| `_port` | `uint16_t` | 监听端口 |
| `_nextId` | `uint64_t` | 自增 ID（供 RunAfter 任务使用；连接 ID 由 Proactor::NextConnId 生成） |
| `_timeout` | `int` | 空闲超时秒数 |
| `_releaseOrNot` | `bool` | 是否启用空闲超时 |
| `_listenSock` | `Socket` | 监听 socket（BuildServer 初始化） |
| `_master` | `Proactor` | **Master Proactor**（主线程运行，处理 accept + timer） |
| `_pool` | `SlavePool*` | Slave 线程池（堆分配，默认 4 线程） |

**时间轮字段**（仿照原版 TimeWheel）：

| 字段 | 类型 | 作用 |
|------|------|------|
| `_timerFd` | `int` | timerfd 文件描述符（每秒触发一次） |
| `_timerTick` | `int` | 当前指针位置（0 ~ 59 循环） |
| `TIMER_CAPACITY` | `static const int` | 时间轮容量 = 60（每格 1 秒，共 60 秒） |
| `_timerWheel` | `vector<vector<PtrConnection>>` | 时间轮格子，`_timerWheel[slot]` 存放该秒到期的连接 |
| `_connTimerSlot` | `unordered_map<uint64_t, int>` | connId → 剩余超时秒数 |

**延迟任务字段**（RunAfter）：

| 字段 | 类型 | 作用 |
|------|------|------|
| `_delayTasks` | `unordered_map<uint64_t, Functor>` | taskId → 待执行的回调 |
| `_delayCountdown` | `unordered_map<uint64_t, int>` | taskId → 剩余倒计时秒数 |

**全局连接表**：

| 字段 | 类型 | 作用 |
|------|------|------|
| `_connections` | `ConnMap` | connId → shared_ptr<Connection>，保证所有连接的**生命周期** |

**用户回调**：

| 字段 | 触发时机 |
|------|---------|
| `_message_cb` | 连接上有新数据到达 |
| `_connected_cb` | 新连接建立完成 |
| `_closed_cb` | 连接关闭 |
| `_any_cb` | 每次事件（预留扩展） |

#### 3.8.2 关键方法

| 方法 | 作用 |
|------|------|
| `Start()` | `SlavePool::Start()` → 提交初始 accept/timer SQE → `_master.Start()` 进入主事件循环 |
| `OnMasterCQE(data, res)` | Master 的 CQE 分发器：ACCEPT → HandleAccept，TIMER → HandleTimer |
| `HandleAccept(newFd)` | 新 fd → fcntl(O_NONBLOCK) → NewConnection → SubmitMasterAccept（重新提交） |
| `HandleTimer(times)` | 推进时间轮 → 释放空闲超时连接 → 执行 RunAfter 任务 → ProcessTasks |
| `NewConnection(fd)` | 创建 Connection → 注入回调 → 加入全局表+Slave表 → PostTask 启动 I/O |
| `RemoveConnection(conn)` | PostTask 到 Master 线程 → RemoveConnectionInMaster（从全局表擦除） |
| `RunAfter(task, delay)` | 将任务和延迟秒数注册到 `_delayTasks` / `_delayCountdown` |

---

## 4. 类协作关系

### 4.1 所有权关系

```
TcpServer (所有者)
  ├── _listenSock : Socket
  ├── _master : Proactor (Master)
  ├── _pool : SlavePool* (所有者)
  │     └── _slaves[i] : Proactor* (Slave)
  │           └── _conns : ConnMap (shared_ptr<Connection>)
  └── _connections : ConnMap (shared_ptr<Connection>)
```

**生命周期保证**：
- `Connection` 通过 `shared_ptr` 被 **双重持有**：`TcpServer::_connections`（全局表）+ `Proactor::_conns`（Slave 本地表）
- 只有当连接完全关闭（`OnCloseComplete` 移除 Slave 表项 + `RemoveConnectionInMaster` 移除全局表项）后，`shared_ptr` 引用计数归零，`Connection` 析构

### 4.2 关键协作链路

#### 链路 A：接受新连接

```
TcpServer::Start()
  └─ SubmitMasterAccept()
       └─ _master.SubmitAccept(_listenSock.Fd(), 0)
            └─ io_uring_prep_accept + io_uring_submit

Master Proactor::Run()
  └─ io_uring_wait_cqe → CQE (op=ACCEPT, res=新fd)
       └─ _cqHandler(userData, res) → TcpServer::OnMasterCQE
            └─ HandleAccept(newFd)
                 ├─ fcntl(newFd, O_NONBLOCK)
                 ├─ NewConnection(newFd)
                 │    ├─ id = Proactor::NextConnId()
                 │    ├─ slave = _pool->Next()
                 │    ├─ conn = make_shared<Connection>(slave, id, fd)
                 │    ├─ 注入回调（message/closed/connected/srv_closed）
                 │    ├─ _connections[id] = conn        ← 全局表
                 │    ├─ slave->AddConnection(id, conn)  ← Slave 表
                 │    └─ slave->PostTask(conn->Established)
                 │         └─ write(slave.eventfd, 1)     ← 唤醒 Slave
                 └─ SubmitMasterAccept()  ← 重新提交 accept（一次性操作）
```

#### 链路 B：接收数据

```
Slave Proactor::Run() 收割 eventfd CQE
  └─ ProcessTasks() 执行积压任务
       └─ Connection::DoEstablished()
            ├─ _status = CONNECTED
            └─ SubmitRecv()
                 └─ _slave->SubmitRecv(_sockfd, _in_buffer.GetWritePos(), ...)
                      └─ io_uring_prep_recv + io_uring_submit

Slave Proactor::Run() 收割 recv CQE (op=RECV, res=n)
  └─ _cqHandler → conn->OnRecvComplete(n)
       ├─ _in_buffer.MoveWriteOffset(n)    ← 数据已在内核写入缓冲区
       ├─ _message_cb(conn, &_in_buffer)   ← 用户处理
       └─ SubmitRecv()                      ← 关键：重新提交 recv
```

#### 链路 C：发送数据

```
用户线程:
  conn->Send(data, len)
    ├─ _out_buffer.WriteAndPush(data, len)  ← 任意线程安全
    └─ _slave->PostTask(lambda)
         └─ write(eventfd, 1)

Slave Proactor::Run() → ProcessTasks()
  └─ Connection::SubmitSend()
       └─ _slave->SubmitSend(_sockfd, _out_buffer.GetReadPos(), ...)

Slave Proactor::Run() 收割 send CQE (op=SEND, res=n)
  └─ conn->OnSendComplete(n)
       ├─ _out_buffer.MoveReadOffset(n)
       ├─ if 还有数据: SubmitSend()    ← 继续发
       └─ if DISCONNECTING && 发完: DoRelease()
```

#### 链路 D：关闭连接

```
Connection::Shutdown()
  └─ _slave->PostTask(DoShutdown)
       ├─ _status = DISCONNECTING
       ├─ 处理 _in_buffer 残留数据
       └─ 若 _out_buffer 有数据：SubmitSend()（发完再关）
          若 _out_buffer 无数据：DoRelease()

Connection::DoRelease()
  ├─ _status = DISCONNECTED
  └─ SubmitClose()
       └─ io_uring_prep_close + io_uring_submit

Slave Proactor::Run() 收割 close CQE (op=CLOSE)
  └─ conn->OnCloseComplete()
       ├─ _slave->RemoveConnection(_id)      ← 从 Slave 表移除
       ├─ _closed_cb(conn)                    ← 用户回调
       └─ _srv_closed_cb(conn)
            └─ TcpServer::RemoveConnection()
                 └─ _master.PostTask(RemoveConnectionInMaster)
                      └─ _connections.erase(id)  ← 从全局表移除
```

#### 链路 E：空闲超时释放

```
Master Proactor::Run() 收割 timer CQE (op=TIMER, res=times)
  └─ TcpServer::HandleTimer(times)
       ├─ for i in 0..times:
       │    ├─ _timerTick = (_timerTick + 1) % 60
       │    ├─ 遍历 _timerWheel[_timerTick]:
       │    │    └─ conn->Release()              ← 超时释放
       │    ├─ _timerWheel[_timerTick].clear()
       │    └─ 遍历 _delayCountdown:             ← RunAfter 倒计时
       │         └─ if --countdown <= 0: task()
       ├─ _master.ProcessTasks()                 ← 处理积压任务
       └─ SubmitMasterTimerRead()                ← 重新提交 timerfd read
```

---

## 5. 完整数据流

### 5.1 从连接到通信的完整时序

```
 Client          Master Thread        Slave Thread N      User Callback
   │                  │                     │                  │
   ├─ SYN ───────────►│                     │                  │
   │                  ├─ CQE(ACCEPT, fd)    │                  │
   │                  ├─ NewConnection(fd)  │                  │
   │                  ├─ pool->Next() → N   │                  │
   │                  ├─ PostTask ─────────►│                  │
   │                  ├─ SubmitAccept()     │                  │
   │                  │                  ProcessTasks()        │
   │                  │                  Established()         │
   │                  │                  SubmitRecv(SQE)       │
   │                  │                     │                  │
   ├─ DATA ──────────►│                     │                  │
   │                  │                  CQE(RECV, n)          │
   │                  │                  OnRecvComplete(n) ────► message_cb()
   │                  │                  SubmitRecv(SQE)       │
   │                  │                     │                  │
   │                  │                     │◄── Send(data) ───┤
   │                  │                  SubmitSend(SQE)       │
   │                  │                  CQE(SEND, n)          │
   │                  │                     │                  │
   ├─ FIN ───────────►│                     │                  │
   │                  │                  CQE(RECV, 0)          │
   │                  │                  OnRecvComplete(0)     │
   │                  │                  SubmitClose(SQE)      │
   │                  │                  CQE(CLOSE)            │
   │                  │                  OnCloseComplete ─────► closed_cb()
   │                  │                     │                  │
```

---

## 6. 与 like-muduo 原版的对照

### 6.1 类映射

| like-muduo 原版 | pa_server 新版 | 变化说明 |
|-----------------|---------------|---------|
| `netAddr` | `netAddr` | 直接移植，无变化 |
| `Socket` | `Socket` | 直接移植，去掉 `Read()`/`Send()`（io_uring 负责） |
| `Buffer` | `Buffer` | 直接移植，无变化 |
| `Any` | `Any` | 直接移植，无变化 |
| `EventLoop` | `Proactor` | **核心替换**：epoll → io_uring |
| `Epoller` | （合入 Proactor） | epoll 实例 → io_uring 实例，`epoll_wait` → `io_uring_wait_cqe` |
| `Channel` | （消失） | 不再需要。io_uring 直接提交 SQE，无需"注册事件" |
| `Eventfd` | （合入 Proactor） | eventfd + Channel + epoll → eventfd + io_uring prep_read |
| `Timerfd` | （合入 TcpServer 的 _timerFd） | timerfd + Channel + epoll → timerfd + io_uring prep_read |
| `TimeWheel` | （合入 TcpServer 的时间轮字段） | 逻辑相同，实现方式从独立类变为 TcpServer 成员 |
| `TimerTask` | （合入 TcpServer::HandleTimer） | 简化为 `_delayTasks` + `_delayCountdown` 两个 map |
| `LoopThread` | （消失） | 线程管理合入 `SlavePool` |
| `LoopThreadPool` | `SlavePool` | 直接对应 |
| `Connection` | `Connection` | **大幅修改**：Channel 回调 → io_uring CQE 回调 |
| `Acceptor` | （合入 TcpServer） | 异步 accept 直接由 TcpServer 通过 Master Proactor 提交 |
| `TcpServer` | `TcpServer` | 相似接口，内部实现改为 Proactor 驱动 |

### 6.2 关键行为差异

| 行为 | 原版 (epoll) | 新版 (io_uring) |
|------|-------------|-----------------|
| **读取数据** | `epoll_wait → EPOLLIN → recv()` | `提交 recv SQE → CQE 到达时数据已在缓冲区` |
| **连续读取** | `EPOLLIN 持续触发` | `每次 OnRecvComplete 后重新 SubmitRecv()` |
| **发送数据** | `epoll_wait → EPOLLOUT → send()` | `提交 send SQE → CQE 确认发送完成` |
| **防止重复提交** | `Channel::DisableWrite()` | `_reading / _writing bool 标志` |
| **跨线程任务** | `Eventfd::Awake() → epoll EPOLLIN → RunTasks()` | `eventfd write → io_uring prep_read CQE → ProcessTasks()` |
| **定时器** | `Timerfd + Channel + epoll TIMER 事件` | `Timerfd + io_uring prep_read CQE (op=TIMER)` |

### 6.3 io_uring 特性利用

| 特性 | 利用方式 |
|------|---------|
| **真异步 I/O** | recv/send 由内核完成，用户态不需阻塞等待 |
| **批量提交** | 多个 SQE 可在一次 `io_uring_submit` 中批量告知内核 |
| **批量收割** | `io_uring_for_each_cqe` 一次收割所有已完成的 CQE |
| **user_data** | 编码 `(connId << 8) | OpType`，实现 O(1) 的 CQE → Connection 路由 |
| **固定缓冲区** | `_eventFdBuf` 使用成员变量，避免每次分配/释放 |
