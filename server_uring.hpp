#ifndef PA_SERVER_HPP
#define PA_SERVER_HPP

#include <liburing.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <cstdint>
#include <cassert>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <fstream>
#include <regex>
#include <sys/stat.h>
#include <signal.h>
#include <execinfo.h>

// ══════════════════════════════════════════════════════════════════════════════
//  模仿 like-muduo/source/server.hpp 的类层次结构，
//  将 epoll Reactor 并发模型替换为基于 io_uring 的主从 Proactor 模型。
//
//  架构对比：
//    like-muduo (Reactor + epoll):
//      EventLoop (1 epoll/thread) → Channel (fd注册) → epoll_wait → 同步I/O
//      多Reactor：base loop处理accept，worker loop处理I/O
//
//    pa_server (Proactor + io_uring):
//      Proactor (1 io_uring/thread) → SubmitXxx (提交SQE) → 内核完成 → 收割CQE
//      主从Proactor：Master处理accept+定时器，Slave处理recv/send/close
//
//  数据流：
//    1. Master 提交 async accept SQE
//    2. accept CQE 到达 → 创建 Connection → 轮询分配给 Slave
//    3. Slave 提交 async recv SQE
//    4. recv CQE 到达 → 数据已在缓冲区 → 处理 → 提交下一次 recv
//    5. send 同理：提交 send SQE → send CQE 到达 → 继续或停止
// ══════════════════════════════════════════════════════════════════════════════

namespace pa {

#define DEFAULTSIZE    1024
#define DEFAULT_TIMEOUT 10

// ──── 轻量日志（兼容 pa_server/Logger.hpp）────────────────────────────────────
enum class logLevel { DEBUG, INFO, WARNING, ERROR, FATAL };

inline const char* toLevelStr(logLevel lv) {
    static const char* s[] = {"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"};
    return s[static_cast<int>(lv)];
}
inline std::string getTime() {
    time_t ts = time(nullptr);
    struct tm tm_buf;
    localtime_r(&ts, &tm_buf);
    char buf[128];
    snprintf(buf, sizeof(buf), "%d-%02d-%02d %02d:%02d:%02d",
             tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
             tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
    return buf;
}
class LogStream {
public:
    LogStream(logLevel lv, const char* file, int line)
        : _lv(lv), _file(file), _line(line) {}
    ~LogStream() {
        std::cout << '[' << getTime() << "] ["
                  << toLevelStr(_lv) << "] ["
                  << _file << "] [" << _line << "] - "
                  << _ss.str() << std::endl;
    }
    template<typename T> LogStream& operator<<(const T& v) {
        _ss << v; return *this;
    }
private:
    logLevel _lv; std::string _file; int _line;
    std::stringstream _ss;
};
#ifdef PA_DEBUG
#define LOG(level) pa::LogStream(pa::logLevel::level, __FILE__, __LINE__)
#else
#define LOG(level) while(0) pa::LogStream(pa::logLevel::level, __FILE__, __LINE__)
#endif

// ──── netAddr（移植自 like-muduo/source/inet_addr.hpp）─────────────────────────
class netAddr {
public:
    netAddr() { std::memset(&_addr, 0, sizeof(_addr)); }
    netAddr(const std::string& ip, uint16_t port) : _ip(ip), _port(port) {
        _addr.sin_family = AF_INET;
        _addr.sin_addr.s_addr = ::inet_addr(_ip.c_str());
        _addr.sin_port = htons(_port);
    }
    explicit netAddr(uint16_t port) : _ip("0.0.0.0"), _port(port) {
        _addr.sin_family = AF_INET;
        _addr.sin_addr.s_addr = INADDR_ANY;
        _addr.sin_port = htons(_port);
    }
    std::string name() const { return _ip + ":" + std::to_string(_port); }
    struct sockaddr* operator&() { return reinterpret_cast<struct sockaddr*>(&_addr); }
    socklen_t size() const { return sizeof(_addr); }
private:
    struct sockaddr_in _addr;
    std::string _ip;
    uint16_t _port;
};

// ──── Socket（移植自 like-muduo/source/server.hpp）─────────────────────────────
class Socket {
public:
    Socket(int fd = -1) : _fd(fd) {}
    ~Socket() { Close(); }
    void Close() { if (_fd >= 0) { ::close(_fd); _fd = -1; } }
    int  Fd() const { return _fd; }

    bool Create() {
        _fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (_fd < 0) { LOG(ERROR) << "socket() failed"; return false; }
        SetNoBlock();
        return true;
    }
    bool Bind(const std::string& ip, uint16_t port) {
        netAddr addr(ip, port);
        if (::bind(_fd, &addr, addr.size()) < 0) {
            LOG(ERROR) << "bind() failed"; return false;
        }
        return true;
    }
    bool Bind(uint16_t port) {
        netAddr addr(port);
        if (::bind(_fd, &addr, addr.size()) < 0) {
            LOG(ERROR) << "bind() failed"; return false;
        }
        return true;
    }
    bool Listen(int backlog = 128) {
        if (::listen(_fd, backlog) < 0) {
            LOG(ERROR) << "listen() failed"; return false;
        }
        return true;
    }
    void SetNoBlock() {
        int flags = ::fcntl(_fd, F_GETFL, 0);
        ::fcntl(_fd, F_SETFL, flags | O_NONBLOCK);
    }
    void ReuseAddress() {
        int val = 1;
        ::setsockopt(_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
        ::setsockopt(_fd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));
    }
    bool BuildServer(uint16_t port, const std::string& ip = "0.0.0.0") {
        if (!Create())    return false;
        ReuseAddress();
        if (!Bind(ip, port)) return false;
        if (!Listen())    return false;
        return true;
    }
private:
    int _fd;
};

// ──── Buffer（移植自 like-muduo/source/server.hpp）─────────────────────────────
class Buffer {
public:
    Buffer(const char* name = nullptr) : _write_idx(0), _read_idx(0), _buffer(DEFAULTSIZE), _name(name) {}

    char*       GetWritePos()       { return &_buffer[0] + _write_idx; }
    const char* GetReadPos()  const { return &_buffer[0] + _read_idx; }
    int ReadableSize()         const { return _write_idx - _read_idx; }
    int PostIdle()             const { return Capacity() - _write_idx; }
    int PreIdle()              const { return _read_idx; }
    int Capacity()             const { return static_cast<int>(_buffer.size()); }

    void MoveReadOffset(int len) {
        if (len > ReadableSize()) {
            LOG(ERROR) << "MoveReadOffset FAILED: len=" << len
                       << " ReadableSize=" << ReadableSize()
                       << " _read_idx=" << _read_idx
                       << " _write_idx=" << _write_idx
                       << " Capacity=" << Capacity()
                       << " this=" << static_cast<const void*>(this)
                       << " name=" << (_name ? _name : "null");
            std::cerr << "MoveReadOffset FAILED: len=" << len
                      << " ReadableSize=" << ReadableSize()
                      << " _read_idx=" << _read_idx
                      << " _write_idx=" << _write_idx
                      << " Capacity=" << Capacity()
                      << " this=" << static_cast<const void*>(this)
                      << " name=" << (_name ? _name : "null") << std::endl;
            void* callstack[128];
            int frames = backtrace(callstack, 128);
            char** strs = backtrace_symbols(callstack, frames);
            for (int i = 0; i < frames; ++i) {
                std::cerr << "  [" << i << "] " << strs[i] << std::endl;
            }
            free(strs);
            abort();
        }
        _read_idx += len;
    }
    void MoveWriteOffset(int len) {
        if (len > PostIdle()) {
            LOG(ERROR) << "MoveWriteOffset FAILED: len=" << len
                       << " PostIdle=" << PostIdle()
                       << " _read_idx=" << _read_idx
                       << " _write_idx=" << _write_idx
                       << " Capacity=" << Capacity()
                       << " this=" << static_cast<const void*>(this);
            std::cerr << "MoveWriteOffset FAILED: len=" << len
                      << " PostIdle=" << PostIdle()
                      << " _read_idx=" << _read_idx
                      << " _write_idx=" << _write_idx
                      << " Capacity=" << Capacity()
                      << " this=" << static_cast<const void*>(this) << std::endl;
            abort();
        }
        _write_idx += len;
    }

    void EnsureWrite(int len) {
        if (PostIdle() >= len) return;
        if (PreIdle() + PostIdle() >= len) {
            size_t sz = ReadableSize();
            std::copy(GetReadPos(), GetReadPos() + sz, _buffer.begin());
            _read_idx  = 0;
            _write_idx = static_cast<int>(sz);
        } else {
            _buffer.resize(_buffer.size() + 2 * len);
        }
    }

    int WriteAndPush(const void* data, int len) {
        EnsureWrite(len);
        std::copy((const char*)data, (const char*)data + len, GetWritePos());
        MoveWriteOffset(len);
        return len;
    }
    int WriteAndPush(const std::string& s)    { return WriteAndPush(s.c_str(), s.size()); }
    int WriteAndPush(const Buffer& other)     { return WriteAndPush(other.GetReadPos(), other.ReadableSize()); }

    int ReadAndPop(void* d, int len) {
        int l = std::min(ReadableSize(), len);
        std::copy(GetReadPos(), GetReadPos() + l, (char*)d);
        MoveReadOffset(l);
        return l;
    }

    char* FindCRLF() { return (char*)::memchr(GetReadPos(), '\n', ReadableSize()); }
    std::string GetLine() {
        char* pos = FindCRLF();
        if (!pos) return "";
        int len = static_cast<int>(pos - GetReadPos() + 1);
        std::string line;
        line.resize(len);
        std::copy(GetReadPos(), GetReadPos() + len, &line[0]);
        return line;
    }
    std::string GetLineAndPop() {
        std::string line = GetLine();
        if (!line.empty()) MoveReadOffset(static_cast<int>(line.size()));
        return line;
    }
    void Clear() { _read_idx = _write_idx = 0; }

private:
    int _write_idx;
    int _read_idx;
    std::vector<char> _buffer;
    const char* _name;
};

// ──── Any（类型擦除上下文，移植自 like-muduo）─────────────────────────────────
class Any {
private:
    class holder {
    public:
        virtual ~holder() {}
        virtual holder* clone() const = 0;
        virtual const std::type_info& type() const = 0;
    };
    template<class T>
    class placeholder : public holder {
    public:
        T _val;
        explicit placeholder(const T& val) : _val(val) {}
        holder* clone() const override { return new placeholder<T>(_val); }
        const std::type_info& type() const override { return typeid(_val); }
    };
    holder* _dummy;
public:
    Any() : _dummy(nullptr) {}
    template<class T> Any(const T& val) : _dummy(new placeholder<T>(val)) {}
    Any(const Any& other) : _dummy(other._dummy ? other._dummy->clone() : nullptr) {}
    ~Any() { delete _dummy; }
    void swap(Any& other) { std::swap(_dummy, other._dummy); }
    Any& operator=(const Any& other) { Any(other).swap(*this); return *this; }
    template<class T> Any& operator=(const T& val) { Any(val).swap(*this); return *this; }
    template<class T> T* get() {
        if (!_dummy || _dummy->type() != typeid(T)) return nullptr;
        return &((placeholder<T>*)_dummy)->_val;
    }
};

// ══════════════════════════════════════════════════════════════════════════════
//  io_uring 操作类型编码
// ══════════════════════════════════════════════════════════════════════════════
//
//  liburing 2.1 的 user_data 是 void* 类型(非 uint64_t)。
//  我们在 64 位系统上把 uint64_t 通过 reinterpret_cast 塞进 void* 中。
//
//  user_data 编码：低 8 位 = OpType，高位 = connId
//    connId = 0  → 内部操作（accept, eventfd, timer, timeout）
//    connId >= 1 → 用户连接
//
enum class OpType : uint64_t {
    NONE    = 0,
    ACCEPT  = 1,   // Master: 异步 accept
    RECV    = 2,   // Slave:  异步 recv
    SEND    = 3,   // Slave:  异步 send
    CLOSE   = 4,   // Slave:  异步 close
    EVENTFD = 5,   // 跨线程唤醒 eventfd read
    TIMER   = 6,   // Master: 时间轮 timerfd read
    TIMEOUT = 7,   // Master: io_uring 原生超时（RunAfter）
};

inline uint64_t MakeUserData(uint64_t connId, OpType op) {
    return (connId << 8) | static_cast<uint64_t>(op);
}
inline uint64_t GetConnId(uint64_t data) { return data >> 8; }
inline OpType   GetOpType(uint64_t data) { return static_cast<OpType>(data & 0xFF); }

// liburing 2.1 的 sqe_set_data / cqe_get_data 使用 void*，用下列辅助转换
inline void*    ToUserPtr(uint64_t data)  { return reinterpret_cast<void*>(data); }
inline uint64_t FromUserPtr(void* ptr)    { return reinterpret_cast<uint64_t>(ptr); }

// ──── 连接状态 ────────────────────────────────────────────────────────────────
enum class ConnStat : int {
    CONNECTING    = 0,
    CONNECTED     = 1,
    DISCONNECTING = 2,
    DISCONNECTED  = 3
};

// ──── 前置声明 ────────────────────────────────────────────────────────────────
class Proactor;
class Connection;
using PtrConnection = std::shared_ptr<Connection>;
using ConnMap       = std::unordered_map<uint64_t, PtrConnection>;

// ══════════════════════════════════════════════════════════════════════════════
//  Connection — TCP 连接
// ══════════════════════════════════════════════════════════════════════════════
//
//  状态机：CONNECTING → CONNECTED → DISCONNECTING → DISCONNECTED
//
//  与 like-muduo 版本的关键区别：
//    - 原版：Channel 注册到 epoll → epoll 通知 → 同步 recv/send
//    - 新版：直接向 io_uring 提交 recv/send SQE → 内核异步完成 → CQE 驱动回调
//    - _reading/_writing 标志防止对同一 fd 提交重复的 SQE
//
class Connection : public std::enable_shared_from_this<Connection> {
public:
    using MessageCallback   = std::function<void(const PtrConnection&, Buffer*)>;
    using ClosedCallback    = std::function<void(const PtrConnection&)>;
    using ConnectedCallback = std::function<void(const PtrConnection&)>;
    using AnyEventCallback  = std::function<void(const PtrConnection&)>;

    Connection(Proactor* slave, uint64_t id, int sockfd);

    // 属性
    int      Fd()          const { return _sockfd; }
    uint64_t Id()          const { return _id; }
    bool     IsConnected() const { return _status == ConnStat::CONNECTED; }
    Any*     GetContext()        { return &_context; }
    void     SetContext(const Any& ctx) { _context = ctx; }

    // 回调注入
    void SetMessageCallback  (const MessageCallback&   cb) { _message_cb   = cb; }
    void SetClosedCallback   (const ClosedCallback&    cb) { _closed_cb    = cb; }
    void SetConnectedCallback(const ConnectedCallback& cb) { _connected_cb = cb; }
    void SetAnyEventCallback (const AnyEventCallback&  cb) { _any_cb       = cb; }
    void SetSrvClosedCallback(const ClosedCallback&    cb) { _srv_closed_cb = cb; }

    // ── 外部接口（线程安全，通过 PostTask 投递到所属 Proactor 线程）───────
    void Established();
    void Send(const char* data, size_t len);
    void Shutdown();
    void Release();
    void EnableInactiveRelease(int sec);
    void CancelInactiveRelease();

    // ── CQE 回调（由 Slave Proactor 在 CQE 处理中调用）────────────────────
    void OnRecvComplete(int n);
    void OnSendComplete(int n);
    void OnCloseComplete(int n);

private:
    void SubmitRecv();
    void SubmitSend();
    void SubmitClose();

    void DoEstablished();
    void DoShutdown();
    void DoRelease();
    void DoEnableInactiveRelease(int sec);
    void DoCancelInactiveRelease();
    void HandleError();

    uint64_t     _id;
    int          _sockfd;
    Proactor*    _slave;
    ConnStat     _status;
    bool         _reading;          // 是否有在途 recv（防重复提交）
    bool         _writing;          // 是否有在途 send（防重复提交）
    bool         _releaseOrNot;     // 是否启用空闲超时释放
    int          _timeout;
    Buffer       _in_buffer;
    Buffer       _out_buffer;
    Any          _context;

    MessageCallback   _message_cb;
    ClosedCallback    _closed_cb;
    ConnectedCallback _connected_cb;
    AnyEventCallback  _any_cb;
    ClosedCallback    _srv_closed_cb;
};

// ══════════════════════════════════════════════════════════════════════════════
//  Proactor — io_uring 事件循环
// ══════════════════════════════════════════════════════════════════════════════
//
//  取代原版的 EventLoop + Epoller + Channel 三层结构。
//  Master Proactor: 提交 accept + timerfd read，CQE 由 TcpServer::OnMasterCQE 处理
//    - 一个独立的 io_uring 实例（取代 epoll fd）
//    - 一个 eventfd（跨线程唤醒，取代原版 Eventfd + Channel 组合）
//    - 一个后台线程（Run方法阻塞运行）
//
//  Slave  Proactor: 提交 recv/send/close，CQE 路由到 Connection 回调
//  Slave  Proactor: 提交 recv/send/close，CQE 路由到 Connection 回调
//
class Proactor {
public:
    using Task      = std::function<void()>;
    using CQHandler = std::function<void(uint64_t userData, int cqeRes)>;

    explicit Proactor(int queueSize = 256);
    ~Proactor();

    Proactor(const Proactor&) = delete;
    Proactor& operator=(const Proactor&) = delete;

    // ── 生命周期 ──────────────────────────────────────────────────────────
    void Start();       // 阻塞当前线程，运行事件循环
    void Stop();        // 通知事件循环退出

    // ── 跨线程任务 ────────────────────────────────────────────────────────
    void PostTask(Task task);

    // ── io_uring I/O 提交（必须在 Proactor 线程中调用）────────────────────
    void SubmitAccept(int listenFd, uint64_t connId);
    void SubmitRecv(int fd, void* buf, size_t len, uint64_t connId);
    void SubmitSend(int fd, const void* buf, size_t len, uint64_t connId);
    void SubmitClose(int fd, uint64_t connId);
    void SubmitRead(int fd, void* buf, size_t len, uint64_t connId, OpType op);

    // ── CQE 处理回调 ──────────────────────────────────────────────────────
    void SetCQHandler(CQHandler h) { _cqHandler = std::move(h); }

    // ── 连接管理（Slave 使用）──────────────────────────────────────────────
    void          AddConnection(uint64_t id, PtrConnection conn);
    void          RemoveConnection(uint64_t id);
    PtrConnection GetConnection(uint64_t id);

    // ── 线程标识 ──────────────────────────────────────────────────────────
    std::thread::id ThreadId()  const { return _tid; }
    bool            IsInThread() const { return std::this_thread::get_id() == _tid; }

    // ── 处理积压任务（公开，供 TcpServer::HandleTimer 调用）────────────────
    void ProcessTasks();
    void FlushSubmits();        // 批量提交 SQ（减少 syscall）

    // ── 全局连接 ID 生成 ──────────────────────────────────────────────────
    static uint64_t NextConnId() { return ++_globalConnId; }

private:
    void Run();
    struct io_uring_sqe* GetSQE();

    io_uring            _ring;
    int                 _eventFd;
    uint64_t            _eventFdBuf;
    bool                _needSubmit;        // 批量提交标志
    std::atomic<bool>   _running;
    std::thread::id     _tid;

    std::mutex          _taskMtx;
    std::vector<Task>   _pendingTasks;

    CQHandler           _cqHandler;
    ConnMap             _conns;

    static std::atomic<uint64_t> _globalConnId;
};

// ══════════════════════════════════════════════════════════════════════════════
//  SlavePool — 从 Proactor 线程池
// ══════════════════════════════════════════════════════════════════════════════
//
//  取代原版的 LoopThreadPool。原版每个 LoopThread 创建一个独立的 EventLoop，
//  这里每个 Slave 线程创建一个独立的 Proactor（各自拥有独立的 io_uring）。
//
class SlavePool {
public:
    explicit SlavePool(int count);
    ~SlavePool();

    void      Start();
    Proactor* Next();           // 轮询分配

    int Count() const { return _count; }

private:
    int                      _count;
    int                      _nextIdx;
    std::mutex               _mtx;
    std::vector<Proactor*>   _slaves;
    std::vector<std::thread> _threads;
};

// ══════════════════════════════════════════════════════════════════════════════


// ══════════════════════════════════════════════════════════════════════════════
//  TcpServer — 主从 Proactor TCP 服务器
// ══════════════════════════════════════════════════════════════════════════════
//
//  取代原版的 TcpServer。原版使用 _base_loop (EventLoop) + Acceptor，
//  这里 Master 使用 epoll（控制面），Slave 使用 io_uring（数据面）。
//
class TcpServer {
public:
    using MessageCallback   = std::function<void(const PtrConnection&, Buffer*)>;
    using ClosedCallback    = std::function<void(const PtrConnection&)>;
    using ConnectedCallback = std::function<void(const PtrConnection&)>;
    using AnyEventCallback  = std::function<void(const PtrConnection&)>;
    using Functor           = std::function<void()>;

    explicit TcpServer(uint16_t port);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // ── 配置 ──────────────────────────────────────────────────────────────
    void SetThreadsCount(int n);
    void SetMessageCallback  (const MessageCallback&   cb) { _message_cb   = cb; }
    void SetClosedCallback   (const ClosedCallback&    cb) { _closed_cb    = cb; }
    void SetConnectedCallback(const ConnectedCallback& cb) { _connected_cb = cb; }
    void SetAnyEventCallback (const AnyEventCallback&  cb) { _any_cb       = cb; }
    void EnableInactiveRelease(int timeout) { _timeout = timeout; _releaseOrNot = true; }
    void RunAfter(const Functor& task, int delay);

    // ── 启动 ──────────────────────────────────────────────────────────────
    void Start();

private:
    void HandleAccept(int newFd);
    void HandleTimer(int times);
    void OnMasterCQE(uint64_t userData, int cqeRes);
    void SubmitMasterAccept();
    void SubmitMasterTimerRead();

    void NewConnection(int fd);
    void RemoveConnection(const PtrConnection& conn);
    void RemoveConnectionInMaster(const PtrConnection& conn);

    // 数据成员
    uint16_t    _port;
    uint64_t    _nextId;            // 连接ID/定时器ID共用自增
    int         _timeout;
    bool        _releaseOrNot;

    Socket           _listenSock;
    Proactor    _master;
    SlavePool*       _pool;

    // 时间轮（仿照原版 TimeWheel）
    int         _timerFd;
    int         _timerTick;
    static const int TIMER_CAPACITY = 60;
    std::vector<std::vector<PtrConnection>> _timerWheel;
    std::unordered_map<uint64_t, int>       _connTimerSlot;

    // RunAfter 延迟任务
    std::unordered_map<uint64_t, Functor>   _delayTasks;
    std::unordered_map<uint64_t, int>       _delayCountdown;

    ConnMap     _connections;       // 全局连接表（保证 shared_ptr 生命周期）

    MessageCallback   _message_cb;
    ClosedCallback    _closed_cb;
    ConnectedCallback _connected_cb;
    AnyEventCallback  _any_cb;
};

// ══════════════════════════════════════════════════════════════════════════════
//  Connection 实现
// ══════════════════════════════════════════════════════════════════════════════

inline Connection::Connection(Proactor* slave, uint64_t id, int sockfd)
    : _id(id)
    , _sockfd(sockfd)
    , _slave(slave)
    , _status(ConnStat::CONNECTING)
    , _reading(false)
    , _writing(false)
    , _releaseOrNot(false)
    , _timeout(DEFAULT_TIMEOUT)
    , _in_buffer("in")
    , _out_buffer("out")
{}

// ── 向 io_uring 提交 I/O 操作 ────────────────────────────────────────────────

inline void Connection::SubmitRecv() {
    if (_reading) return;
    _reading = true;
    _in_buffer.EnsureWrite(65536);
    _slave->SubmitRecv(_sockfd, _in_buffer.GetWritePos(),
                       static_cast<size_t>(_in_buffer.PostIdle()), _id);
}

inline void Connection::SubmitSend() {
    if (_writing) return;
    if (_out_buffer.ReadableSize() == 0) return;
    _writing = true;
    _slave->SubmitSend(_sockfd, _out_buffer.GetReadPos(),
                       static_cast<size_t>(_out_buffer.ReadableSize()), _id);
}

inline void Connection::SubmitClose() {
    _slave->SubmitClose(_sockfd, _id);
}

// ── CQE 回调 ─────────────────────────────────────────────────────────────────

inline void Connection::OnRecvComplete(int n) {
    _reading = false;

    if (n > 0) {
        _in_buffer.MoveWriteOffset(n);
        if (_message_cb) {
            _message_cb(shared_from_this(), &_in_buffer);
        }
        if (_status == ConnStat::CONNECTED) {
            SubmitRecv();   // Proactor 模式：每次读完重新提交
        }
    } else if (n == 0) {
        LOG(DEBUG) << "Connection[" << _id << "] peer closed";
        if (_in_buffer.ReadableSize() > 0 && _message_cb) {
            _message_cb(shared_from_this(), &_in_buffer);
        }
        DoRelease();
    } else {
        if (n == -EAGAIN || n == -EINTR) { SubmitRecv(); return; }
        LOG(ERROR) << "Connection[" << _id << "] recv error: " << strerror(-n);
        HandleError();
    }
}

inline void Connection::OnSendComplete(int n) {
    _writing = false;

    if (n > 0) {
        _out_buffer.MoveReadOffset(n);
        if (_out_buffer.ReadableSize() > 0) {
            SubmitSend();
        } else if (_status == ConnStat::DISCONNECTING) {
            DoRelease();
        }
    } else if (n < 0) {
        if (n == -EAGAIN || n == -EINTR) { SubmitSend(); return; }
        LOG(ERROR) << "Connection[" << _id << "] send error: " << strerror(-n);
        HandleError();
    }
}

inline void Connection::OnCloseComplete(int /*n*/) {
    LOG(DEBUG) << "Connection[" << _id << "] fd closed";
    _status = ConnStat::DISCONNECTED;
    _slave->RemoveConnection(_id);

    if (_closed_cb)     _closed_cb(shared_from_this());
    if (_srv_closed_cb) _srv_closed_cb(shared_from_this());
}

// ── 外部接口 ─────────────────────────────────────────────────────────────────

inline void Connection::Established() {
    if (_slave->IsInThread()) {
        DoEstablished();
    } else {
        _slave->PostTask([this]() { DoEstablished(); });
    }
}
inline void Connection::DoEstablished() {
    assert(_slave->IsInThread());
    assert(_status == ConnStat::CONNECTING);
    _status = ConnStat::CONNECTED;
    SubmitRecv();
    if (_connected_cb) _connected_cb(shared_from_this());
}

inline void Connection::Send(const char* data, size_t len) {
    _out_buffer.WriteAndPush(data, static_cast<int>(len));
    // 优化：若已在 Proactor 线程，直接提交发送，省去 eventfd 往返
    if (_slave->IsInThread()) {
        if ((_status == ConnStat::CONNECTED || _status == ConnStat::DISCONNECTING) && !_writing) {
            SubmitSend();
        }
    } else {
        _slave->PostTask([this]() {
            if ((_status == ConnStat::CONNECTED || _status == ConnStat::DISCONNECTING) && !_writing) {
                SubmitSend();
            }
        });
    }
}

inline void Connection::Shutdown() {
    _status = ConnStat::DISCONNECTING;
    if (_slave->IsInThread()) {
        DoShutdown();
    } else {
        _slave->PostTask([this]() { DoShutdown(); });
    }
}
inline void Connection::DoShutdown() {
    // _status 已在 Shutdown() 中同步设好，此处不再重复设置
    // 注：与 like-muduo 原版不同，这里不调 _message_cb 处理残留数据，
    // 因为 Proactor 模式下可能引发二次响应（残留字节被误解析为新请求）。
    // 连接正在关闭，残留数据直接丢弃。
    if (_out_buffer.ReadableSize() == 0) {
        DoRelease();
    } else if (!_writing) {
        SubmitSend();
    }
}

inline void Connection::Release() {
    if (_slave->IsInThread()) {
        DoRelease();
    } else {
        _slave->PostTask([this]() { DoRelease(); });
    }
}
inline void Connection::DoRelease() {
    if (_status == ConnStat::DISCONNECTED) return;
    LOG(DEBUG) << "Connection[" << _id << "] releasing";
    _status = ConnStat::DISCONNECTED;
    if (_releaseOrNot) DoCancelInactiveRelease();
    SubmitClose();
}

inline void Connection::EnableInactiveRelease(int sec) {
    _slave->PostTask([this, sec]() { DoEnableInactiveRelease(sec); });
}
inline void Connection::DoEnableInactiveRelease(int sec) {
    _releaseOrNot = true;
    _timeout = sec;
}

inline void Connection::CancelInactiveRelease() {
    _slave->PostTask([this]() { DoCancelInactiveRelease(); });
}
inline void Connection::DoCancelInactiveRelease() {
    _releaseOrNot = false;
}

inline void Connection::HandleError() {
    if (_in_buffer.ReadableSize() > 0 && _message_cb) {
        _message_cb(shared_from_this(), &_in_buffer);
    }
    DoRelease();
}

// ══════════════════════════════════════════════════════════════════════════════
//  Proactor 实现
// ══════════════════════════════════════════════════════════════════════════════

inline std::atomic<uint64_t> Proactor::_globalConnId{0};

inline Proactor::Proactor(int queueSize)
    : _eventFdBuf(0)
    , _running(false)
{
    int ret = io_uring_queue_init(queueSize, &_ring, 0);
    if (ret < 0) {
        LOG(FATAL) << "io_uring_queue_init failed: " << strerror(-ret);
        exit(1);
    }
    _eventFd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (_eventFd < 0) {
        LOG(FATAL) << "eventfd creation failed";
        exit(1);
    }
}

inline Proactor::~Proactor() {
    Stop();
    io_uring_queue_exit(&_ring);
    if (_eventFd >= 0) { close(_eventFd); _eventFd = -1; }
}

inline void Proactor::Start() {
    _running.store(true, std::memory_order_relaxed);
    _tid = std::this_thread::get_id();
    Run();
}

inline void Proactor::Stop() {
    _running.store(false, std::memory_order_relaxed);
    uint64_t val = 1;
    ssize_t n = write(_eventFd, &val, sizeof(val));
    (void)n;
}

inline void Proactor::PostTask(Task task) {
    {
        std::lock_guard<std::mutex> lk(_taskMtx);
        _pendingTasks.push_back(std::move(task));
    }
    uint64_t val = 1;
    ssize_t n = write(_eventFd, &val, sizeof(val));
    (void)n;
}

	// ── 获取 SQE（自动处理 SQ 满的情况）───────────────────────────────────────────

	inline struct io_uring_sqe* Proactor::GetSQE() {
	    struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
	    if (sqe) return sqe;

	    // SQ 满：提交已有项即可释放所有 SQ 槽位，无需收割 CQE
	    // 注：收割 CQE 会导致与主循环 io_uring_for_each_cqe 的 khead 竞争，
	    //     引发 CQE 重复处理（二次 OnSendComplete/OnRecvComplete）。
	    io_uring_submit(&_ring);
	    return io_uring_get_sqe(&_ring);
	}

// ── io_uring I/O 提交方法 ─────────────────────────────────────────────────────

inline void Proactor::SubmitAccept(int listenFd, uint64_t connId) {
    struct io_uring_sqe* sqe = GetSQE();
    if (!sqe) return;
    io_uring_prep_accept(sqe, listenFd, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, ToUserPtr(MakeUserData(connId, OpType::ACCEPT)));
    _needSubmit = true;
}

inline void Proactor::SubmitRecv(int fd, void* buf, size_t len, uint64_t connId) {
    struct io_uring_sqe* sqe = GetSQE();
    if (!sqe) return;
    io_uring_prep_recv(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, ToUserPtr(MakeUserData(connId, OpType::RECV)));
    _needSubmit = true;
}

inline void Proactor::SubmitSend(int fd, const void* buf, size_t len, uint64_t connId) {
    struct io_uring_sqe* sqe = GetSQE();
    if (!sqe) return;
    io_uring_prep_send(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, ToUserPtr(MakeUserData(connId, OpType::SEND)));
    _needSubmit = true;
}

inline void Proactor::SubmitClose(int fd, uint64_t connId) {
    struct io_uring_sqe* sqe = GetSQE();
    if (!sqe) return;
    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data(sqe, ToUserPtr(MakeUserData(connId, OpType::CLOSE)));
    _needSubmit = true;
}

inline void Proactor::SubmitRead(int fd, void* buf, size_t len, uint64_t connId, OpType op) {
    struct io_uring_sqe* sqe = GetSQE();
    if (!sqe) return;
    io_uring_prep_read(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, ToUserPtr(MakeUserData(connId, op)));
    _needSubmit = true;
}

// ── 连接管理 ──────────────────────────────────────────────────────────────────

inline void Proactor::AddConnection(uint64_t id, PtrConnection conn) {
    _conns[id] = std::move(conn);
}
inline void Proactor::RemoveConnection(uint64_t id) {
    _conns.erase(id);
}
inline PtrConnection Proactor::GetConnection(uint64_t id) {
    auto it = _conns.find(id);
    return (it != _conns.end()) ? it->second : nullptr;
}

// ── 处理积压任务 ──────────────────────────────────────────────────────────────

inline void Proactor::ProcessTasks() {
    std::vector<Task> tasks;
    {
        std::lock_guard<std::mutex> lk(_taskMtx);
        tasks.swap(_pendingTasks);
    }
    for (auto& t : tasks) t();
}

inline void Proactor::FlushSubmits() {
    if (_needSubmit) {
        _needSubmit = false;
        io_uring_submit(&_ring);
    }
}

// ── 主事件循环 ────────────────────────────────────────────────────────────────
//
//  每个 Proactor 线程的核心循环。
//  
//  与 epoll 的关键区别：
//    epoll:   epoll_wait → 遍历就绪 fd → 同步执行 I/O → 处理结果
//    io_uring: 提交 SQE → io_uring_wait_cqe → CQE 中已包含 I/O 结果 → 回调
//
//  eventfd 扮演"唤醒 fd"角色：任何线程 PostTask 时写入 eventfd，
//  io_uring prep_read 操作完成产生 CQE，我们在处理 CQE 时执行积压任务。
//

inline void Proactor::Run() {
    LOG(DEBUG) << "Proactor::Run start, tid=" << std::this_thread::get_id();

    // 注册 eventfd read（一次性操作，触发后需重新提交）
    SubmitRead(_eventFd, &_eventFdBuf, sizeof(uint64_t), 0, OpType::EVENTFD);
    FlushSubmits();

    while (_running.load(std::memory_order_relaxed)) {
        struct io_uring_cqe* cqe = nullptr;
        int ret = io_uring_wait_cqe(&_ring, &cqe);
        if (ret < 0) {
            if (ret == -EINTR) continue;
            LOG(ERROR) << "io_uring_wait_cqe error: " << strerror(-ret);
            break;
        }

        unsigned head;
        unsigned count = 0;
        io_uring_for_each_cqe(&_ring, head, cqe) {
            uint64_t userData = FromUserPtr(io_uring_cqe_get_data(cqe));
            int      res      = cqe->res;
            OpType   op       = GetOpType(userData);

            if (op == OpType::EVENTFD) {
                // 跨线程唤醒 → 处理积压任务 → 重新注册 eventfd read
                ProcessTasks();
                SubmitRead(_eventFd, &_eventFdBuf, sizeof(uint64_t),
                           0, OpType::EVENTFD);
            } else if (_cqHandler) {
                // 委托给 Master 或 Slave 的回调
                _cqHandler(userData, res);
            }
            ++count;
        }
        io_uring_cq_advance(&_ring, count);
        FlushSubmits();
    }

    LOG(DEBUG) << "Proactor::Run exit, tid=" << std::this_thread::get_id();
}

// ══════════════════════════════════════════════════════════════════════════════
//  SlavePool 实现
// ══════════════════════════════════════════════════════════════════════════════

inline SlavePool::SlavePool(int count)
    : _count(count > 0 ? count : 1)
    , _nextIdx(0)
{
    _slaves.resize(_count);
    for (int i = 0; i < _count; ++i) {
        _slaves[i] = new Proactor(256);
    }
}

inline SlavePool::~SlavePool() {
    for (auto* s : _slaves) s->Stop();
    for (auto& t : _threads) {
        if (t.joinable()) t.join();
    }
    for (auto* s : _slaves) delete s;
}

inline void SlavePool::Start() {
    for (int i = 0; i < _count; ++i) {
        _threads.emplace_back([this, i]() {
            Proactor* slave = _slaves[i];

            // Slave 的 CQE 处理器：根据 OpType 路由到 Connection 回调
            slave->SetCQHandler([slave](uint64_t userData, int cqeRes) {
                OpType   op  = GetOpType(userData);
                uint64_t cid = GetConnId(userData);

                auto conn = slave->GetConnection(cid);
                if (!conn) return;

                switch (op) {
                case OpType::RECV:  conn->OnRecvComplete(cqeRes);  break;
                case OpType::SEND:  conn->OnSendComplete(cqeRes);  break;
                case OpType::CLOSE: conn->OnCloseComplete(cqeRes); break;
                default: break;
                }
            });

            slave->Start();
        });
    }
}

inline Proactor* SlavePool::Next() {
    std::lock_guard<std::mutex> lk(_mtx);
    Proactor* s = _slaves[_nextIdx];
    _nextIdx = (_nextIdx + 1) % _count;
    return s;
}

// ══════════════════════════════════════════════════════════════════════════════
//  TcpServer 实现
// ══════════════════════════════════════════════════════════════════════════════

inline TcpServer::TcpServer(uint16_t port)
    : _port(port)
    , _nextId(0)
    , _timeout(DEFAULT_TIMEOUT)
    , _releaseOrNot(false)
    , _pool(nullptr)
    , _timerFd(-1)
    , _timerTick(0)
    , _timerWheel(TIMER_CAPACITY)
{
    if (!_listenSock.BuildServer(_port)) {
        LOG(FATAL) << "Failed to build server on port " << _port;
        exit(1);
    }
    LOG(INFO) << "Server listening on port " << _port;

    // 创建时间轮 timerfd（每秒 tick 一次）
    _timerFd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
    if (_timerFd < 0) {
        LOG(FATAL) << "timerfd_create failed";
        exit(1);
    }
    struct itimerspec its;
    its.it_value.tv_sec     = 1;
    its.it_value.tv_nsec    = 0;
    its.it_interval.tv_sec  = 1;
    its.it_interval.tv_nsec = 0;
    timerfd_settime(_timerFd, 0, &its, nullptr);

    // 绑定 Master 的 CQE 处理器
    _master.SetCQHandler([this](uint64_t userData, int cqeRes) {
        OnMasterCQE(userData, cqeRes);
    });

    _pool = new SlavePool(4);
}

inline TcpServer::~TcpServer() {
    _master.Stop();
    delete _pool;
    if (_timerFd >= 0) close(_timerFd);
}

inline void TcpServer::SetThreadsCount(int n) {
    delete _pool;
    _pool = new SlavePool(n);
}

// ── 启动 ──────────────────────────────────────────────────────────────────────

inline void TcpServer::Start() {
    _pool->Start();
    // 在进入 Run() 之前提交初始 accept 和 timerfd read SQE
    SubmitMasterAccept();
    SubmitMasterTimerRead();
    _master.FlushSubmits();  // flush initial accept + timer SQEs
    LOG(INFO) << "Master Proactor (io_uring) starting on main thread...";
    _master.Start();
}

// ── Master CQE 分发 ───────────────────────────────────────────────────────────

inline void TcpServer::OnMasterCQE(uint64_t userData, int cqeRes) {
    switch (GetOpType(userData)) {
    case OpType::ACCEPT: HandleAccept(cqeRes); break;
    case OpType::TIMER:  HandleTimer(cqeRes);  break;
    default: break;
    }
}

// ── Master 提交操作 ───────────────────────────────────────────────────────────

inline void TcpServer::SubmitMasterAccept() {
    _master.SubmitAccept(_listenSock.Fd(), 0);
}

inline void TcpServer::SubmitMasterTimerRead() {
    static uint64_t timerBuf = 0;
    _master.SubmitRead(_timerFd, &timerBuf, sizeof(uint64_t), 0, OpType::TIMER);
}

// ── accept 处理（io_uring 异步 accept，每次完成后重新提交）───────────────────

inline void TcpServer::HandleAccept(int newFd) {
    if (newFd < 0) {
        if (newFd != -EAGAIN && newFd != -EINTR) {
            LOG(ERROR) << "Accept error: " << strerror(-newFd);
        }
        SubmitMasterAccept();
        return;
    }

    // 设置非阻塞
    int flags = fcntl(newFd, F_GETFL, 0);
    fcntl(newFd, F_SETFL, flags | O_NONBLOCK);

    NewConnection(newFd);

    // Proactor 模式：accept 是一次性的，必须重新提交
    SubmitMasterAccept();
}

inline void TcpServer::NewConnection(int fd) {
    uint64_t id = Proactor::NextConnId();
    Proactor* slave = _pool->Next();

    auto conn = std::make_shared<Connection>(slave, id, fd);

    // 注入用户回调（同原版）
    conn->SetMessageCallback(_message_cb);
    conn->SetClosedCallback(_closed_cb);
    conn->SetConnectedCallback(_connected_cb);
    conn->SetAnyEventCallback(_any_cb);
    conn->SetSrvClosedCallback([this](const PtrConnection& c) {
        RemoveConnection(c);
    });

    // 加入全局表（Master 线程安全）
    _connections[id] = conn;

    if (_releaseOrNot) {
        conn->EnableInactiveRelease(_timeout);
        int slot = (_timerTick + _timeout) % TIMER_CAPACITY;
        _timerWheel[slot].push_back(conn);
        _connTimerSlot[id] = _timeout;
    }

    // 关键：AddConnection 必须和 GetConnection 在同一线程（Slave 线程）
    // 否则 std::unordered_map 并发 insert/find 导致 data race
    slave->PostTask([slave, id, conn]() {
        slave->AddConnection(id, conn);
        conn->Established();
    });

    LOG(DEBUG) << "New connection id=" << id << " fd=" << fd;
}

// ── 移除连接 ──────────────────────────────────────────────────────────────────

inline void TcpServer::RemoveConnection(const PtrConnection& conn) {
    _master.PostTask([this, conn]() { RemoveConnectionInMaster(conn); });
}

inline void TcpServer::RemoveConnectionInMaster(const PtrConnection& conn) {
    uint64_t id = conn->Id();
    _connections.erase(id);
    _connTimerSlot.erase(id);
    _delayTasks.erase(id);
    _delayCountdown.erase(id);
    LOG(DEBUG) << "Connection " << id << " removed, remaining: "
               << _connections.size();
}

// ── 时间轮处理（每秒 tick）────────────────────────────────────────────────────

inline void TcpServer::HandleTimer(int times) {
    if (times <= 0) { SubmitMasterTimerRead(); return; }

    for (int i = 0; i < times; ++i) {
        _timerTick = (_timerTick + 1) % TIMER_CAPACITY;

        // 1. 释放空闲超时的连接
        auto& bucket = _timerWheel[_timerTick];
        for (auto& conn : bucket) {
            if (conn && conn->IsConnected()) {
                LOG(DEBUG) << "Connection " << conn->Id() << " idle timeout";
                conn->Release();
            }
        }
        bucket.clear();

        // 2. 执行 RunAfter 延迟任务
        std::vector<uint64_t> doneIds;
        for (auto& [taskId, countdown] : _delayCountdown) {
            if (--countdown <= 0) doneIds.push_back(taskId);
        }
        for (auto id : doneIds) {
            _delayCountdown.erase(id);
            auto it = _delayTasks.find(id);
            if (it != _delayTasks.end()) {
                it->second();
                _delayTasks.erase(it);
            }
        }
    }

    // 处理 Master 的积压任务
    _master.ProcessTasks();

    SubmitMasterTimerRead();
}

// ── RunAfter（延迟执行回调）───────────────────────────────────────────────────

inline void TcpServer::RunAfter(const Functor& task, int delay) {
    ++_nextId;
    uint64_t taskId = _nextId;
    _delayTasks[taskId] = task;
    _delayCountdown[taskId] = delay;
}


// ══════════════════════════════════════════════════════════════════════════════
//  HTTP 协议层（移植自 like-muduo/source/http.hpp）
//  在 pa::TcpServer 之上提供 HTTP/1.1 服务端支持
// ══════════════════════════════════════════════════════════════════════════════

// ──── HTTP 状态码表 ───────────────────────────────────────────────────────────
inline std::unordered_map<int, std::string> _statu_msg = {
    {100, "Continue"}, {101, "Switching Protocol"}, {102, "Processing"},
    {103, "Early Hints"}, {200, "OK"}, {201, "Created"}, {202, "Accepted"},
    {203, "Non-Authoritative Information"}, {204, "No Content"},
    {205, "Reset Content"}, {206, "Partial Content"}, {207, "Multi-Status"},
    {208, "Already Reported"}, {226, "IM Used"},
    {300, "Multiple Choice"}, {301, "Moved Permanently"}, {302, "Found"},
    {303, "See Other"}, {304, "Not Modified"}, {305, "Use Proxy"},
    {306, "unused"}, {307, "Temporary Redirect"}, {308, "Permanent Redirect"},
    {400, "Bad Request"}, {401, "Unauthorized"}, {402, "Payment Required"},
    {403, "Forbidden"}, {404, "Not Found"}, {405, "Method Not Allowed"},
    {406, "Not Acceptable"}, {407, "Proxy Authentication Required"},
    {408, "Request Timeout"}, {409, "Conflict"}, {410, "Gone"},
    {411, "Length Required"}, {412, "Precondition Failed"},
    {413, "Payload Too Large"}, {414, "URI Too Long"},
    {415, "Unsupported Media Type"}, {416, "Range Not Satisfiable"},
    {417, "Expectation Failed"}, {418, "I'm a teapot"},
    {421, "Misdirected Request"}, {422, "Unprocessable Entity"},
    {423, "Locked"}, {424, "Failed Dependency"}, {425, "Too Early"},
    {426, "Upgrade Required"}, {428, "Precondition Required"},
    {429, "Too Many Requests"}, {431, "Request Header Fields Too Large"},
    {451, "Unavailable For Legal Reasons"},
    {501, "Not Implemented"}, {502, "Bad Gateway"},
    {503, "Service Unavailable"}, {504, "Gateway Timeout"},
    {505, "HTTP Version Not Supported"}, {506, "Variant Also Negotiates"},
    {507, "Insufficient Storage"}, {508, "Loop Detected"},
    {510, "Not Extended"}, {511, "Network Authentication Required"}
};

// ──── MIME 类型表 ─────────────────────────────────────────────────────────────
inline std::unordered_map<std::string, std::string> _mime_msg = {
    {".aac", "audio/aac"}, {".abw", "application/x-abiword"},
    {".arc", "application/x-freearc"}, {".avi", "video/x-msvideo"},
    {".azw", "application/vnd.amazon.ebook"}, {".bin", "application/octet-stream"},
    {".bmp", "image/bmp"}, {".bz", "application/x-bzip"},
    {".bz2", "application/x-bzip2"}, {".csh", "application/x-csh"},
    {".css", "text/css"}, {".csv", "text/csv"},
    {".doc", "application/msword"},
    {".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
    {".eot", "application/vnd.ms-fontobject"}, {".epub", "application/epub+zip"},
    {".gif", "image/gif"}, {".htm", "text/html"}, {".html", "text/html"},
    {".ico", "image/vnd.microsoft.icon"}, {".ics", "text/calendar"},
    {".jar", "application/java-archive"}, {".jpeg", "image/jpeg"},
    {".jpg", "image/jpeg"}, {".js", "text/javascript"},
    {".json", "application/json"}, {".jsonld", "application/ld+json"},
    {".mid", "audio/midi"}, {".midi", "audio/x-midi"},
    {".mjs", "text/javascript"}, {".mp3", "audio/mpeg"},
    {".mpeg", "video/mpeg"}, {".mpkg", "application/vnd.apple.installer+xml"},
    {".odp", "application/vnd.oasis.opendocument.presentation"},
    {".ods", "application/vnd.oasis.opendocument.spreadsheet"},
    {".odt", "application/vnd.oasis.opendocument.text"},
    {".oga", "audio/ogg"}, {".ogv", "video/ogg"}, {".ogx", "application/ogg"},
    {".otf", "font/otf"}, {".png", "image/png"}, {".pdf", "application/pdf"},
    {".ppt", "application/vnd.ms-powerpoint"},
    {".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
    {".rar", "application/x-rar-compressed"}, {".rtf", "application/rtf"},
    {".sh", "application/x-sh"}, {".svg", "image/svg+xml"},
    {".swf", "application/x-shockwave-flash"}, {".tar", "application/x-tar"},
    {".tif", "image/tiff"}, {".tiff", "image/tiff"}, {".ttf", "font/ttf"},
    {".txt", "text/plain"}, {".vsd", "application/vnd.visio"},
    {".wav", "audio/wav"}, {".weba", "audio/webm"}, {".webm", "video/webm"},
    {".webp", "image/webp"}, {".woff", "font/woff"}, {".woff2", "font/woff2"},
    {".xhtml", "application/xhtml+xml"}, {".xls", "application/vnd.ms-excel"},
    {".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
    {".xml", "application/xml"}, {".xul", "application/vnd.mozilla.xul+xml"},
    {".zip", "application/zip"}, {".3gp", "video/3gpp"},
    {".3g2", "video/3gpp2"}, {".7z", "application/x-7z-compressed"}
};

// ──── Util（静态工具方法）─────────────────────────────────────────────────────
class Util {
public:
    static void Split(const std::string &src, const std::string &deli,
                      std::vector<std::string> *v) {
        int offset = 0;
        while (offset < static_cast<int>(src.size())) {
            int pos = src.find(deli, offset);
            if (pos == static_cast<int>(std::string::npos)) {
                v->emplace_back(src.substr(offset));
                return;
            }
            if (pos - offset != 0) {
                v->emplace_back(src.substr(offset, pos - offset));
            }
            offset = pos + static_cast<int>(deli.size());
        }
    }

    static bool ReadFile(const std::string &fileName, std::string *s) {
        std::ifstream fin(fileName, std::ios::binary);
        if (!fin.is_open()) {
            LOG(ERROR) << "File open error: " << fileName;
            return false;
        }
        fin.seekg(0, std::ios::end);
        size_t fsize = static_cast<size_t>(fin.tellg());
        fin.seekg(0, std::ios::beg);
        s->resize(fsize);
        char *pos = &(*s)[0];
        fin.read(pos, static_cast<std::streamsize>(fsize));
        if (!fin.good()) {
            LOG(ERROR) << "reading from " << fileName << " error";
            return false;
        }
        fin.close();
        return true;
    }

    static void WriteFile(const std::string &fileName, const std::string &s) {
        std::ofstream fout(fileName, std::ios::binary | std::ios::trunc);
        if (!fout.is_open()) {
            LOG(ERROR) << "File open error: " << fileName;
            return;
        }
        fout.write(s.c_str(), static_cast<std::streamsize>(s.size()));
        if (!fout.good()) {
            LOG(ERROR) << "writing to " << fileName << " error";
        }
        fout.close();
    }

    // URL编码：将特殊字符编码为 %HH 格式，空格可选编码为 +
    static std::string UrlEncode(const std::string &url, bool convert_space_to_plus) {
        std::string res;
        for (char c : url) {
            if (c == '.' || c == '-' || c == '_' || c == '~' || isalnum(c)) {
                res += c;
            } else if (convert_space_to_plus && c == ' ') {
                res += '+';
            } else {
                char tmp[4] = {0};
                snprintf(tmp, 4, "%%%02x", static_cast<unsigned char>(c));
                res += tmp;
            }
        }
        return res;
    }

    static int HexToI(char c) {
        if (isupper(c)) return c - 'A' + 10;
        if (islower(c)) return c - 'a' + 10;
        if (isdigit(c)) return c - '0';
        return -1;
    }

    static std::string UrlDecode(const std::string &url, bool convert_plus_to_space) {
        std::string res;
        for (int i = 0; i < static_cast<int>(url.size()); ++i) {
            if (url[i] == '%' && i + 2 < static_cast<int>(url.size())) {
                int h = HexToI(url[i + 1]) << 4;
                int l = HexToI(url[i + 2]);
                res += static_cast<char>(h + l);
                i += 2;
            } else if (convert_plus_to_space && url[i] == '+') {
                res += ' ';
            } else {
                res += url[i];
            }
        }
        return res;
    }

    static std::string StatuDesc(int code) {
        auto iter = _statu_msg.find(code);
        return (iter != _statu_msg.end()) ? iter->second : "Unknown";
    }

    static std::string ExetMime(const std::string &name) {
        int pos = static_cast<int>(name.rfind('.'));
        if (pos == -1) return "application/octet-stream";
        std::string ext = name.substr(pos);
        auto iter = _mime_msg.find(ext);
        return (iter != _mime_msg.end()) ? iter->second : "application/octet-stream";
    }

    static bool IsDirectory(const std::string &filename) {
        struct stat st;
        if (stat(filename.c_str(), &st) < 0) return false;
        return S_ISDIR(st.st_mode);
    }

    static bool IsRegular(const std::string &filename) {
        struct stat st;
        if (stat(filename.c_str(), &st) < 0) return false;
        return S_ISREG(st.st_mode);
    }

    // 防止目录穿越攻击（如 /../../../etc/passwd）
    static bool ValidPath(const std::string &path) {
        std::vector<std::string> v;
        Split(path, "/", &v);
        int level = 0;
        for (auto &seg : v) {
            if (seg == "..") {
                if (--level < 0) return false;
            } else {
                ++level;
            }
        }
        return true;
    }
};

// ──── HttpRequest ─────────────────────────────────────────────────────────────
class HttpRequest {
public:
    std::string _method;
    std::string _url;
    std::string _version;
    std::string _body;
    std::smatch _matches;
    std::unordered_map<std::string, std::string> _headers;
    std::unordered_map<std::string, std::string> _parameters;

    HttpRequest() : _version("HTTP/1.1") {}

    void ReSet() {
        _method.clear();
        _url.clear();
        _version = "HTTP/1.1";
        _body.clear();
        std::smatch match;
        _matches.swap(match);
        _headers.clear();
        _parameters.clear();
    }

    void SetHeader(const std::string &key, const std::string &val) {
        _headers.insert(std::make_pair(key, val));
    }
    bool HasHeader(const std::string &key) const {
        return _headers.find(key) != _headers.end();
    }
    std::string GetHeader(const std::string &key) const {
        auto it = _headers.find(key);
        return (it != _headers.end()) ? it->second : "";
    }

    void SetParam(const std::string &key, const std::string &val) {
        _parameters.insert(std::make_pair(key, val));
    }
    bool HasParam(const std::string &key) const {
        return _parameters.find(key) != _parameters.end();
    }
    std::string GetParam(const std::string &key) const {
        auto it = _parameters.find(key);
        return (it != _parameters.end()) ? it->second : "";
    }

    size_t ContentLength() const {
        if (!HasHeader("Content-Length")) return 0;
        return std::stol(GetHeader("Content-Length"));
    }

    bool Close() const {
        if (HasHeader("Connection") && GetHeader("Connection") == "keep-alive")
            return false;
        return true;
    }
};

// ──── HttpResponse ────────────────────────────────────────────────────────────
class HttpResponse {
public:
    int _status;
    bool _redirect;
    std::string _redirect_url;
    std::string _body;
    std::unordered_map<std::string, std::string> _headers;

    HttpResponse() : _status(200), _redirect(false) {}
    explicit HttpResponse(int statu) : _status(statu), _redirect(false) {}

    void ReSet() {
        _status = 200;
        _redirect = false;
        _body.clear();
        _redirect_url.clear();
        _headers.clear();
    }

    void SetHeader(const std::string &key, const std::string &val) {
        _headers.insert(std::make_pair(key, val));
    }
    bool HasHeader(const std::string &key) {
        return _headers.find(key) != _headers.end();
    }
    std::string GetHeader(const std::string &key) {
        auto it = _headers.find(key);
        return (it != _headers.end()) ? it->second : "";
    }

    void SetContent(const std::string &body, const std::string &type = "text/html") {
        _body = body;
        SetHeader("Content-Type", type);
    }

    void SetRedirect(const std::string &url, int statu = 302) {
        _status = statu;
        _redirect = true;
        _redirect_url = url;
    }

    bool Close() {
        if (HasHeader("Connection") && GetHeader("Connection") == "keep-alive")
            return false;
        return true;
    }
};

// ──── HTTP 解析状态 ───────────────────────────────────────────────────────────
enum class HttpStatus {
    RECV_HTTP_LINE,
    RECV_HTTP_HEADER,
    RECV_HTTP_BODY,
    RECV_HTTP_ERROR,
    RECV_HTTP_OVER
};

#define MAX_LENGTH 8192

// ──── HttpContext（HTTP 协议解析器）────────────────────────────────────────────
class HttpContext {
private:
    int        _resp_statu;
    HttpStatus _recv_statu;
    HttpRequest _req;

public:
    HttpContext() : _resp_statu(200), _recv_statu(HttpStatus::RECV_HTTP_LINE) {}

    HttpStatus  RecvStatus()  { return _recv_statu; }
    int         RespStatus()  { return _resp_statu; }
    HttpRequest& GetReq()     { return _req; }

    void Reset() {
        _resp_statu = 200;
        _recv_statu = HttpStatus::RECV_HTTP_LINE;
        _req.ReSet();
    }

    void RecvHttpReq(Buffer* buf) {
        switch (_recv_statu) {
            case HttpStatus::RECV_HTTP_ERROR:
            case HttpStatus::RECV_HTTP_OVER:
                return;
        case HttpStatus::RECV_HTTP_LINE:
            RecvHttpLine(buf);
            // fall through: 如果一行数据里包含了多行，继续解析头部
            if (_recv_statu == HttpStatus::RECV_HTTP_LINE) return;
            [[fallthrough]];
        case HttpStatus::RECV_HTTP_HEADER:
            RecvHttpHead(buf);
            if (_recv_statu == HttpStatus::RECV_HTTP_HEADER) return;
            [[fallthrough]];
        case HttpStatus::RECV_HTTP_BODY:
            RecvBody(buf);
        }
    }

private:
    bool RecvHttpLine(Buffer* buf) {
        if (_recv_statu != HttpStatus::RECV_HTTP_LINE) return false;
        std::string line = buf->GetLineAndPop();
        if (line.empty()) {
            if (buf->ReadableSize() > MAX_LENGTH) {
                _recv_statu = HttpStatus::RECV_HTTP_ERROR;
                _resp_statu = 414;
            }
            return true;  // 还没收到完整的行，等待更多数据
        }
        if (static_cast<int>(line.size()) > MAX_LENGTH) {
            _recv_statu = HttpStatus::RECV_HTTP_ERROR;
            _resp_statu = 414;
            return false;
        }
        if (!ParseHttpLine(line)) return false;
        _recv_statu = HttpStatus::RECV_HTTP_HEADER;
        return true;
    }

    bool ParseHttpLine(const std::string& line) {
        std::smatch matches;
        std::regex e("(GET|HEAD|POST|PUT|DELETE) ([^?]*)(?:\\?(.*))? (HTTP/1\\.[01])(?:\n|\r\n)?",
                     std::regex::icase);
        if (!std::regex_match(line, matches, e)) {
            _recv_statu = HttpStatus::RECV_HTTP_ERROR;
            _resp_statu = 400;
            return false;
        }
        // 0: GET /path?query HTTP/1.1
        // 1: GET    2: /path    3: query    4: HTTP/1.1
        _req._method = matches[1];
        std::transform(_req._method.begin(), _req._method.end(),
                       _req._method.begin(), ::toupper);
        _req._url     = matches[2];
        _req._version = matches[4];

        // 解析查询字符串
        std::vector<std::string> query_arr;
        std::string query = matches[3];
        Util::Split(query, "&", &query_arr);
        for (auto &str : query_arr) {
            size_t pos = str.find("=");
            if (pos == std::string::npos) {
                _recv_statu = HttpStatus::RECV_HTTP_ERROR;
                _resp_statu = 400;
                return false;
            }
            std::string key = Util::UrlDecode(str.substr(0, pos), true);
            std::string val = Util::UrlDecode(str.substr(pos + 1), true);
            _req.SetParam(key, val);
        }
        return true;
    }

    bool RecvHttpHead(Buffer* buf) {
        if (_recv_statu != HttpStatus::RECV_HTTP_HEADER) return false;
        while (true) {
            std::string line = buf->GetLineAndPop();
            if (line.empty()) {
                if (buf->ReadableSize() > MAX_LENGTH) {
                    _recv_statu = HttpStatus::RECV_HTTP_ERROR;
                    _resp_statu = 414;
                    return false;
                }
                return true;  // 头部还没收完
            }
            if (static_cast<int>(line.size()) > MAX_LENGTH) {
                _recv_statu = HttpStatus::RECV_HTTP_ERROR;
                _resp_statu = 414;
                return false;
            }
            if (line == "\n" || line == "\r\n") break;  // 空行 = 头部结束
            if (!ParseHttpHead(line)) return false;
        }
        _recv_statu = HttpStatus::RECV_HTTP_BODY;
        return true;
    }

    bool ParseHttpHead(std::string& line) {
        // key: val\r\n
        if (line.back() == '\n') line.pop_back();
        if (!line.empty() && line.back() == '\r') line.pop_back();
        int pos = static_cast<int>(line.find(": "));
        if (pos == -1) {
            _recv_statu = HttpStatus::RECV_HTTP_ERROR;
            _resp_statu = 400;
            return false;
        }
        std::string key = line.substr(0, pos);
        std::string val = line.substr(pos + 2);
        _req.SetHeader(key, val);
        return true;
    }

    bool RecvBody(Buffer* buf) {
        if (_recv_statu != HttpStatus::RECV_HTTP_BODY) return false;
        size_t len = _req.ContentLength();
        if (len == 0) {
            _recv_statu = HttpStatus::RECV_HTTP_OVER;
            return true;
        }
        size_t remain = len - _req._body.size();
        if (static_cast<size_t>(buf->ReadableSize()) >= remain) {
            _req._body.append(buf->GetReadPos(), remain);
            buf->MoveReadOffset(static_cast<int>(remain));
            _recv_statu = HttpStatus::RECV_HTTP_OVER;
            return true;
        }
        int n = buf->ReadableSize();
        _req._body.append(buf->GetReadPos(), n);
        buf->MoveReadOffset(n);
        return true;
    }
};

// ──── HttpServer（HTTP 服务器）─────────────────────────────────────────────────
class HttpServer {
public:
    using Handler_t = std::function<void(const HttpRequest&, HttpResponse*)>;

private:
    using Router_t = std::vector<std::pair<std::regex, Handler_t>>;

    int                _port;
    Router_t           _get_router;
    Router_t           _post_router;
    Router_t           _put_router;
    Router_t           _delete_router;
    std::string        _asset_dir;
    TcpServer          _server;

    // ── 组织 HTTP 响应并发送 ─────────────────────────────────────────────────
    void WriteResponse(const PtrConnection& conn, const HttpRequest& req,
                       HttpResponse& res) {
        std::stringstream s;
        s << req._version << " " << res._status << " "
          << Util::StatuDesc(res._status) << "\r\n";

        if (req.Close()) res.SetHeader("Connection", "close");
        else             res.SetHeader("Connection", "keep-alive");

        if (!res._body.empty() && !res.HasHeader("Content-Length")) {
            res.SetHeader("Content-Length", std::to_string(res._body.size()));
        }
        if (!res._body.empty() && !res.HasHeader("Content-Type")) {
            res.SetHeader("Content-Type", "application/octet-stream");
        }
        if (res._redirect) {
            res.SetHeader("Location", res._redirect_url);
        }

        for (auto &kv : res._headers) {
            s << kv.first << ": " << kv.second << "\r\n";
        }
        s << "\r\n" << res._body;
        conn->Send(s.str().c_str(), s.str().size());
    }

    // ── 判断是否为静态资源请求 ───────────────────────────────────────────────
    bool IsStatic(HttpRequest& req) {
        if (_asset_dir.empty() || !Util::ValidPath(req._url)) return false;
        if (req._method != "GET" && req._method != "HEAD")   return false;
        if (req._url.back() == '/') req._url.append("index.html");
        std::string path = _asset_dir + req._url;
        return Util::IsRegular(path);
    }

    void StaticHandler(const HttpRequest& req, HttpResponse* res) {
        LOG(INFO) << "serving static file: " << req._url;
        std::string path = _asset_dir + req._url;
        if (!Util::ReadFile(path, &res->_body)) return;
        std::string mime = Util::ExetMime(path);
        res->SetHeader("Content-Type", mime);
    }

    // ── 路由分发 ─────────────────────────────────────────────────────────────
    void Dispatcher(HttpRequest& req, HttpResponse* res, const Router_t& funcs) {
        for (auto &t : funcs) {
            if (std::regex_match(req._url, req._matches, t.first)) {
                return t.second(req, res);
            }
        }
        res->_status = 404;
    }

    void Router(HttpRequest& req, HttpResponse* res) {
        if (IsStatic(req)) {
            return StaticHandler(req, res);
        }
        if (req._method == "GET" || req._method == "HEAD") {
            return Dispatcher(req, res, _get_router);
        } else if (req._method == "POST") {
            return Dispatcher(req, res, _post_router);
        } else if (req._method == "PUT") {
            return Dispatcher(req, res, _put_router);
        } else if (req._method == "DELETE") {
            return Dispatcher(req, res, _delete_router);
        }
        res->_status = 405;
    }

    // ── 错误页面 ─────────────────────────────────────────────────────────────
    void ErrorHandler(const HttpRequest& /*req*/, HttpResponse* res) {
        std::string body;
        body += "<html><head>";
        body += "<meta http-equiv='Content-Type' content='text/html;charset=utf-8'>";
        body += "</head><body>";
        body += "<h1>" + std::to_string(res->_status) + " ";
        body += Util::StatuDesc(res->_status) + "</h1>";
        body += "</body></html>";
        res->SetContent(body, "text/html");
    }

    // ── TcpServer 回调 ───────────────────────────────────────────────────────
    void OnConnected(const PtrConnection& conn) {
        conn->SetContext(HttpContext());
    }

    void OnMessage(const PtrConnection& conn, Buffer* buf) {
        while (buf->ReadableSize() > 0) {
            HttpContext* context = conn->GetContext()->get<HttpContext>();
            context->RecvHttpReq(buf);

            HttpRequest& req = context->GetReq();
            HttpResponse res(context->RespStatus());

            // HTTP 解析错误
            if (context->RespStatus() >= 400) {
                ErrorHandler(req, &res);
                WriteResponse(conn, req, res);
                context->Reset();
                buf->MoveReadOffset(buf->ReadableSize());
                conn->Shutdown();
                return;
            }

            // 请求尚未接收完整
            if (context->RecvStatus() != HttpStatus::RECV_HTTP_OVER) {
                return;
            }

            // 路由处理 + 响应
            Router(req, &res);
            WriteResponse(conn, req, res);
            context->Reset();

            if (res.Close()) {
                conn->Shutdown();
                return;
            }
        }
    }

public:
    explicit HttpServer(int port, int timeout = DEFAULT_TIMEOUT)
        : _port(port), _asset_dir("./wwwroot/"), _server(port)
    {
        _server.SetConnectedCallback(
            std::bind(&HttpServer::OnConnected, this, std::placeholders::_1));
        _server.SetMessageCallback(
            std::bind(&HttpServer::OnMessage, this,
                      std::placeholders::_1, std::placeholders::_2));
        _server.EnableInactiveRelease(timeout);
    }

    void AddGetMethod(const std::string& pattern, Handler_t handler) {
        _get_router.emplace_back(std::regex(pattern), std::move(handler));
    }
    void AddPostMethod(const std::string& pattern, Handler_t handler) {
        _post_router.emplace_back(std::regex(pattern), std::move(handler));
    }
    void AddPutMethod(const std::string& pattern, Handler_t handler) {
        _put_router.emplace_back(std::regex(pattern), std::move(handler));
    }
    void AddDeleteMethod(const std::string& pattern, Handler_t handler) {
        _delete_router.emplace_back(std::regex(pattern), std::move(handler));
    }

    void SetAssetDir(const std::string& dir) { _asset_dir = dir; }
    void SetThreadCount(int count)            { _server.SetThreadsCount(count); }
    void Start()                              { _server.Start(); }
};

// ──── SIGPIPE 抑制（静态初始化）───────────────────────────────────────────────
class netWork {
public:
    netWork() { signal(SIGPIPE, SIG_IGN); }
};
static netWork nw;

} // namespace pa

#endif // PA_SERVER_HPP
