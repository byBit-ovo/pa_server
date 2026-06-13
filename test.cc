#include <liburing.h>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>
#include <string.h>
#include <thread>
#include <sys/socket.h>
#include <memory>
const char* book_file = "book.txt";
const char book_name[] = "认知觉醒\n非暴力沟通\n被讨厌的勇气";
using std::cout;
using std::endl;
using std::cerr;

int main()
{

	
    int sv[2];  // sv[0] 读端，sv[1] 写端
    
    // 创建一对连接的 socket（类似管道，但更适合 io_uring）
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        perror("socketpair failed");
        return 1;
    }
    
    char buff[200] = {0};

    io_uring ring;
    if (io_uring_queue_init(64, &ring, 0) != 0) {
        perror("init queue error");
        return 1;
    }
	// write(sv[1], "abcde", 3);
    // 写线程：10秒后写入数据
    std::thread t([&]() {
        sleep(5);
        cout << "开始写入数据..." << endl;
        int bytes_written = write(sv[1], book_name, sizeof(book_name));
        cout << "写入 " << bytes_written << " 字节" << endl;
        close(sv[1]);  // 写完关闭写端
    });

    // 主线程：提交异步读请求

	// 永远不会阻塞，只会返回 NULL 表示 SQ 已满。
    io_uring_sqe* sqe_r = io_uring_get_sqe(&ring);
    io_uring_sqe* sqe_w = io_uring_get_sqe(&ring);
    
    // 注意：socket 的读操作如果没有数据，会阻塞等待（而不是立即返回 EOF）
    io_uring_prep_read(sqe_r, sv[0], buff, sizeof(buff), 0);
	io_uring_prep_write(sqe_w, sv[1], "hello", 5, 0);
    
    io_uring_submit(&ring);
    cout << "读请求已提交，等待数据..." << endl;
    
    // 阻塞等待读完成（会真正等待直到写入线程写入数据）
    io_uring_cqe* cqe;
	cout << "waiting in io_uring_wait_cqe" << endl;
    io_uring_wait_cqe(&ring, &cqe);
    
    if (cqe->res < 0) {
        cerr << "read failed: " << strerror(-cqe->res) << endl;
        return 1;
    }
    
    int bytes_read = cqe->res;
    buff[bytes_read] = '\0';
    io_uring_cqe_seen(&ring, cqe);
    
    cout << "读取 " << bytes_read << " 字节" << endl;
    cout << "内容：\n" << buff << endl;
    
    close(sv[0]);
    t.join();
    
    io_uring_queue_exit(&ring);
    return 0;
}