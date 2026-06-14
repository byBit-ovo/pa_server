#include "server.hpp"
#include <iostream>
#include <signal.h>

using namespace pa;

// ──── 简单 JSON 序列化（避免引入第三方库）──────────────────────────────────────
inline std::string jsonEscape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n";  break;
        case '\r': out += "\\r";  break;
        case '\t': out += "\\t";  break;
        default:   out += c;
        }
    }
    return out;
}

int main() {
    // 不注册 SIGINT 处理器——保留默认行为，Ctrl+C 直接终止进程
    signal(SIGPIPE, SIG_IGN);  // 防止向已关闭连接写入时触发 SIGPIPE

    const int  PORT       = 8080;
    const int  THREADS    = 3;        // 根据 CPU 核数调整
    const int  IDLE_SEC   = 30;       // 空闲连接超时

    // ── 创建 HTTP 服务器 ──────────────────────────────────────────────────────
    HttpServer server(PORT, IDLE_SEC);
    server.SetThreadCount(THREADS);
    server.SetAssetDir("./wwwroot/");

    // ── 注册路由 ──────────────────────────────────────────────────────────────

    // 1. GET /hello — 纯文本压测（wrk -t4 -c100 -d30s http://host:8080/hello）
    server.AddGetMethod("/hello", [](const HttpRequest& req, HttpResponse* resp) {
        (void)req;
        resp->SetContent("Hello, world!\n", "text/plain");
    });

    // 2. GET /json — JSON 响应压测
    server.AddGetMethod("/json", [](const HttpRequest& req, HttpResponse* resp) {
        (void)req;
        std::string body = "{\"status\":\"ok\",\"message\":\"Hello from pa_server\"}";
        resp->SetContent(body, "application/json");
    });

    // 3. POST /echo — 回显请求正文（测试读写混合）
    server.AddPostMethod("/echo", [](const HttpRequest& req, HttpResponse* resp) {
        resp->SetContent(req._body, req.GetHeader("Content-Type"));
    });

    // 4. GET /params?key=val — 查询字符串解析测试
    server.AddGetMethod("/params", [](const HttpRequest& req, HttpResponse* resp) {
        std::string body = "{";
        bool first = true;
        for (auto& [k, v] : req._parameters) {
            if (!first) body += ",";
            first = false;
            body += "\"" + jsonEscape(k) + "\":\"" + jsonEscape(v) + "\"";
        }
        body += "}";
        resp->SetContent(body, "application/json");
    });

    // 5. POST /upload — 测量请求体接收速度
    server.AddPostMethod("/upload", [](const HttpRequest& req, HttpResponse* resp) {
        resp->SetContent(
            "{\"received\":" + std::to_string(req._body.size()) + "}",
            "application/json");
    });

    // 6. PUT /put — PUT 方法测试
    server.AddPutMethod("/put", [](const HttpRequest& req, HttpResponse* resp) {
        resp->SetContent(
            "{\"method\":\"PUT\",\"body_size\":" + std::to_string(req._body.size()) + "}",
            "application/json");
    });

    // 7. DELETE /delete — DELETE 方法测试
    server.AddDeleteMethod("/delete", [](const HttpRequest& req, HttpResponse* resp) {
        (void)req;
        resp->SetContent("{\"status\":\"deleted\"}", "application/json");
    });

    // ── 打印启动信息 ──────────────────────────────────────────────────────────
    std::cout << "\n"
              << "╔══════════════════════════════════════════════════════╗\n"
              << "║   pa_server — io_uring Master-Slave Proactor        ║\n"
              << "║   HTTP/1.1 Server                                   ║\n"
              << "╠══════════════════════════════════════════════════════╣\n"
              << "║   Port:     " << PORT << "                                       ║\n"
              << "║   Threads:  " << THREADS << "                                         ║\n"
              << "║   Idle:     " << IDLE_SEC << "s                                       ║\n"
              << "╠══════════════════════════════════════════════════════╣\n"
              << "║   Routes:                                           ║\n"
              << "║     GET  /hello          plain text                 ║\n"
              << "║     GET  /json           JSON response              ║\n"
              << "║     GET  /params?k=v     query string test          ║\n"
              << "║     POST /echo           echo body                  ║\n"
              << "║     POST /upload         count body size            ║\n"
              << "║     PUT  /put            PUT test                   ║\n"
              << "║     DELETE /delete       DELETE test                ║\n"
              << "║     GET  /*              static files (./wwwroot/)  ║\n"
              << "╠══════════════════════════════════════════════════════╣\n"
              << "║   Benchmark:                                        ║\n"
              << "║     wrk -t4 -c100 -d30s http://127.0.0.1:8080/hello ║\n"
              << "║     ab -n 100000 -c 100 http://127.0.0.1:8080/hello ║\n"
              << "╚══════════════════════════════════════════════════════╝\n"
              << std::endl;

    // ── 启动 ──────────────────────────────────────────────────────────────────
    server.Start();

    return 0;
}
