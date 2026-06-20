#include "server_uring.hpp"
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
    signal(SIGPIPE, SIG_IGN);

    const int  PORT       = 8080;
    const int  THREADS    = 3;
    const int  IDLE_SEC   = 30;

    HttpServer server(PORT, IDLE_SEC);
    server.SetThreadCount(THREADS);
    server.SetAssetDir("./wwwroot/");

    server.AddGetMethod("/hello", [](const HttpRequest& req, HttpResponse* resp) {
        (void)req;
        resp->SetContent("Hello, world!\n", "text/plain");
    });

    server.AddGetMethod("/json", [](const HttpRequest& req, HttpResponse* resp) {
        (void)req;
        std::string body = "{\"status\":\"ok\",\"message\":\"Hello from pa_server\"}";
        resp->SetContent(body, "application/json");
    });

    server.AddPostMethod("/echo", [](const HttpRequest& req, HttpResponse* resp) {
        resp->SetContent(req._body, req.GetHeader("Content-Type"));
    });

    server.AddPostMethod("/upload", [](const HttpRequest& req, HttpResponse* resp) {
        resp->SetContent(
            "{\"received\":" + std::to_string(req._body.size()) + "}",
            "application/json");
    });

    server.AddPutMethod("/put", [](const HttpRequest& req, HttpResponse* resp) {
        resp->SetContent(
            "{\"method\":\"PUT\",\"body_size\":" + std::to_string(req._body.size()) + "}",
            "application/json");
    });

    server.AddDeleteMethod("/delete", [](const HttpRequest& req, HttpResponse* resp) {
        (void)req;
        resp->SetContent("{\"status\":\"deleted\"}", "application/json");
    });

    std::cout << "\n"
              << "  pa_server (pure io_uring) — Master + Slave 全 io_uring\n"
              << "  Port: " << PORT << "  Threads: " << THREADS << "\n"
              << std::endl;

    server.Start();
    return 0;
}
