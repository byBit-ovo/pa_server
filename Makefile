CXX      := g++
CXXFLAGS := -std=c++17 -O2 -I.
LDFLAGS  := -luring -lpthread -rdynamic

# ─── 两个目标：epoll+uring 混合 vs 纯 uring ──────────────────────────────
TARGET_EPOLL  := pa_server_epoll
TARGET_URING  := pa_server_uring
SRC_EPOLL     := test_epoll.cc
SRC_URING     := test_uring.cc

.PHONY: all clean run-epoll run-uring bench-epoll bench-uring debug-epoll debug-uring

all: $(TARGET_EPOLL) $(TARGET_URING)

$(TARGET_EPOLL): $(SRC_EPOLL) server_epoll.hpp
	$(CXX) $(CXXFLAGS) $(SRC_EPOLL) -o $@ $(LDFLAGS)

$(TARGET_URING): $(SRC_URING) server_uring.hpp
	$(CXX) $(CXXFLAGS) $(SRC_URING) -o $@ $(LDFLAGS)

# 带调试日志的版本
debug-epoll: CXXFLAGS := -std=c++17 -O2 -DPA_DEBUG -I.
debug-epoll: $(TARGET_EPOLL)

debug-uring: CXXFLAGS := -std=c++17 -O2 -DPA_DEBUG -I.
debug-uring: $(TARGET_URING)

clean:
	rm -f $(TARGET_EPOLL) $(TARGET_URING)

run-epoll: $(TARGET_EPOLL)
	./$(TARGET_EPOLL)

run-uring: $(TARGET_URING)
	./$(TARGET_URING)

# 保留兼容性：默认 debug 构建 epoll 版本
debug: debug-epoll

# 保留旧的 TARGET 名兼容
pa_server: $(TARGET_EPOLL)
	cp $(TARGET_EPOLL) pa_server
