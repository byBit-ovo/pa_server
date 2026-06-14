CXX      := g++
CXXFLAGS := -std=c++17 -O2 -I.
LDFLAGS  := -luring -lpthread -rdynamic

TARGET := pa_server
SRC    := test.cc

$(TARGET): $(SRC) server.hpp
	$(CXX) $(CXXFLAGS) $(SRC) -o $@ $(LDFLAGS)

.PHONY: clean run bench debug

# 带调试日志的版本
debug: CXXFLAGS := -std=c++17 -O2 -DPA_DEBUG -I.
debug: $(TARGET)

clean:
	rm -f $(TARGET)

run: $(TARGET)
	./$(TARGET)
