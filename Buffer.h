#include <string>
#include <vector>
#include <assert.h>
#include "Logger.hpp"
namespace pa{
#define DEFAULT_SIZE 1024
class Buffer
{
private:
    int _write_idx;
    int _read_idx;
    std::vector<char> _buffer;

public:
    Buffer():_write_idx(0),_read_idx(0),_buffer(DEFAULT_SIZE){}
    inline char *GetWritePos() { return &_buffer[0] + _write_idx; }
    inline const char *GetReadPos() const{ return &_buffer[0] + _read_idx; }
    inline int Capacity() { return _buffer.size(); }
    inline int PostIdle() { return Capacity() - _write_idx; }
    inline int PreIdle() { return _read_idx; }
    inline int ReadableSize() const{ return _write_idx - _read_idx; }
    inline void MoveReadOffset(int len) {
        assert(len <= ReadableSize());
        _read_idx += len;
    }
    inline void MoveWriteOffset(int len) {
        if(len>=PostIdle()){
            LOG(logLevel::DEBUG) << "len: " << len << " PostIdle: " << PostIdle();
            abort();
        }
        _write_idx += len;
    }
    inline void Clear() { _read_idx = _write_idx = 0; }
    void EnsureWrite(int len);
    int Write(const void* data,int len);
    int Write(const Buffer& other);
    int Write(const std::string& s);
    int WriteAndPush(const void* data,int len);
    int WriteAndPush(const std::string& str);
    int WriteAndPush(const Buffer& other);
    int Read(void* d,int len);
    int Read(std::string* out);
    int ReadAndPop(void* d,int len);
    int ReadAndPop(std::string* out);
    char* FindCRLF();
    std::string GetLine();
    std::string GetLineAndPop();
};
}//namespace pa