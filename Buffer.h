#include <string>
#include <vector>
#include <assert.h>
#include "Logger.hpp"
namespace pa{
#define DEFAULT_SIZE 1024
class Buffer
{
private:
    int m_write_idx;
    int m_read_idx;
    std::vector<char> m_buffer;

public:
    Buffer();
	int WriteAndPush(const void* data,int len);
    int WriteAndPush(const std::string& str);
    int WriteAndPush(const Buffer& other);
	int ReadAndPop(void* d,int len);
    int ReadAndPop(std::string* out);
private:
    inline char *GetWritePositon() { return &m_buffer[0] + m_write_idx; }
    inline const char *GetReadPos() const{ return &m_buffer[0] + m_read_idx; }
    inline int Capacity() { return m_buffer.size(); }
    inline int PostIdle() { return Capacity() - m_write_idx; }
    inline int PreIdle() { return m_read_idx; }
    inline int ReadableSize() const{ return m_write_idx - m_read_idx; }
    inline void MoveReadOffset(int len) {
        assert(len <= ReadableSize());
        m_read_idx += len;
    }
    inline void MoveWriteOffset(int len) {
        if(len>=PostIdle()){
            LOG(logLevel::DEBUG) << "len: " << len << " PostIdle: " << PostIdle();
            abort();
        }
        m_write_idx += len;
    }
    inline void Clear() { m_read_idx = m_write_idx = 0; }
    void EnsureWrite(int len);
    int Write(const void* data,int len);
    int Write(const Buffer& other);
    int Write(const std::string& s);
    int Read(void* d,int len);
    int Read(std::string* out);
    char* FindCRLF();
    std::string GetLine();
    std::string GetLineAndPop();
};
}//namespace pa