#include "Buffer.h"

namespace pa{
class Buffer
{
private:
    int _write_idx;
    int _read_idx;
    std::vector<char> _buffer;

public:
    Buffer():_write_idx(0),_read_idx(0),_buffer(DEFAULTSIZE){}
    char *GetWritePos() { return &_buffer[0] + _write_idx; }
    const char *GetReadPos() const{ return &_buffer[0] + _read_idx; }
    int Capacity() { return _buffer.size(); }
    int PostIdle() { return Capacity() - _write_idx; }
    int PreIdle() { return _read_idx; }
    int ReadableSize() const{ return _write_idx - _read_idx; }
    void MoveReadOffset(int len) {
        assert(len <= ReadableSize());
        _read_idx += len;
    }
    void MoveWriteOffset(int len) {
        if(len>=PostIdle()){
            std::cout << "len: " << len << " PostIdle: " << PostIdle();
			 
            abort();
        }
        _write_idx += len;
    }
    void Clear() { _read_idx = _write_idx = 0; }
    void EnsureWrite(int len){
        if(PostIdle() > len){
            return;
        }
        if(PreIdle()+PostIdle()>len){
            size_t size = ReadableSize();
            std::copy(GetReadPos(), GetReadPos() + ReadableSize(), _buffer.begin());
            _read_idx = 0;
            _write_idx = size;
        }else{
            _buffer.resize(Capacity() + 2*len);
        }
    }
    int Write(const void* data,int len){
        EnsureWrite(len);
        const char *Data = (const char*)data;
        std::copy(Data, Data + len, GetWritePos());
        return len;
    }
    int Write(const Buffer& other){
        return Write(other.GetReadPos(), other.ReadableSize());
    }
    int Write(const std::string& s){
        return Write(s.c_str(), s.size());
    }
    int WriteAndPush(const void* data,int len){
        Write(data, len);
        MoveWriteOffset(len);
        return len;
    }
    int WriteAndPush(const std::string& str){
        return WriteAndPush(str.c_str(), str.size());
    }
    int WriteAndPush(const Buffer& other){
        return WriteAndPush(other.GetReadPos(), other.ReadableSize());
    }
    int Read(void* d,int len){
        int lenth = std::min(ReadableSize(), len);
        char *data = (char *)d;
        std::copy(GetReadPos(), GetReadPos() + lenth, data);
        return lenth;
    }
    int Read(std::string* out){
        if(out->empty())
            return 0;
        char *ot = &(*out)[0];
        return Read(ot, out->size());
    }
    int ReadAndPop(void* d,int len){
        int l = Read(d, len);
        MoveReadOffset(l);
        return l;
    }
    int ReadAndPop(std::string* out){
        if(out->empty())
            return 0;
        int len = out->size();
        char bu[len];
        int lenth = ReadAndPop(bu,len);
        *out = bu;
        return lenth;
    }
    char* FindCRLF(){
        char *pos = (char *)memchr(GetReadPos(), '\n', ReadableSize());
        return pos;
    }
    std::string GetLine(){
        char *pos = FindCRLF();
        if (pos == nullptr)
            return "";
        int len = pos - GetReadPos() + 1;
        char buf[len+1];
        Read(buf, len);
        buf[len] = 0;
        return buf;
    }
    std::string GetLineAndPop(){
        std::string line = GetLine();
        int len = line.size();
        MoveReadOffset(len);
        return line;
    }
};
}//namespace pa