#include "Buffer.h"

namespace pa{

    Buffer::Buffer():m_write_idx(0),m_read_idx(0),m_buffer(DEFAULT_SIZE){}

    void Buffer::EnsureWrite(int len){
        if(PostIdle() > len){
            return;
        }
        if(PreIdle()+PostIdle()>len){
            size_t size = ReadableSize();
            std::copy(GetReadPos(), GetReadPos() + ReadableSize(), m_buffer.begin());
            m_read_idx = 0;
            m_write_idx = size;
        }else{
            m_buffer.resize(Capacity() + 2*len);
        }
    }
    int Buffer::Write(const void* data,int len){
        EnsureWrite(len);
        const char *Data = (const char*)data;
        std::copy(Data, Data + len, GetWritePositon());
        return len;
    }
    int Buffer::Write(const Buffer& other){
        return Write(other.GetReadPos(), other.ReadableSize());
    }
    int Buffer::Write(const std::string& s){
        return Write(s.c_str(), s.size());
    }
    int Buffer::WriteAndPush(const void* data,int len){
        Write(data, len);
        MoveWriteOffset(len);
        return len;
    }
    int Buffer::WriteAndPush(const std::string& str){
        return WriteAndPush(str.c_str(), str.size());
    }
    int Buffer::WriteAndPush(const Buffer& other){
        return WriteAndPush(other.GetReadPos(), other.ReadableSize());
    }
    int Buffer::Read(void* d,int len){
        int lenth = std::min(ReadableSize(), len);
        char *data = (char *)d;
        std::copy(GetReadPos(), GetReadPos() + lenth, data);
        return lenth;
    }
    int Buffer::Read(std::string* out){
        if(out->empty())
            return 0;
        char *ot = &(*out)[0];
        return Read(ot, out->size());
    }
    int Buffer::ReadAndPop(void* d,int len){
        int l = Read(d, len);
        MoveReadOffset(l);
        return l;
    }
    int Buffer::ReadAndPop(std::string* out){
        if(out->empty())
            return 0;
        int len = out->size();
        char bu[len];
        int lenth = ReadAndPop(bu,len);
        *out = bu;
        return lenth;
    }
    char* Buffer::FindCRLF(){
        char *pos = (char *)memchr(GetReadPos(), '\n', ReadableSize());
        return pos;
    }
    std::string Buffer::GetLine(){
        char *pos = FindCRLF();
        if (pos == nullptr)
            return "";
        int len = pos - GetReadPos() + 1;
        char buf[len+1];
        Read(buf, len);
        buf[len] = 0;
        return buf;
    }
    std::string Buffer::GetLineAndPop(){
        std::string line = GetLine();
        int len = line.size();
        MoveReadOffset(len);
        return line;
    }

}//namespace pa