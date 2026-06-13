#pragma once
#include "mutex.hpp"
#include <fstream>
#include <filesystem>
#include <time.h>
#include <string>
#include <cstring>

const std::string dftPath = "./log/";
const std::string dftName = "log.txt";
std::string toLevel[5] = {"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"};

std::string getTime()
{
    struct tm cur_time;
    time_t time_stamp = time(nullptr);
    char buffer[1024];
    if (localtime_r(&time_stamp, &cur_time) != nullptr)
    {
        // ssbuffer <<cur_time.tm_year+1900 <<'.'
        //          <<cur_time.tm_mon+1     <<'.'
        //          <<cur_time.tm_mday;
        // return ssbuffer.str();
        snprintf(buffer, sizeof(buffer), "%d-%02d-%02d %02d:%02d:%02d", cur_time.tm_year + 1900, cur_time.tm_mon + 1,
                cur_time.tm_mday, cur_time.tm_hour, cur_time.tm_min, cur_time.tm_sec);
        return buffer;
    }
    return "";
}
class sync_base
{
public:
    virtual ~sync_base() = default;
    virtual void fsync(std::string &message) = 0;
};

class consoleLog : public sync_base
{
public:
    ~consoleLog() override {}
    void fsync(std::string &message) override
    {
        lockGuard guard(_lock);
        std::cout << message << std ::endl;
    }

private:
    mutex _lock;
};

class fileLog : public sync_base
{
public:
    virtual ~fileLog() override {}
    fileLog(const std::string &path = dftPath, const std::string &filename = dftName) : _path(path), _fileName(filename)
    {
        lockGuard guard(_lock); // 创建路径
        if (std::filesystem::exists(_path))
        {
            return;
        }
        try
        {
            std::filesystem::create_directories(_path);
        }
        catch (std::filesystem::filesystem_error &e)
        {
            std::cerr << e.what() << std::endl;
        }
    }
    void fsync(std::string &message) override
    {
        lockGuard guard(_lock);
        std::string filePos = _path + _fileName;
        std::ofstream ofs(filePos, std::ios::app);
        if (!ofs.is_open())
        {
            std::cout << "logfile error" << std::endl;
            return;
        }
        ofs << message << std ::endl;
        ofs.close();
    }
private:
    std::string _path;
    std::string _fileName;
    mutex _lock;
};
enum class logLevel
{
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    FATAL,
};
class logger
{
public:
    logger()
    {
        _log = std::make_shared<consoleLog>();
    }
    void to_file(const std::string &path = dftPath, const std::string &filename = dftName)
    {
        _log = std::make_shared<fileLog>(dftPath, dftName);
    }
    void to_console()
    {
        _log = std::make_shared<consoleLog>();
    }

    //     logMessage
    class logMessage
    {
    public:
        logMessage(logLevel level,std::string& file_name,int line,logger& loger)
        : _time(getTime()),
        _level(level), _id(getpid()),
        _file_name(file_name),
        _line(line),_loger(loger)
        {
            std::stringstream ssbuffer;
            ssbuffer << '[' << _time <<  "] " << '[' << toLevel[(int)level] << "] [" << _file_name << "] [" << _line << "] - ";
            _info = ssbuffer.str();
        }
        template <typename T>
        logMessage &operator<<(const T& info)
        {
            std::stringstream ssbuffer;
            ssbuffer << info;
            _info += ssbuffer.str();
            return *this;
        }
        ~logMessage()
        {
            _loger._log->fsync(_info);
        }

    private:
        logger &_loger;
        std::string _time;
        logLevel _level;
        pid_t _id;
        std::string _file_name;
        int _line;
        std::string _info;
    };
    //     logMessage
    
    logMessage operator()(logLevel level,std::string file_name,int line)
    {
        return logMessage(level, file_name, line,*this);
    }
    // void operator()(logLevel level,std::string file_name,int line)
    // {
    //     logMessage(level, file_name, line,*this);
    // }


private:
    std::shared_ptr<sync_base> _log;
};

logger glogger;
#define LOG(level) glogger(level, __FILE__, __LINE__)
#define TOCONSOLE() glogger.to_console()
#define TOFILE() glogger.to_file() 