#include <pthread.h>
#include <iostream>
#include <unistd.h>

class mutex
{
public:
    mutex()
    {
        int n = pthread_mutex_init(&_lock, nullptr);
        if (n != 0)
        {
            std::cerr << "mutex_init_error" << std::endl;
        }
    }
    ~mutex()
    {
        int n = pthread_mutex_destroy(&_lock);
        if (n != 0)
        {
            std::cerr << "mutex_destroy_error" << std::endl;
        }
    }
    pthread_mutex_t *getLockPtr() { return &_lock; }
    void lock()
    {
        int n = pthread_mutex_lock(&_lock);
        if (n != 0)
        {
            std::cerr << "mutex_lock_error" << std::endl;
        }
    }

    void unlock()
    {
        int n = pthread_mutex_unlock(&_lock);
        if (n != 0)
        {
            std::cerr << "mutex_unlock_error" << std::endl;
        }
    }

    mutex(const mutex &other) = delete;
    const mutex &operator=(const mutex &other) = delete;

private:
    pthread_mutex_t _lock;
};

class lockGuard
{
public:
    lockGuard(mutex &mtx) : _mtx(mtx)
    {
        _mtx.lock();
    }
    ~lockGuard()
    {
        _mtx.unlock();
    }

private:
    mutex &_mtx;
};