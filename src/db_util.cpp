/**
 * create: 2015-01-18
 * author: zigzed@gmail.com
 */
#include "db_util.hpp"
#include "db_impl.hpp"
#include <unistd.h>

namespace pagedb {

    size_t _write(int fd, const void* buf, size_t len, const char* msg)
    {
        const uint8_t* ptr = (const uint8_t* )buf;
        size_t actual = 0;
        while(actual < len) {
            ssize_t s = write(fd, ptr + actual, len - actual);
            if(s < 0) {
                throw pagedb_error(Status::IOError(msg, strerror(errno)));
            }
            actual += s;
        }
        return actual;
    }

    size_t _pwrite(int fd, const void* buf, size_t len, off_t pos, const char* msg)
    {
        const uint8_t* ptr = (const uint8_t* )buf;
        size_t actual = 0;
        while(actual < len) {
            ssize_t s = pwrite(fd, ptr + actual, len - actual, pos + actual);
            if(s < 0) {
                throw pagedb_error(Status::IOError(msg, strerror(errno)));
            }
            actual += s;
        }
        return actual;
    }

    size_t _pread(int fd, off_t pos, void* buf, size_t len, const char* msg)
    {
        uint8_t* ptr = (uint8_t* )buf;
        size_t actual = 0;
        while(actual < len) {
            ssize_t s = pread(fd, ptr + actual, len - actual, pos + actual);
            if(s == 0)
                break;
            else if(s < 0) {
                throw pagedb_error(Status::IOError(msg, strerror(errno)));
            }
            actual += s;
        }
        return actual;
    }

}
