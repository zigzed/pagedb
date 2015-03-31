/**
 * create: 2015-01-18
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DB_UTIL_HPP
#define PAGEDB_DB_UTIL_HPP
#include "cxxlib2/typedef.hpp"
#include "pagedb/pagedb/status.hpp"

namespace pagedb {

    size_t _write(int fd, const void* buf, size_t len, const char* msg);
    size_t _pwrite(int fd, const void* buf, size_t len, off_t pos, const char* msg);
    size_t _pread(int fd, off_t pos, void* buf, size_t len, const char* msg);

}

#endif
