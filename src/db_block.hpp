/**
 * create: 2015-03-12
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DBLOCK_HPP
#define PAGEDB_DBLOCK_HPP
#include "cxxlib2/typedef.hpp"
#include "cxxlib2/ipc/mmap.hpp"
#include <stdexcept>
#include "pagedb/status.hpp"

namespace pagedb {

    class db_block_error : public std::runtime_error {
    public:
        explicit db_block_error(const Status& status)
            : std::runtime_error(status.ToString())
            , status_(status)
        {
        }

        const Status status() const {
            return status_;
        }

        ~db_block_error() throw() {}
    private:
        Status  status_;
    };

    class db_block_cursor;

    typedef int (*COMP_FUNC)(const void* x, const void* y, uint32_t len);

    class db_block {
    public:
        db_block(uint32_t klen, uint32_t vlen,
                 COMP_FUNC  comp_func,
                 cpp::ipc::mapped_region* m) throw(db_block_error);
        db_block(cpp::ipc::mapped_region* m,
                 COMP_FUNC comp_func) throw(db_block_error);
        ~db_block();

        bool                full() const;
        bool                save(const void* key, const void* val);
        void*               find(const void* key, uint32_t len) const throw(db_block_error);
        // 返回第一条 >= key 的记录
        db_block_cursor*    lower_bound(const void* key, uint32_t len) const throw(db_block_error);
        // 返回第一条 > key 的记录
        db_block_cursor*    upper_bound(const void* key, uint32_t len) const throw(db_block_error);

        void                sort() throw(db_block_error);
        bool                sorted() const;
        size_t              count() const;
        void                range(void*& lower, void*& upper) const;
        void*   memory() const;
        size_t  length() const;
        size_t  hdrlen() const;
        size_t  capacity() const;
    private:
        void    dosort() throw(db_block_error);

        struct header {
            uint32_t    hlen;
            uint32_t    klen;
            uint32_t    vlen;
            uint32_t    size;
        };
        struct exthdr;

        header*     header_;
        exthdr*     exthdr_;

        void*       memory_;
        uint32_t    length_;
        uint32_t    cursor_;

        bool                        dirty_;
        cpp::ipc::mapped_region*    block_;

        COMP_FUNC                   compf_;
    };

    class db_block_cursor {
    public:
        db_block_cursor(const void* memory, uint32_t length,
                        uint32_t klen, uint32_t vlen,
                        const void* key, uint32_t len,
                        COMP_FUNC comp_func);
        ~db_block_cursor();

        bool        next();
        bool        valid() const;
        bool        equal(const db_block_cursor* cursor) const;
        size_t      count(const db_block_cursor* cursor) const;
        const void* key() const;
        const void* val() const;
    private:
        const void* key_;
        uint32_t    len_;

        uint32_t    klen_;
        uint32_t    vlen_;

        const void* memory_;
        uint32_t    length_;
        uint32_t    offset_;
        bool        valid_;
        COMP_FUNC   compf_;
    };

}

#endif
