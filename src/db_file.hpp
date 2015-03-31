/**
 * create: 2015-03-13
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DBFILE_HPP
#define PAGEDB_DBFILE_HPP
#include "db_block.hpp"
#include "pagedb/status.hpp"
#include "cxxlib2/ipc/mmap.hpp"
#include <stdexcept>

namespace pagedb {

    class db_file_error : public std::runtime_error {
    public:
        explicit db_file_error(const Status& status)
            : std::runtime_error(status.ToString())
            , status_(status)
        {
        }

        const Status status() const {
            return status_;
        }

        ~db_file_error() throw() {}
    private:
        Status  status_;
    };

    class db_file_cursor;

    class db_file {
    public:
        static void remove(const char* filename);

        db_file(const char* filename, uint32_t block_size,
                uint32_t klen, uint32_t vlen,
                COMP_FUNC comp_func,
                bool readonly) throw(db_file_error);
        ~db_file();

        // get a new block
        db_block*       insert();

        const db_block* fetch(size_t index) const;
        db_block*       fetch(size_t index);
		db_block*		tail ();
        size_t          count() const;

        uint32_t        ksize() const;
        uint32_t        vsize() const;

        // get a cursor for iterator
        db_file_cursor* begin(const void* key, uint32_t len) const;
    private:
        void create(const char* filename, uint32_t block_size,
                    uint32_t klen, uint32_t vlen) throw(db_file_error);
        void open(const char* filename, bool readonly) throw(db_file_error);

        struct header {
            uint32_t    magic;
            uint32_t    hdlen;  // header size
            uint32_t    block;  // block size
            uint32_t    count;  // block count
            uint32_t    ksize;  // key length
            uint32_t    vsize;  // val length
            uint32_t    spare;
            uint32_t    cksum;  // checksum of header
        };

        std::string filename_;

        off_t       size_;

        header*     header_;

        cpp::ipc::file_mapping_t    mapping_;
        cpp::ipc::mapped_region     hdr_map_;

        COMP_FUNC   compf_;
    };

    // iterator for the db_block in a file. db_file_cursor is read only
    class db_file_cursor {
    public:
        db_file_cursor(const db_file* dbf,
                       const void* key, uint32_t klen);
        ~db_file_cursor();

        bool            next();
        bool            valid() const;
        const void*     key() const;
        const void*     val() const;
    private:
        bool step_to_block();

        const db_file*      dbf_;
        const void*         key_;
        uint32_t            len_;
        size_t              pos_;
        size_t              blk_;
        const db_block*     dbb_;
        db_block_cursor*    lower_;
        db_block_cursor*    upper_;
    };

}

#endif
