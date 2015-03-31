/**
 * create: 2015-01-04
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DB_IMPL_HPP
#define PAGEDB_DB_IMPL_HPP
#include <stdexcept>
#include "pagedb/pagedb/db.hpp"
#include "pagedb/pagedb/status.hpp"

namespace pagedb {

    class pagedb_error : std::runtime_error {
    public:
        pagedb_error(const Status& status)
            : std::runtime_error(status.ToString())
            , status_(status)
        {}
        const Status status() const {
            return status_;
        }
    private:
        Status status_;
    };

    /** PageDB structre
     *
     * PageDB       = FILE-HEADER [BLOCK]*
     * FILE-HEADER  = MAGIC-NUMBER KEYLEN VALLEN BLOCKSIZE CHECKSUM
     * BLOCK        = BLOCK-HEADER [TAGGED-CHUNK]* [PADDING]
     * BLOCK-HEADER = BLOCK-SIZE CHECKSUM
     * BLOCK-FOOTER = CHECKSUM
     * TAGGED-CHUNK = TAG CHUNK-LENGTH CHUNK
     * TAG          = {
     *                  0x00:KV-PAIR, 0x01:COMPRESSED-KV-PAIR,
     *                  0x10:BLOOM FILTER,
     *                  0x20:COUNT, 0x21:MIN-KEY, 0x22:MAX-KEY,
     *                }
     *
     *
     */

    struct pagdb_header;
    struct block_header;

    class pdb_writer : public DB {
    public:
        pdb_writer(const Options& options, const std::string& name);
        ~pdb_writer();

        Status Put(const Slice& key,
                   const Slice& val);
        Status Sync();
        Iterator* NewIterator();
    private:
        pdb_writer(const pdb_writer& );
        void operator= (const pdb_writer& );

        void save_header(int pfd, int ifd);
        void load_header(int pfd, int ifd);
        void make_indexs(int pdb, int idx);

        const Options       options_;
        const std::string   dbname_;

        class block_cache_t;

        block_cache_t*  block_cache_;
        pagdb_header*   page_header_;

        int             pagdb_fd_;
        int             index_fd_;
    };

    class pdb_reader : public DB {
    public:
        pdb_reader(const Options& options, const std::string& name);
        ~pdb_reader();

        Status Put(const Slice& key,
                   const Slice& val);
        Status Sync();
        Iterator* NewIterator();
    private:
        pdb_reader(const pdb_reader& );
        pdb_reader& operator= (const pdb_reader& );

        void load_header(int fd);
        void read_blocks(off_t pos);
        bool seek_blocks(const Slice& target, size_t* offset);

        friend class pdb_reader_iter;

        const Options       options_;
        const std::string   dbname_;

        uint8_t*            block_;
        uint8_t*            plain_;
        pagdb_header*       page_header_;
        size_t              csize_;
        size_t              psize_;

        int                 pagdb_fd_;
        int                 index_fd_;
    };

}

#endif
