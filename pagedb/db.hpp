/**
 * create: 2015-01-04
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DB_HPP
#define PAGEDB_DB_HPP
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "pagedb/pagedb/slice.hpp"
#include "pagedb/pagedb/status.hpp"

namespace pagedb {

    class Iterator;

    struct Options {
        bool    create_if_missing;
        bool    error_if_exists;
        bool    paranoid_checks;
        bool    use_bloomfilter;
        size_t  block_size;
        Options()
            : create_if_missing(false)
            , error_if_exists(false)
            , paranoid_checks(false)
            , use_bloomfilter(true)
            , block_size(2 * 1024 * 1024)
        {
        }
    };

    struct ReadOptions {
        bool verify_checksums;
        ReadOptions()
            : verify_checksums(false)
        {
        }
    };

    struct WriteOptions {
        bool sync;
        WriteOptions()
            : sync(false)
        {
        }
    };

    class DB {
    public:
        static Status Open(const Options& options,
                           const std::string& name,
                           size_t key_length,
                           size_t val_length,
                           DB** dbptr);
        DB() {}
        virtual ~DB();

        virtual Status Put(const WriteOptions& options,
                           const Slice& key,
                           const Slice& val) = 0;
        virtual Status Get(const ReadOptions& options,
                           const Slice& key,
                           Slice& val) = 0;
        virtual Status Sync() = 0;
        virtual Iterator* NewIterator(const ReadOptions& options) = 0;
    private:
        DB(const DB& );
        void operator=(const DB& );
    };

    Status DestroyDB(const std::string& name, const Options& options);
    Status RepairDB(const std::string& name, const Options& options);

}

#endif
