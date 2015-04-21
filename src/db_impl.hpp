/**
 * create: 2015-01-04
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DB_IMPL_HPP
#define PAGEDB_DB_IMPL_HPP
#include <stdexcept>
#include "pagedb/db.hpp"
#include "db_file.hpp"

namespace pagedb {

    class PageDb : public DB {
    public:
        PageDb(const Options& options,
               const std::string& name,
               size_t key_length,
               size_t val_length);
        ~PageDb();

        Status Put(const Slice& key,
                   const Slice& val);
        Status Get(const Slice& key,
                   std::string& val) const;
        Status Sync();
        Iterator* NewIterator() const;
        Iterator* EqualRange(const Slice& begin, const Slice& end) const;
    private:
        db_file*    dbf_;
        db_block*   dbb_;
    };

}

#endif
