/**
 * create: 2015-01-20
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DB_ITER_HPP
#define PAGEDB_DB_ITER_HPP
#include "pagedb/db.hpp"
#include "db_impl.hpp"

namespace pagedb {

    class PageDbIter : public Iterator {
    public:
        explicit PageDbIter(const db_file* dbf);
        ~PageDbIter();

        bool Valid() const;
        void SeekToFirst();
        void Seek(const Slice& target);
        void Next();
        Slice key() const;
        Slice val() const;
        Status status() const;
    private:
        PageDbIter(const PageDbIter& );
        PageDbIter& operator= (const PageDbIter& );

        const db_file*  dbf_;
        db_file_cursor* cur_;
        Status          error_;
    };


}

#endif
