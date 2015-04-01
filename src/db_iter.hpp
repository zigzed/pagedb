/**
 * create: 2015-01-20
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DB_ITER_HPP
#define PAGEDB_DB_ITER_HPP
#include "pagedb/db.hpp"
#include "db_impl.hpp"

namespace pagedb {

    class pdb_iterator : public Iterator {
    public:
        ~pdb_iterator();

        bool Valid() const;
        void SeekToFirst();
        void SeekToLast();
        void Seek(const Slice& target);
        void Next();
        void Prev();
        Slice key() const;
        Slice val() const;
        Status status() const;
    private:
        pdb_iterator(const pdb_iterator& );
        pdb_iterator& operator= (const pdb_iterator& );

    };


}

#endif
