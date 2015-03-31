/**
 * create: 2015-01-20
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DB_ITER_HPP
#define PAGEDB_DB_ITER_HPP
#include "pagedb/pagedb/iterator.hpp"
#include "db_impl.hpp"

namespace pagedb {

    struct index_record;

    class pdb_reader_iter : public Iterator {
    public:
        ~pdb_reader_iter();

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
        pdb_reader_iter(const pdb_reader_iter& );
        pdb_reader_iter& operator= (const pdb_reader_iter& );

        size_t read_record(index_record& record);

        pdb_reader* reader_;
        int         idx_fd_;
        off_t       blknum_;
        size_t      blkoff_;
        Slice       target_;
        bool        bvalid_;

        uint8_t*    buffer_;
        size_t      length_;
    };


}

#endif
