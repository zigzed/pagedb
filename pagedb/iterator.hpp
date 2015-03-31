/**
 * create: 2014-01-04
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_ITERATOR_HPP
#define PAGEDB_ITERATOR_HPP
#include "pagedb/pagedb/slice.hpp"
#include "pagedb/pagedb/status.hpp"

namespace pagedb {

    class Iterator {
    public:
        Iterator();
        virtual ~Iterator();

        virtual bool Valid() const = 0;
        virtual void SeekToFirst() = 0;
        virtual void SeekToLast() = 0;
        virtual void Seek(const Slice& target) = 0;
        virtual void Next() = 0;
        virtual void Prev() = 0;
        virtual Slice key() const = 0;
        virtual Slice val() const = 0;
        virtual Status status() const = 0;
    private:
        Iterator(const Iterator& );
        void operator=(const Iterator& );
    };

}

#endif
