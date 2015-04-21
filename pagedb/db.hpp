/**
 * create: 2015-01-04
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_DB_HPP
#define PAGEDB_DB_HPP
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "pagedb/slice.hpp"
#include "pagedb/status.hpp"

namespace pagedb {

	// user defined compare function:
	//	if x < y, return -1
	//	if x > y, return 1
	//	if x == y, return 0
	typedef int (*COMP_FUNC)(const void* x, const void* y, uint32_t len);
	
    struct Options {
        size_t  	block_size;
        bool        read_only;
		COMP_FUNC	comp_func;
        Options()
            : block_size(4 * 1024 * 1024)
            , read_only(true)
			, comp_func(0)
        {
        }

        Options& set_block_size(size_t bs) {
            block_size = bs;
            return *this;
        }
        Options& set_read_only(bool ro) {
            read_only = ro;
            return *this;
        }
        Options& set_comp_func(COMP_FUNC cf) {
            comp_func = cf;
            return *this;
        }
    };
	
	class Iterator {
    public:
        virtual ~Iterator();

        virtual bool Valid() const = 0;
        virtual void SeekToFirst() = 0;
        virtual void Seek(const Slice& target) = 0;
        virtual void Next() = 0;
        virtual Slice key() const = 0;
        virtual Slice val() const = 0;
        virtual Status status() const = 0;
    protected:
        Iterator();
    private:
        Iterator(const Iterator& );
        void operator=(const Iterator& );
    };

    class DB {
    public:
		// open a database object
        static Status Open(const Options& options,
                           const std::string& name,
                           size_t key_length,
                           size_t val_length,
                           DB** dbptr);
		static Status Destroy(const std::string& name);
		
        virtual ~DB();

		// append a record
        virtual Status Put(const Slice& key,
						   const Slice& val) = 0;
		// search a record
        virtual Status Get(const Slice& key,
                           std::string& val) const = 0;
        virtual Status Sync() = 0;
        virtual Iterator* NewIterator() const = 0;
        virtual Iterator* EqualRange(const Slice& begin, const Slice& end) const = 0;
    protected:
        DB();
    private:
        DB(const DB& );
        void operator=(const DB& );
    };

}

#endif
