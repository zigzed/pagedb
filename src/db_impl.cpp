/**
 * create: 2015-01-04
 * author: zigzed@gmail.com
 */
#include "pagedb/db.hpp"
#include "db_impl.hpp"
#include "db_iter.hpp"

namespace pagedb {

    Status DB::Open(const Options& options,
                    const std::string& name,
                    size_t key_length,
                    size_t val_length,
                    DB** dbptr)
    {
        try {
            *dbptr = new PageDb(options, name, key_length, val_length);
        }
        catch(const db_file_error& e) {
            return e.status();
        }

        return Status::OK();
    }

    Status DB::Destroy(const std::string& name)
    {
        db_file::remove(name.c_str());
        return Status::OK();
    }

    DB::DB()
    {
    }

    DB::~DB()
    {
    }

    PageDb::PageDb(const Options& options,
                   const std::string& name,
                   size_t key_length,
                   size_t val_length)
        : dbf_(NULL)
        , dbb_(NULL)
    {
        dbf_ = new db_file(name.c_str(),
                           options.block_size,
                           key_length, val_length,
                           options.comp_func,
                           options.read_only);

        if(!options.read_only) {
            dbb_ = dbf_->tail();
        }
    }

    PageDb::~PageDb()
    {
        delete dbb_;
        dbb_ = NULL;
        delete dbf_;
        dbf_ = NULL;
    }

    Status PageDb::Put(const Slice& key, const Slice& val)
    {
        if(key.size() != dbf_->ksize()) {
            return Status::InvalidArgument("invalid key length");
        }
        if(val.size() != dbf_->vsize()) {
            return Status::InvalidArgument("invalid value length");
        }
        if(dbf_->readonly()) {
            return Status::InvalidArgument("put to a readonly pagedb");
        }

        try {
            while(!dbb_->save(key.data(), val.data())) {
                dbb_->sort();
                delete dbb_;
                dbb_ = dbf_->insert();
            }
        }
        catch(const db_block_error& e) {
            return e.status();
        }

        return Status::OK();
    }

    Status PageDb::Get(const Slice& key, Slice& val)
    {
        if(key.size() > dbf_->ksize() + dbf_->vsize()) {
            return Status::InvalidArgument("invalid key length");
        }

        try {
            db_file_cursor* cursor = dbf_->begin(key.data(), key.size());
            if(cursor->valid()) {
                val = Slice((const char* )cursor->val(), dbf_->vsize());
                delete cursor;
                cursor = NULL;
                return Status::OK();
            }
        }
        catch(const db_block_error& e) {
            return e.status();
        }

        return Status::NotFound("no such key found");
    }

    Status PageDb::Sync()
    {
        if(dbf_->readonly() || !dbb_) {
           return Status::OK();
        }

        try {
            dbb_->sort();
            delete dbb_;
            dbb_ = NULL;
            dbf_->flush();

            dbb_ = dbf_->tail();
        }
        catch(const db_block_error& e) {
            return e.status();
        }

        return Status::OK();
    }

    Iterator* PageDb::NewIterator()
    {
        return new PageDbIter(dbf_);
    }

}
