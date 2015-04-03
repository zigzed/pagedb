/**
 * create: 2015-01-20
 * author: zigzed@gmail.com
 */
#include "db_iter.hpp"

namespace pagedb {

    Iterator::Iterator()
    {
    }

    Iterator::~Iterator()
    {
    }

    PageDbIter::PageDbIter(const db_file* dbf)
        : dbf_(dbf)
        , cur_(NULL)
    {
    }

    PageDbIter::~PageDbIter()
    {
        delete cur_;
        cur_ = NULL;
    }

    bool PageDbIter::Valid() const
    {
        return cur_ && cur_->valid();
    }

    void PageDbIter::SeekToFirst()
    {
        try {
            delete cur_;
            cur_ = dbf_->begin();
        }
        catch(const db_file_error& e) {
            error_ = e.status();
        }
    }

    void PageDbIter::Seek(const Slice& target)
    {
        try {
            delete cur_;
            cur_ = dbf_->begin(target.data(), target.size());
        }
        catch(const db_file_error& e) {
            error_ = e.status();
        }
    }

    void PageDbIter::Next()
    {
        try {
            if(cur_) {
                cur_->next();
            }
        }
        catch(const db_file_error& e) {
            error_ = e.status();
        }
    }

    Slice PageDbIter::key() const
    {
        if(cur_ && cur_->valid()) {
            return Slice((const char* )cur_->key(), dbf_->ksize());
        }
        return Slice();
    }

    Slice PageDbIter::val() const
    {
        if(cur_ && cur_->valid()) {
            return Slice((const char* )cur_->val(), dbf_->vsize());
        }
        return Slice();
    }

    Status PageDbIter::status() const
    {
        return error_;
    }

}
