/**
 * create: 2015-03-12
 * author: zigzed@gmail.com
 */
#include "db_block.hpp"
#include <string.h>
#include <stdlib.h>
#include "cxxlib2/cvt/xxhash.hpp"

namespace pagedb {

    static const uint32_t BF_SORTED = 0x00000001;

    struct db_block::exthdr {
        uint32_t    flags;
        uint32_t    count;
        uint32_t    spare;
        uint32_t    cksum;
    };

    int default_sort_func(const void* x, const void* y, uint32_t len)
    {
        return memcmp(x, y, len);
    }

    // open a new db_block
    db_block::db_block(uint32_t klen, uint32_t vlen, COMP_FUNC comp_func,
                       cpp::ipc::mapped_region *m) throw(db_block_error)
        : header_(0), exthdr_(0)
        , memory_(m->data()), length_(m->size())
        , cursor_(0)
        , dirty_(false)
        , block_(m)
        , compf_(comp_func)
    {
        if(length_ < sizeof(header) + sizeof(exthdr)) {
            throw db_block_error(Status::InvalidArgument("block size is to small"));
        }

        header_ = (header* )memory_;
        header_->hlen  = sizeof(header) + sizeof(exthdr);
        header_->klen  = klen;
        header_->vlen  = vlen;
        header_->size  = header_->hlen;

        exthdr_ = (exthdr* )((char* )memory_ + sizeof(header));
        exthdr_->flags = 0;
        exthdr_->count = 0;
        exthdr_->spare = 0;
        exthdr_->cksum = 0;

        cursor_ = header_->hlen;
        dirty_ = true;

        if(compf_ == NULL) {
            compf_ = default_sort_func;
        }
    }

    // open an exist db_block for read
    db_block::db_block(cpp::ipc::mapped_region *m,
                       COMP_FUNC comp_func) throw(db_block_error)
        : header_(0), exthdr_(0)
        , memory_(m->data()), length_(m->size())
        , cursor_(0)
        , dirty_(false)
        , block_(m)
        , compf_(comp_func)
    {
        if(length_ < sizeof(header)) {
            throw db_block_error(Status::InvalidArgument("block size is to small"));
        }

        header_ = (header* )memory_;
        if(length_ < header_->hlen) {
            throw db_block_error(Status::InvalidArgument("block size is to small for extended header"));
        }
        if(length_ < header_->size) {
            throw db_block_error(Status::Corruption("block size mismatched", "db_block"));
        }

        exthdr_ = (exthdr* )((char* )memory_ + sizeof(header));

        cursor_ = header_->size;

        if(compf_ == NULL) {
            compf_ = default_sort_func;
        }
    }

    db_block::~db_block()
    {
        if(dirty_) {
            block_->commit();
        }
        delete block_;
        block_ = NULL;
    }

    bool db_block::full() const
    {
        uint32_t total_length = header_->klen + header_->vlen;

        return cursor_ + total_length <= length_;
    }

    bool db_block::save(const void* key, const void* val)
    {
        uint32_t total_length = header_->klen + header_->vlen;
        if(cursor_ + total_length > length_)
            return false;

        memcpy((char* )memory_ + cursor_, key, header_->klen);
        if(header_->vlen > 0) {
            memcpy((char* )memory_ + cursor_ + header_->klen, val, header_->vlen);
        }
        dirty_ = true;
        exthdr_->flags &= ~(BF_SORTED);

        cursor_ += total_length;
        header_->size  += total_length;
        exthdr_->count += 1;

        return true;
    }

    void db_block::sort() throw(db_block_error)
    {
        if((exthdr_->flags & BF_SORTED) == BF_SORTED)
            return;

        dosort();
        exthdr_->flags |= BF_SORTED;
        dirty_ = true;
    }

    size_t db_block::count() const
    {
        return exthdr_->count;
    }

    bool db_block::sorted() const
    {
        return (exthdr_->flags & BF_SORTED) == BF_SORTED;
    }

    void db_block::range(void*& lower, void*& upper) const
    {
        if((exthdr_->flags & BF_SORTED) == BF_SORTED) {
            lower = (char* )memory_ + hdrlen();
            upper = (char* )memory_ + cursor_;
        }
        else {
            lower = NULL;
            upper = NULL;
        }
    }

    void* db_block::memory() const
    {
        return memory_;
    }

    size_t db_block::length() const
    {
        return cursor_;
    }

    size_t db_block::hdrlen() const
    {
        return header_->hlen;
    }

    size_t db_block::capacity() const
    {
        return length_;
    }

    struct comp_key_arg {
        uint32_t    size;
        COMP_FUNC   func;
    };

    static int comp_key_with_length(const void* p1, const void* p2, void* p)
    {
        comp_key_arg* arg = (comp_key_arg* )p;
        uint32_t    l = arg->size;
        COMP_FUNC   f = arg->func;

        return (*f)(p1, p2, l);
    }

    void db_block::dosort() throw(db_block_error)
    {
        size_t capacity = cursor_ - hdrlen();
        size_t size = (header_->klen + header_->vlen);
        size_t nmem = capacity / size;
        if(capacity != nmem * size) {
            throw db_block_error(Status::Corruption("data is not aligned", "sort"));
        }
        if(nmem != exthdr_->count) {
            throw db_block_error(Status::Corruption("data count is mismatched", "sort"));
        }

        comp_key_arg comp_arg;
        comp_arg.size = header_->klen + header_->vlen;
        comp_arg.func = compf_;

        // 在保存时对 key 和 val 排序，这样可以支持对 key 和 val 的 prefix search
        qsort_r((char* )memory_ + hdrlen(),
                nmem,
                size,
                comp_key_with_length,
                &comp_arg);
    }

    static void* do_lower_bound(const void* key, const void* base,
                                size_t nmemb, size_t size,
                                int (*compar)(const void* , const void* , void* ),
                                comp_key_arg* args)
    {
        int lower = -1;
        int upper = nmemb;// - 1;
        int middle = 0;
        void* ptr = 0;
        int   res = -1;
        while(upper - lower > 1) {
            middle = lower + (upper - lower) / 2;
            ptr = (char* )base + size * middle;
            res = compar(ptr, key, args);
            if(res >= 0)
                upper = middle;
            else
                lower = middle;
        }
        return (char* )base + size * upper;
    }

    static void* do_upper_bound(const void* key, const void* base,
                                size_t nmemb, size_t size,
                                int (*compar)(const void* , const void* , void* ),
                                comp_key_arg* args)
    {
        int lower = -1;
        int upper = nmemb;
        int middle = 0;
        void* ptr = 0;
        int   res = -1;
        while(upper - lower > 1) {
            middle = lower + (upper - lower) / 2;
            ptr = (char* )base + size * middle;
            res = compar(ptr, key, args);
            if(res <= 0)
                lower = middle;
            else
                upper = middle;
        }
        return (char* )base + size * upper;
    }

    void* db_block::find(const void* key, uint32_t len) const throw(db_block_error)
    {
        // 查找时最多允许比较的长度为 klen + vlen
        if(len > header_->klen + header_->vlen)
            len = header_->klen + header_->vlen;

        size_t capacity = cursor_ - hdrlen();
        size_t size = (header_->klen + header_->vlen);
        size_t nmem = capacity / size;
        if(capacity != nmem * size) {
            throw db_block_error(Status::Corruption("data is not aligned", "find"));
        }
        if(nmem != exthdr_->count) {
            throw db_block_error(Status::Corruption("data count is mismatched", "find"));
        }
        void* ptr = (char* )memory_ + hdrlen();

        comp_key_arg comp_arg;
        comp_arg.size = len;
        comp_arg.func = compf_;

        void* res = do_lower_bound(key,  ptr,
                                   nmem, size,
                                   comp_key_with_length,
                                   &comp_arg);
        if(res == NULL)
            return NULL;

        if(comp_key_with_length(res, key, &comp_arg) == 0)
            return res;

        return NULL;
    }

    db_block_cursor* db_block::lower_bound(const void* key, uint32_t len) const throw(db_block_error)
    {
        // 查找时最多允许比较的长度为 klen + vlen
        if(len > header_->klen + header_->vlen)
            len = header_->klen + header_->vlen;

        size_t capacity = cursor_ - hdrlen();
        size_t size = (header_->klen + header_->vlen);
        size_t nmem = capacity / size;
        if(capacity != nmem * size) {
            throw db_block_error(Status::Corruption("data is not aligned", "lower_bound"));
        }
        if(nmem != exthdr_->count) {
            throw db_block_error(Status::Corruption("data count is mismatched", "lower_bound"));
        }
        void* ptr = (char* )memory_ + hdrlen();

        comp_key_arg comp_arg;
        comp_arg.size = len;
        comp_arg.func = compf_;

        void* res = do_lower_bound(key,  ptr,
                                   nmem, size,
                                   comp_key_with_length,
                                   &comp_arg);

        uint32_t length = (char* )res - (char* )memory_;

        // capacity should add hdrlen()
        return new db_block_cursor(res, capacity - length + hdrlen(),
                                   header_->klen, header_->vlen,
                                   key, len,
                                   compf_);
    }
    
    db_block_cursor* db_block::lower_bound() const throw(db_block_error)
	{
		size_t capacity = cursor_ - hdrlen();
        size_t size = (header_->klen + header_->vlen);
        size_t nmem = capacity / size;
        if(capacity != nmem * size) {
            throw db_block_error(Status::Corruption("data is not aligned", "lower_bound"));
        }
        if(nmem != exthdr_->count) {
            throw db_block_error(Status::Corruption("data count is mismatched", "lower_bound"));
        }
        void* ptr = (char* )memory_ + hdrlen();

        return new db_block_cursor(ptr, capacity,
                                   header_->klen, header_->vlen,
                                   NULL, 0,
                                   compf_);
	}

    db_block_cursor* db_block::upper_bound(const void* key, uint32_t len) const throw(db_block_error)
    {
        // 查找时最多允许比较的长度为 klen + vlen
        if(len > header_->klen + header_->vlen)
            len = header_->klen + header_->vlen;

        size_t capacity = cursor_ - hdrlen();
        size_t size = (header_->klen + header_->vlen);
        size_t nmem = capacity / size;
        if(capacity != nmem * size) {
            throw db_block_error(Status::Corruption("data is not aligned", "upper_bound"));
        }
        if(nmem != exthdr_->count) {
            throw db_block_error(Status::Corruption("data count is mismatched", "upper_bound"));
        }
        void* ptr = (char* )memory_ + hdrlen();

        comp_key_arg comp_arg;
        comp_arg.size = len;
        comp_arg.func = compf_;

        void* res = do_upper_bound(key,  ptr,
                                   nmem, size,
                                   comp_key_with_length,
                                   &comp_arg);

        uint32_t length = (char* )res - (char* )memory_;

        // capacity should add hdrlen()
        return new db_block_cursor(res, capacity - length + hdrlen(),
                                   header_->klen, header_->vlen,
                                   key, len,
                                   compf_);
    }
    
    db_block_cursor* db_block::upper_bound() const throw(db_block_error)
	{
		size_t capacity = cursor_ - hdrlen();
        size_t size = (header_->klen + header_->vlen);
        size_t nmem = capacity / size;
        if(capacity != nmem * size) {
            throw db_block_error(Status::Corruption("data is not aligned", "upper_bound"));
        }
        if(nmem != exthdr_->count) {
            throw db_block_error(Status::Corruption("data count is mismatched", "upper_bound"));
        }
        void* ptr = (char* )memory_ + hdrlen() + capacity;

        // capacity should add hdrlen()
        return new db_block_cursor(ptr, 0,
                                   header_->klen, header_->vlen,
                                   NULL, 0,
                                   compf_);
	}

    ////////////////////////////////////////////////////////////////////////////
    db_block_cursor::db_block_cursor(const void* memory, uint32_t length,
                                     uint32_t klen, uint32_t vlen,
                                     const void* key, uint32_t len, COMP_FUNC comp_func)
        : key_(key), len_(len)
        , klen_(klen), vlen_(vlen)
        , memory_(memory), length_(length)
        , offset_(0)
        , valid_(true)
        , compf_(comp_func)
    {
        if(offset_ + klen_ + vlen_ > length_)
            valid_ = false;
    }

    db_block_cursor::~db_block_cursor()
    {
    }

    bool db_block_cursor::next()
    {
        valid_ = false;
        if(offset_ + klen_ + vlen_ >= length_) {
            return false;
        }

//		if(key_ && len_ > 0) {
//			comp_key_arg comp_arg;
//			comp_arg.size = len_;
//			comp_arg.func = compf_;

//			void* nextptr = (char* )memory_ + offset_ + klen_ + vlen_;
//			if(comp_key_with_length(key_, nextptr, &comp_arg) == 0) {
//				offset_ += (klen_ + vlen_);
//				valid_ = true;
//			}
//		}
//		else {
//            offset_ += (klen_ + vlen_);
//			valid_ = true;
//		}
        offset_ += (klen_ + vlen_);
        valid_ = true;

        return valid_;
    }

    bool db_block_cursor::valid() const
    {
        return valid_;
    }

    bool db_block_cursor::equal(const db_block_cursor* cursor) const
    {
        return (char* )memory_ + offset_ == (char* )cursor->memory_ + cursor->offset_;
    }

    size_t db_block_cursor::count(const db_block_cursor* cursor) const
    {
        char* rhs = (char* )cursor->memory_ + cursor->offset_;
        char* lhs = (char* )memory_ + offset_;
        if(rhs >= lhs)
            return (rhs - lhs) / (klen_ + vlen_);
        else
            return (lhs - rhs) / (klen_ + vlen_);
    }

    const void* db_block_cursor::key() const
    {
        return (char* )memory_ + offset_;
    }

    const void* db_block_cursor::val() const
    {
        return (char* )memory_ + offset_ + klen_;
    }

}
