/**
 * create: 2015-01-20
 * author: zigzed@gmail.com
 */
#include "db_iter.hpp"
#include "db_util.hpp"

namespace pagedb {

    struct index_record {
        Slice       lower;
        Slice       upper;
        off_t       offset;
        uint32_t    cksum;
    };

    pdb_reader_iter::~pdb_reader_iter()
    {
        if(!target_.empty()) {
            free((char* )target_.data());
            target_.clear();
        }
        if(buffer_) {
            free(buffer_);
            buffer_ = NULL;
        }
    }

    void pdb_reader_iter::Seek(const Slice& target)
    {
        char* ptr = (char* )malloc(target.size());
        memcpy(ptr, target.data(), target.size());
        target_ = Slice(ptr, target.size());
        blknum_ = 0;
        blkoff_ = 0;

        if(buffer_ == NULL) {
            buffer_ = (uint8_t* )malloc(length_);
        }

        bvalid_ = false;

        index_record record;
        size_t s = read_record(record);
        while(s > 0) {
            blknum_ += s;
            if(record.lower.compare(target_) <= 0 && target_.compare(record.upper) <= 0) {
                bvalid_ = true;
                reader_->read_blocks(record.offset);
                if(reader_->seek_blocks(target_, &blkoff_)) {
                    break;
                }
            }
            s = read_record(record);
        }
    }

}
