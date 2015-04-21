/**
 * create: 2015-03-13
 * author: zigzed@gmail.com
 */
#include "db_file.hpp"
#include "cxxlib2/sys/filesystem.hpp"
#include "cxxlib2/cvt/xxhash.hpp"

namespace pagedb {

    static const uint32_t DB_FILE_MAGIC_TAG = 0xDBF0;
    static const uint32_t DB_FILE_MAGIC_VER = 0x0001;
    static const uint32_t DB_FILE_MAGIC     = (DB_FILE_MAGIC_TAG << 16) | DB_FILE_MAGIC_VER;

    void db_file::remove(const char* filename)
    {
        cpp::ipc::file_mapping::remove(filename);
    }

    db_file::db_file(const char* filename,
                     uint32_t blocksize,
                     uint32_t klen, uint32_t vlen,
                     COMP_FUNC comp_func,
                     bool readonly) throw(db_file_error)
        : filename_(filename)
        , size_(0)
        , header_(0)
        , compf_(comp_func)
    {
        cpp::sys::make_dir(cpp::sys::Path(filename).base().c_str(), 0755);

        cpp::sys::Path filepath(filename);
        if(readonly && !filepath.exist()) {
            throw db_file_error(Status::IOError("dbfile is not exist", filename));
        }

        if(!filepath.exist()) {
            create(filename, blocksize, klen, vlen);
        }
        else {
            open(filename, readonly);
            if(blocksize != header_->block) {
                throw db_file_error(Status::InvalidArgument("block size mismatched", filename));
            }
            if(klen != header_->ksize || vlen != header_->vsize) {
                throw db_file_error(Status::InvalidArgument("key/value size mismatched", filename));
            }
        }
    }

    void db_file::create(const char* filename,
                         uint32_t blocksize,
                         uint32_t klen, uint32_t vlen) throw(db_file_error)
    {
        // open file mapping, check file header
        mapping_ = cpp::ipc::file_mapping_t(new cpp::ipc::file_mapping(filename,
                                                                       cpp::ipc::memory_mappable::ReadWrite));
        // set file size at least one block
        mapping_->size(sizeof(header) + blocksize);

        if(!hdr_map_.attach(mapping_, cpp::ipc::mapped_region::ReadWrite)) {
            throw db_file_error(Status::Corruption("mapping file header", "db_file"));
        }
        hdr_map_.move(0, sizeof(header));
        header_ = (header* )hdr_map_.data();
        memset(header_, 0, sizeof(header));
        header_->magic = DB_FILE_MAGIC;
        header_->hdlen = sizeof(header);
        header_->block = blocksize;
        header_->count = 0;
        header_->ksize = klen;
        header_->vsize = vlen;
        header_->cksum = XXH32(header_, sizeof(header), 0);

        hdr_map_.commit();

        size_ = sizeof(header);
    }

    void db_file::open(const char* filename, bool readonly) throw(db_file_error)
    {
        cpp::sys::Path filepath(filename);
        if(!filepath.exist()) {
            throw db_file_error(Status::IOError("file is not exist", filename));
        }

        // open file mapping, check file header
        mapping_ = cpp::ipc::file_mapping_t(
                    new cpp::ipc::file_mapping(filename,
                                               readonly ? cpp::ipc::memory_mappable::ReadOnly : cpp::ipc::memory_mappable::ReadWrite)
                    );
        if(mapping_->size() < sizeof(header)) {
            throw db_file_error(Status::Corruption("file size is too small to keep header", "db_file"));
        }
        size_ = mapping_->size();

        if(!hdr_map_.attach(mapping_, readonly ? cpp::ipc::mapped_region::ReadOnly : cpp::ipc::mapped_region::ReadWrite)) {
            throw db_file_error(Status::Corruption("mapping file header", "db_file"));
        }
        hdr_map_.move(0, sizeof(header));
        header_ = (header* )hdr_map_.data();
        if(header_->magic != DB_FILE_MAGIC) {
            throw db_file_error(Status::Corruption("invalid file magic", "db_file"));
        }
        {
            header tmp;
            memcpy(&tmp, header_, sizeof(header));
            tmp.cksum = 0;
            uint32_t cksum = XXH32(&tmp, sizeof(header), 0);
            if(cksum != header_->cksum) {
                throw db_file_error(Status::Corruption("invalid file checksum", "db_file"));
            }
        }

        if(header_->hdlen > size_) {
            throw db_file_error(Status::Corruption("file size is too small to keep extended header", "db_file"));
        }

        if(size_ < header_->hdlen + header_->block * header_->count) {
            throw db_file_error(Status::Corruption("file size and header mismatched", "db_file"));
        }
    }

    db_file::~db_file()
    {
        flush();
    }

    void db_file::flush()
    {
        if(!readonly()) {
            header_->cksum = 0;
            header_->cksum = XXH32(header_, sizeof(header), 0);
            hdr_map_.commit();
        }
    }

    size_t db_file::count() const
    {
        return header_->count;
    }

    uint32_t db_file::ksize() const
    {
        return header_->ksize;
    }

    uint32_t db_file::vsize() const
    {
        return header_->vsize;
    }

    bool db_file::readonly() const
    {
        return mapping_->mode() == cpp::ipc::memory_mappable::ReadOnly;
    }

    const db_block* db_file::fetch(size_t index) const
    {
        if(index >= header_->count)
            return NULL;

        cpp::ipc::mapped_region* blk_map = new cpp::ipc::mapped_region(mapping_,
                                                                       cpp::ipc::mapped_region::ReadOnly,
                                                                       header_->hdlen + header_->block * index,
                                                                       header_->block);
        // if open readonly, we need binary search, which is random access
        blk_map->advise(cpp::ipc::mapped_region::Random);
        return new db_block(blk_map, compf_);
    }

    db_block* db_file::fetch(size_t index)
    {
        if(readonly()) {
            throw db_file_error(Status::InvalidArgument("fetch a readonly block for writing"));
        }
        if(index >= header_->count)
            return NULL;

        cpp::ipc::mapped_region* blk_map = new cpp::ipc::mapped_region(mapping_,
                                                                       cpp::ipc::mapped_region::ReadWrite,
                                                                       header_->hdlen + header_->block * index,
                                                                       header_->block);
        // if open readwrite, we want to append new value, which is sequential access
        blk_map->advise(cpp::ipc::mapped_region::Sequential);
        return new db_block(blk_map, compf_);
    }
    
    db_block* db_file::tail()
	{
        if(header_->count == 0) {
            if(!readonly()) {
                return this->insert();
            }
            return NULL;
        }
		return fetch(header_->count - 1);
	}

    db_block* db_file::insert()
    {
        if(readonly()) {
            throw db_file_error(Status::InvalidArgument("insert to a readonly block"));
        }

        size_ += header_->block;

        mapping_->size(size_);
        header_->count++;
        header_->cksum = 0;
        header_->cksum = XXH32(header_, sizeof(header), 0);
        hdr_map_.commit();

        cpp::ipc::mapped_region* blk_map = new cpp::ipc::mapped_region(mapping_,
                                                                       cpp::ipc::mapped_region::ReadWrite,
                                                                       header_->hdlen + header_->block * (header_->count - 1),
                                                                       header_->block);
        // for insert, we append new value only, which is sequential access
        blk_map->advise(cpp::ipc::mapped_region::Sequential);

        return new db_block(header_->ksize, header_->vsize, compf_, blk_map);
    }

    db_file_cursor* db_file::begin(const void* key, uint32_t len) const
    {
        return new db_file_cursor(this, key, len);
    }
    
    db_file_cursor* db_file::begin() const
    {
		return new db_file_cursor(this);
	}

    db_file_cursor* db_file::bound(const void* begin, uint32_t blen,
                                   const void* end, uint32_t elen) const
    {
        return new db_file_cursor(this, begin, blen, end, elen);
    }
	
    ////////////////////////////////////////////////////////////////////////////
    db_file_cursor::db_file_cursor(const db_file* dbf,
                                   const void* key, uint32_t len)
        : dbf_(dbf)
        , begin_(key), blen_(len)
        , end_(key), elen_(len)
        , pos_(0), blk_(0), dbb_(NULL)
        , lower_(NULL), upper_(NULL)
    {
        blk_ = dbf_->count();
        dbb_ = dbf_->fetch(pos_);
		if(dbb_) {
            lower_ = dbb_->lower_bound(begin_, blen_);
            upper_ = dbb_->upper_bound(end_, elen_);
			step_to_block();
		}
    }

	db_file_cursor::db_file_cursor (const db_file* dbf)
		: dbf_(dbf)
        , begin_(NULL), blen_(0)
        , end_(NULL), elen_(0)
		, pos_(0), blk_(0), dbb_(NULL)
		, lower_(NULL), upper_(NULL)
	{
		blk_ = dbf_->count();
		dbb_ = dbf_->fetch(pos_);
		if(dbb_) {
			lower_ = dbb_->lower_bound();
			upper_ = dbb_->upper_bound();
			step_to_block();
		}
	}

    db_file_cursor::db_file_cursor(const db_file* dbf,
                                   const void* begin, uint32_t blen,
                                   const void* end, uint32_t elen)
        : dbf_(dbf)
        , begin_(begin), blen_(blen)
        , end_(end), elen_(elen)
        , pos_(0), blk_(0), dbb_(NULL)
        , lower_(NULL), upper_(NULL)
    {
        blk_ = dbf_->count();
        dbb_ = dbf_->fetch(pos_);
        if(dbb_) {
            lower_ = dbb_->lower_bound(begin_, blen_);
            upper_ = dbb_->upper_bound(end_, elen_);
            step_to_block();
        }
    }

    
    db_file_cursor::~db_file_cursor()
    {
        delete lower_;
        lower_ = NULL;
        delete upper_;
        upper_ = NULL;
        delete dbb_;
        dbb_ = NULL;
    }

    bool db_file_cursor::step_to_block()
    {
        while(!lower_->valid() || lower_->equal(upper_)) {
            delete dbb_;
            dbb_ = NULL;
            delete lower_;
            lower_ = NULL;
            delete upper_;
            upper_ = NULL;

            ++pos_;
            if(pos_ >= blk_)
                return false;

            dbb_ = dbf_->fetch(pos_);
            // 如果没有排序则认为是最后一个数据块，不做查找。
            if(begin_ && blen_ > 0) {
				if(!dbb_->sorted()) {
					return false;
				}
                lower_ = dbb_->lower_bound(begin_, blen_);
                upper_ = dbb_->upper_bound(end_, elen_);
			}
			else {
				lower_ = dbb_->lower_bound();
				upper_ = dbb_->upper_bound();
			}
        }
        return true;
    }

    bool db_file_cursor::next()
    {
//        if(lower_->valid() && !lower_->equal(upper_)) {
//            if(lower_->next()) {
//                return true;
//            }
//        }
//        return step_to_block();

        lower_->next();
        return step_to_block();
    }

    bool db_file_cursor::valid() const
    {
        return lower_ && (pos_ < blk_);
    }

    const void* db_file_cursor::key() const
    {
        return lower_->key();
    }

    const void* db_file_cursor::val() const
    {
        return lower_->val();
    }

}
