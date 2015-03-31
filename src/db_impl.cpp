/**
 * create: 2015-01-04
 * author: zigzed@gmail.com
 */
#include "db_impl.hpp"
#include "db_util.hpp"
#include "pagedb/pagedb/iterator.hpp"
#include "cxxlib2/sys/filesystem.hpp"
#include "cxxlib2/cfs/chunkfs.hpp"
#include "cxxlib2/alg/bloomfilter.hpp"
#include "cxxlib2/cvt/xxhash.hpp"
#include "snappy-1.1.2/snappy-c.h"
#include "lz4.h"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace pagedb {

    static const uint32_t   PAGEDB_MAGIC_NUMBER   = 0x50444200;
    static const uint32_t   PAGEDB_VERSION        = 0x00000001;
    static const uint32_t   PAGEDB_BLOCKCACHE_LEN = 64 * 1024;
    static const char*      PAGEDB_PDB_EXT        = ".pag";
    static const char*      PAGEDB_IDX_EXT        = ".idx";

#pragma pack(push)
#pragma pack(1)
    struct pagdb_header {
        uint32_t magic_number;
        uint32_t version;
        uint32_t compression_type;
        uint32_t block_cache_size;
        uint32_t checksum;
    };

    struct block_header {
        uint32_t orglen;
        uint32_t cmzlen;
        uint32_t chksum;
    };
#pragma pack(pop)

    namespace {

        std::string make_pdb_name(const std::string& dbname)
        {
            cpp::sys::make_dir(cpp::sys::Path(dbname.c_str()).base().c_str(), 0755);

            return dbname + PAGEDB_PDB_EXT;
        }

        std::string make_idx_name(const std::string& dbname)
        {
            cpp::sys::make_dir(cpp::sys::Path(dbname.c_str()).base().c_str(), 0755);

            return dbname + PAGEDB_IDX_EXT;
        }

    }

    class pdb_writer::block_cache_t {
    public:
        block_cache_t(size_t size, CompressionType comp, int mfd, int ifd);
        ~block_cache_t();

        bool    save(const Slice& key, const Slice& val);
        bool    sync(bool flush);

        size_t  used() const;
        size_t  free() const;
        size_t  size() const;

        const uint8_t* data() const;

    private:
        static size_t length_of_rec(const Slice& key, const Slice& val);
        static size_t length_of_idx(const Slice& low, const Slice& hi);

        off_t   save_pdb();
        bool    save_idx(off_t off);

        uint8_t*    begin_;
        uint8_t*    cur_;
        uint8_t*    end_;

        uint8_t*    compressed_;
        size_t      csize_;
        CompressionType ctype_;

        Slice       lower_;
        Slice       upper_;
        off_t       offset_;

        int         pdb_;
        int         idx_;
    };

    pdb_writer::block_cache_t::block_cache_t(size_t size, CompressionType comp, int mfd, int ifd)
        : begin_(NULL), cur_(NULL), end_(NULL)
        , compressed_(NULL), csize_(0), ctype_(comp)
        , pdb_(mfd), idx_(ifd)
    {
        if(size < PAGEDB_BLOCKCACHE_LEN) {
            size = PAGEDB_BLOCKCACHE_LEN;
        }

        begin_ = (uint8_t* )malloc(size);
        if(begin_ == NULL) {
            throw pagedb_error(Status::IOError("no enough memory for block cache"));
        }
        cur_ = begin_;
        end_ = begin_ + size;

        switch(comp) {
        case kCompressionNone:
            csize_ = size + sizeof(uint16_t) * 2;
            break;
        case kCompressionSnappy:
            csize_ = snappy_max_compressed_length(size);
            break;
        case kCompressionLZ4:
            csize_ = LZ4_compressBound(size);
            break;
        default:
            throw pagedb_error(Status::InvalidArgument("unknown compression type"));
        }

        if(csize_ > 0) {
            csize_ += sizeof(block_header);

            compressed_ = (uint8_t* )malloc(csize_);
            if(compressed_ == NULL) {
                throw pagedb_error(Status::IOError("no enough memory for compressing"));
            }
        }
    }

    pdb_writer::block_cache_t::~block_cache_t()
    {
        sync(true);
        ::free(compressed_);
        compressed_ = NULL;
        ::free(begin_);
        begin_ = NULL;
    }

    size_t pdb_writer::block_cache_t::size() const
    {
        return end_ - begin_;
    }

    size_t pdb_writer::block_cache_t::used() const
    {
        return cur_ - begin_;
    }

    size_t pdb_writer::block_cache_t::free() const
    {
        return end_ - cur_;
    }

    const uint8_t* pdb_writer::block_cache_t::data() const
    {
        return begin_;
    }

    bool pdb_writer::block_cache_t::save(const Slice& key, const Slice& val)
    {
        if(length_of_rec(key, val) > this->size()) {
            throw pagedb_error(Status::InvalidArgument("key value pair is too big for block cache"));
        }

        if(length_of_rec(key, val) > this->free()) {
            sync(false);
        }

        uint16_t klen = key.size();
        uint16_t vlen = val.size();
        uint8_t* kptr = NULL;
        memcpy(cur_, &klen, sizeof(klen));
        cur_ += klen;
        memcpy(cur_, &vlen, sizeof(vlen));
        cur_ += vlen;
        memcpy(cur_, key.data(), klen);
        kptr = cur_;
        cur_ += klen;
        memcpy(cur_, val.data(), vlen);
        cur_ += vlen;

        if(lower_.empty() || key.compare(lower_) < 0) {
            lower_ = Slice((const char* )kptr, klen);
        }
        if(upper_.empty() || key.compare(upper_) > 0) {
            upper_ = Slice((const char* )kptr, klen);
        }
        return true;
    }

    bool pdb_writer::block_cache_t::sync(bool flush)
    {
        if(used() > 0) {
            off_t pos = save_pdb();
            save_idx(pos);
        }

        if(flush) {
            fdatasync(pdb_);
            fdatasync(idx_);
        }

        return true;
    }

    ////////////////////////////////////////////////////////////////////////////
    pdb_writer::pdb_writer(const Options& option, const std::string& name)
        : options_(option), dbname_(name)
        , block_cache_(NULL)
        , page_header_(NULL)
        , pagdb_fd_(-1), index_fd_(-1)
    {
        if(option.open_mode != kWriteOnly) {
            throw pagedb_error(Status::InvalidArgument("invalid open mode"));
        }

        std::string pdbname = make_pdb_name(name);
        std::string idxname = make_idx_name(name);

        int flags = O_RDWR | O_CREAT | O_APPEND;
        pagdb_fd_ = open(pdbname.c_str(), flags, 0644);
        if(pagdb_fd_ == -1) {
            throw pagedb_error(Status::IOError("open pagedb failed", strerror(errno)));
        }
        index_fd_ = open(idxname.c_str(), flags, 0644);
        if(index_fd_ == -1) {
            throw pagedb_error(Status::IOError("open index failed", strerror(errno)));
        }

        off_t pagdb_len = lseek(pagdb_fd_, 0, SEEK_END);
        off_t index_len = lseek(index_fd_, 0, SEEK_END);
        if(pagdb_len == 0 && index_len == 0) {
            save_header(pagdb_fd_, index_fd_);
        }
        else if(pagdb_len == 0 && index_len > 0) {
            close(index_fd_);
            index_fd_ = -1;
            ::remove(idxname.c_str());

            index_fd_ = open(idxname.c_str(), flags, 0644);
            if(index_fd_ == -1) {
                throw pagedb_error(Status::IOError("open index failed", strerror(errno)));
            }
        }
        else if(pagdb_len > 0 && index_len == 0) {
            make_indexs(pagdb_fd_, index_fd_);
            load_header(pagdb_fd_, index_fd_);
        }
        else {
            load_header(pagdb_fd_, index_fd_);
        }

        lseek(pagdb_fd_, 0, SEEK_END);
        lseek(index_fd_, 0, SEEK_END);
    }

    pdb_writer::~pdb_writer()
    {
        Sync();

        if(pagdb_fd_ != -1) {
            close(pagdb_fd_);
            pagdb_fd_ = -1;
        }
        if(index_fd_ != -1) {
            close(index_fd_);
            index_fd_ = -1;
        }

        delete block_cache_;
        block_cache_ = NULL;

        free(page_header_);
        page_header_ = NULL;
    }

    Status pdb_writer::Put(const Slice& key,
                           const Slice& val)
    {
        try {
            block_cache_->save(key, val);
        }
        catch(const pagedb_error& e) {
            return e.status();
        }
        return Status::OK();
    }

    Status pdb_writer::Sync()
    {
        try {
            block_cache_->sync(false);
        }
        catch(const pagedb_error& e) {
            return e.status();
        }
        return Status::OK();
    }

    Iterator* pdb_writer::NewIterator()
    {
        throw pagedb_error(Status::NotSupported("reading from a writeonly file"));
    }

    void pdb_writer::save_header(int pfd, int ifd)
    {
        uint32_t block_cache_size = options_.block_size;
        if(block_cache_size < PAGEDB_BLOCKCACHE_LEN) {
            block_cache_size = PAGEDB_BLOCKCACHE_LEN;
        }
        page_header_ = (pagdb_header* )malloc(sizeof(pagdb_header));
        page_header_->magic_number  = PAGEDB_MAGIC_NUMBER;
        page_header_->version       = PAGEDB_VERSION;
        page_header_->compression_type = options_.compression;
        page_header_->block_cache_size = block_cache_size;
        page_header_->checksum      = 0;
        uint32_t checksum = XXH32(page_header_, sizeof(pagdb_header), 0);
        page_header_->checksum      = checksum;

        _pwrite(pfd, page_header_, sizeof(pagdb_header), 0, "writing pagedb header");

        block_cache_ = new block_cache_t(block_cache_size, options_.compression, pfd, ifd);
    }

    void pdb_writer::load_header(int pfd, int ifd)
    {
        page_header_ = (pagdb_header* )malloc(sizeof(pagdb_header));
        size_t s = _pread(pfd, 0, page_header_, sizeof(pagdb_header), "reading pagedb header");
        if(s != sizeof(pagdb_header)) {
            throw pagedb_error(Status::Corruption("reading pagedb header mismatched"));
        }
        if(page_header_->magic_number != PAGEDB_MAGIC_NUMBER) {
            throw pagedb_error(Status::Corruption("invalid magic number"));
        }
        if(page_header_->version != PAGEDB_VERSION) {
            throw pagedb_error(Status::Corruption("invalid version"));
        }
        CompressionType ctype = (CompressionType)page_header_->compression_type;
        switch(ctype) {
        case kCompressionNone:
        case kCompressionSnappy:
        case kCompressionLZ4:
            break;
        default:
            throw pagedb_error(Status::NotSupported("unknown compression algorithm"));
        }

        uint32_t checksum = page_header_->checksum;
        page_header_->checksum = 0;
        if(XXH32(page_header_, sizeof(pagdb_header), 0) != checksum) {
            throw pagedb_error(Status::Corruption("invalid checksum"));
        }
        page_header_->checksum = checksum;

        block_cache_ = new block_cache_t(page_header_->block_cache_size,
                                         (CompressionType)page_header_->compression_type,
                                         pfd, ifd);
    }

    void pdb_writer::make_indexs(int pfd, int ifd)
    {
        // TODO: walk the pagdb and generate the index
    }


    off_t pdb_writer::block_cache_t::save_pdb()
    {
        uint8_t* compressed = compressed_;
        block_header* header = (block_header* )compressed;
        compressed += sizeof(block_header);
        header->orglen = this->used();
        header->cmzlen = header->orglen;

        size_t compressed_length = 0;
        switch(ctype_) {
        case kCompressionNone:
            memcpy(compressed, begin_, this->used());
            break;
        case kCompressionSnappy:
            compressed_length = csize_;
            if(snappy_compress((const char* )begin_, used(),
                               (char* )compressed, &compressed_length)
                    != SNAPPY_OK) {
                throw pagedb_error(Status::IOError("snappy compressing error"));
            }
            header->cmzlen = compressed_length;
            break;
        case kCompressionLZ4:
            compressed_length = LZ4_compress((const char* )begin_,
                                             (char* )compressed, used());
            if(compressed_length == 0) {
                throw pagedb_error(Status::IOError("LZ4 compressing error"));
            }
            header->cmzlen = compressed_length;
            break;
        default:
            throw pagedb_error(Status::InvalidArgument("unknown compression algorithm"));
        }
        header->chksum = 0;
        uint32_t csum = XXH32(compressed_, header->cmzlen + sizeof(block_header), 0);
        header->chksum = csum;

        off_t pos = lseek(pdb_, 0, SEEK_CUR);
        _write(pdb_, compressed_, header->cmzlen + sizeof(block_header), "saving page file");
        cur_ = begin_;

        return pos;
    }

    /** Ë÷Òý¼ÇÂ¼£º
     * lower key length (2 bytes)
     * upper key length (2 bytes)
     * lower key (lower key length bytes)
     * upper key (upper key length bytes)
     * offset (8 bytes, compressed pdb file)
     * checksum (4 bytes)
     */

    bool pdb_writer::block_cache_t::save_idx(off_t pos)
    {
        if(length_of_idx(lower_, upper_) > csize_) {
            throw pagedb_error(Status::InvalidArgument("index key range is overflow"));
        }
        uint16_t klen1 = lower_.size();
        uint16_t klen2 = upper_.size();

        size_t actual = 0;
        memcpy(compressed_ + actual, &klen1, sizeof(klen1));
        actual += sizeof(klen1);
        memcpy(compressed_ + actual, &klen2, sizeof(klen2));
        actual += sizeof(klen2);
        memcpy(compressed_ + actual, lower_.data(), klen1);
        actual += klen1;
        memcpy(compressed_ + actual, upper_.data(), klen2);
        actual += klen2;
        memcpy(compressed_ + actual, &pos, sizeof(pos));
        actual += sizeof(pos);

        uint32_t checksum = XXH32(compressed_, actual, 0);
        memcpy(compressed_ + actual, &checksum, sizeof(checksum));
        actual += sizeof(checksum);

        lower_.clear();
        upper_.clear();

        _write(idx_, compressed_, actual, "saving index");

        return true;
    }

    size_t pdb_writer::block_cache_t::length_of_rec(const Slice& key, const Slice& val)
    {
        // pagedb record:
        //  key length + val length +
        //  key + val
        return sizeof(uint16_t) + sizeof(uint16_t) + key.size() + val.size();
    }

    size_t pdb_writer::block_cache_t::length_of_idx(const Slice& low, const Slice& hi)
    {
        // index record:
        //  lower key length + upper key length +
        //  lower key + upper key +
        //  offset +
        //  checksum
        return sizeof(uint16_t) + sizeof(uint16_t) + low.size() + hi.size() +
                sizeof(off_t) + sizeof(uint32_t);
    }


    ////////////////////////////////////////////////////////////////////////////
    pdb_reader::pdb_reader(const Options& option, const std::string& name)
        : options_(option), dbname_(name)
        , block_(NULL), plain_(NULL), csize_(0)
        , page_header_(NULL)
        , pagdb_fd_(-1), index_fd_(-1)
    {
        if(option.open_mode != kReadOnly) {
            throw pagedb_error(Status::InvalidArgument("invalid open mode"));
        }

        std::string pdbname = make_pdb_name(name);
        std::string idxname = make_idx_name(name);

        pagdb_fd_ = open(pdbname.c_str(), O_RDONLY | O_NOATIME);
        if(pagdb_fd_ == -1) {
            throw pagedb_error(Status::IOError("opening pagdb", strerror(errno)));
        }
        index_fd_ = open(idxname.c_str(), O_RDONLY | O_NOATIME);
        if(index_fd_ == -1) {
            throw pagedb_error(Status::IOError("opening index", strerror(errno)));
        }

        load_header(pagdb_fd_);
    }

    pdb_reader::~pdb_reader()
    {
        free(page_header_);
        page_header_ = NULL;

        free(block_);
        block_ = NULL;
        free(plain_);
        plain_ = NULL;

        if(pagdb_fd_ != -1) {
            close(pagdb_fd_);
            pagdb_fd_ = -1;
        }
        if(index_fd_ != -1) {
            close(index_fd_);
            index_fd_ = -1;
        }
    }

    Status pdb_reader::Put(const Slice& key,
                           const Slice& val)
    {
        throw pagedb_error(Status::NotSupported("writing to a readonly file"));
    }

    Status pdb_reader::Sync()
    {
        return Status::OK();
    }

    Iterator* pdb_reader::NewIterator()
    {
        // TODO:
    }

    void pdb_reader::load_header(int fd)
    {
        page_header_ = (pagdb_header* )malloc(sizeof(pagdb_header));
        size_t s = _pread(fd, 0, page_header_, sizeof(pagdb_header), "reading pagedb header");
        if(s != sizeof(pagdb_header)) {
            throw pagedb_error(Status::Corruption("reading pagedb header mismatched"));
        }
        if(page_header_->magic_number != PAGEDB_MAGIC_NUMBER) {
            throw pagedb_error(Status::Corruption("invalid magic number"));
        }
        if(page_header_->version != PAGEDB_VERSION) {
            throw pagedb_error(Status::Corruption("invalid version"));
        }
        CompressionType ctype = (CompressionType)page_header_->compression_type;
        switch(ctype) {
        case kCompressionNone:
        case kCompressionSnappy:
        case kCompressionLZ4:
            break;
        default:
            throw pagedb_error(Status::NotSupported("unknown compression algorithm"));
        }

        uint32_t checksum = page_header_->checksum;
        page_header_->checksum = 0;
        if(XXH32(page_header_, sizeof(pagdb_header), 0) != checksum) {
            throw pagedb_error(Status::Corruption("invalid checksum"));
        }
        page_header_->checksum = checksum;

        switch(ctype) {
        case kCompressionNone:
            csize_ = page_header_->block_cache_size;
            break;
        case kCompressionSnappy:
            csize_ = snappy_max_compressed_length(page_header_->block_cache_size);
            break;
        case kCompressionLZ4:
            csize_ = LZ4_compressBound(page_header_->block_cache_size);
            break;
        }
        csize_ += sizeof(block_header);

        plain_ = (uint8_t* )malloc(page_header_->block_cache_size);
        block_ = (uint8_t* )malloc(csize_);
    }

    void pdb_reader::seek_blocks(off_t pos)
    {
        block_header* header = (block_header* )block_;
        size_t s = _pread(pagdb_fd_, pos, header, sizeof(block_header), "reading block header");
        if(s != sizeof(block_header)) {
            throw pagedb_error(Status::IOError("reading block header mismatched"));
        }
        if(header->orglen > page_header_->block_cache_size) {
            throw pagedb_error(Status::Corruption("block header::block size mismatched"));
        }
        if(header->cmzlen > csize_ - sizeof(block_header)) {
            throw pagedb_error(Status::Corruption("block header::file size mismatched"));
        }
        pos += sizeof(block_header);
        s = _pread(pagdb_fd_, pos, block_ + sizeof(block_header), header->cmzlen, "reading block");
        if(s != header->cmzlen) {
            throw pagedb_error(Status::Corruption("block mismatched"));
        }

        uint32_t checksum = header->chksum;
        header->chksum = 0;
        if(XXH32(block_, header->cmzlen + sizeof(block_header), 0) != checksum) {
            throw pagedb_error(Status::Corruption("block checksum mismatched"));
        }
        header->chksum = checksum;

        size_t plain_length = page_header_->block_cache_size;
        switch(page_header_->compression_type) {
        case kCompressionNone:
            memcpy(plain_, block_ + sizeof(block_header), header->orglen);
            break;
        case kCompressionSnappy:
            if(snappy_uncompress((const char* )block_ + sizeof(block_header), header->cmzlen,
                                 (char* )plain_, &plain_length) != SNAPPY_OK) {
                throw pagedb_error(Status::Corruption("snappy uncompress error"));
            }
        case kCompressionLZ4:
            if(LZ4_decompress_fast((const char* )block_ + sizeof(block_header),
                                   (char* )plain_, header->orglen) != header->cmzlen) {
                throw pagedb_error(Status::Corruption("LZ4 uncompress error"));
            }
            break;
        }

        psize_ = header->orglen;
    }

    ////////////////////////////////////////////////////////////////////////////
/*
    Status sparsedb::save_header(int fd)
    {
        file_header_->checksum = 0;
        file_header_->checksum = XXH32(file_header_, sizeof(sfile_header), 0);

        const uint8_t*  buf = (const uint8_t* )file_header_;
        size_t          len = sizeof(sfile_header);

		_pwrite(fd, buf, len, 0);

        return Status::OK();
    }



    sparsedb::sparsedb(const Options& options, const std::string& name)
        : options_(options), dbname_(name)
        , ofile_cache_(NULL), block_cache_(NULL)
        , file_header_(NULL)
        , compressed_(NULL)
        , file_handle_(-1)
    {
        switch(options.open_mode) {
        case kReadOnly:
            open_for_reader();
            break;
        case kReadWrite:
            open_for_writer();
            break;
        }
    }

    Status sparsedb::open_for_writer()
    {
        file_header_ = new sfile_header;

        int flags = O_RDWR | O_APPEND | O_CREAT;
        int fd = open(dbname_.c_str(), flags, 0644);
        if(fd == -1) {
            throw sparse_error(Status::IOError(strerror(errno)));
        }
        file_handle_ = fd;
        off_t length = lseek(file_handle_, 0, SEEK_END);
        if(length > 0) {
            load_header(file_handle_);
        }
        else {
            file_header_->magic_number  = SPARSEDB_MAGIC_NUMBER;
            file_header_->version       = SPARSEDB_VERSION;
            file_header_->reserved      = 0;
            file_header_->data_end      = sizeof(sfile_header);
            file_header_->checksum      = 0;
            file_header_->compression_type = options_.compression;

            uint32_t block_size = options_.block_size;
            switch(options_.compression) {
            case kCompressionNone:
                block_size = 0;
                break;
            case kCompressionSnappy:
                if(block_size < 10) {
                    block_size = 64 * 1024;
                }
                block_size = snappy_max_compressed_length(block_size);
                compressed_= (uint8_t* )malloc(block_size);
            default:
                throw sparse_error(Status::NotSupported("unknown compression type"));
            }
            file_header_->compression_block_size = block_size;

            save_header(file_handle_);
        }

        lseek(file_handle_, file_header_->data_end, SEEK_SET);

        ofile_cache_ = new cache_t(options_.file_cache);
        block_cache_ = new cache_t(options_.block_size);

        return Status::OK();
    }

    Status sparsedb::open_for_reader()
    {
        file_header_ = new sfile_header;

        int flags = O_RDONLY;
        int fd = open(dbname_.c_str(), flags);
        if(fd == -1) {
            throw sparse_error(Status::IOError(strerror(errno)));
        }
        file_handle_ = fd;
        return load_header(file_handle_);
    }
*/

}
