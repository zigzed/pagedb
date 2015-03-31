/**
 * create: 2014-01-04
 * author: zigzed@gmail.com
 */
#ifndef PAGEDB_OPTIONS_HPP
#define PAGEDB_OPTIONS_HPP
#include <stddef.h>
#include <stdint.h>

namespace pagedb {

    enum CompressionType {
        kCompressionNone,
        kCompressionSnappy,
        kCompressionLZ4
    };

    enum OpenMode {
        kReadOnly,
        kWriteOnly
    };

    enum CheckMode {
        kCheckNone,
        kCheckHeader,
        kCheckDetail
    };

    struct Options {
        uint32_t        block_size;
        CompressionType compression;
        OpenMode        open_mode;
        CheckMode       check_mode;

        Options()
            : file_cache(1024 * 1024)
            , block_size(64 * 1024)
            , compression(kCompressionSnappy)
            , open_mode(kReadOnly)
            , fix_at_open(false)
        {
        }
    };

}

#endif
