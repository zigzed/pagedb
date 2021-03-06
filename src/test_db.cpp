/**
 * create: 2015-04-03
 * author: zigzed@gmail.com
 */
#include "pagedb/db.hpp"
#include <stdio.h>
#include <vector>
#include <map>
#include "cxxlib2/time/tickcount.hpp"
#include "cxxlib2/sys/thread.hpp"

void sequence_insert()
{
    pagedb::DB* pdb = NULL;
    pagedb::DB::Open(pagedb::Options().set_block_size(1 * 1024).set_read_only(false),
                     "pagedb1.pdb", 8, 4, &pdb);
    {
        char key[64];
        for(int i = 0; i < 1000000; ++i) {
            sprintf(key, "%08d", i);
            pdb->Put(pagedb::Slice(key), pagedb::Slice((const char* )&i, sizeof(i)));
        }
        pdb->Sync();
    }

    {
        pagedb::Iterator* iterator = pdb->NewIterator();
        iterator->Seek(pagedb::Slice("0000000", 7));
        int found = 0;
        for(; iterator->Valid(); iterator->Next()) {
            if(memcmp(iterator->key().data(), "0000000", 7) > 0)
                break;
            char key[64] = {0};
            strncpy(key, iterator->key().data(), 8);
            int  val = *(int* )iterator->val().data();

            char exp_key[64];
            sprintf(exp_key, "0000000%d", found);
            int  exp_val = found;

            if(strncmp(exp_key, key, 8) == 0 && exp_val == val) {
                printf("[done]: %s:%d\n", (char* )key, *(int* )iterator->val().data());
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       (char* )key, *(int* )iterator->val().data(),
                       exp_key, exp_val
                       );
            }
            ++found;
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }
        delete iterator;
        iterator = NULL;
    }
    {
        std::string val;
        pagedb::Status s = pdb->Get(pagedb::Slice("00000008", 8), val);
        if(!s.ok()) {
            printf("[fail]: get failed\n");
        }
        else {
            printf("[done]: %s:%d\n", "00000008", *(int* )val.data());
        }

        s = pdb->Get(pagedb::Slice("11111111", 8), val);
        if(s.ok()) {
            printf("[fail]: get unexpected result\n");
        }
        if(s.IsNotFound()) {
            printf("[done]: non-exist not found\n");
        }
    }
    {
        pagedb::Iterator* cursor = pdb->NewIterator();
        cursor->Seek(pagedb::Slice("0000008", 7));
        int found = 0;
        for(; cursor->Valid(); cursor->Next()) {
            if(!cursor->key().starts_with("0000008"))
                break;
            char key[64] = {0};
            strncpy(key, cursor->key().data(), 8);
            int val = *(int* )cursor->val().data();

            char exp_key[64] = {0};
            sprintf(exp_key, "0000008%d", found);
            int  exp_val = 80 + found;

            if(strncmp(exp_key, key, 8) == 0 && exp_val == val) {
                printf("[done]: %s:%d\n", key, val);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       (char* )key, val,
                       exp_key, exp_val
                       );
            }
            ++found;
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }
    {
        pagedb::Iterator* cursor = pdb->NewIterator();
        cursor->Seek(pagedb::Slice("0001000", 7));
        int found = 0;
        for(; cursor->Valid(); cursor->Next()) {
            if(!cursor->key().starts_with("0001000"))
                break;
            char key[64] = {0};
            strncpy(key, cursor->key().data(), 8);
            int val = *(int* )cursor->val().data();

            char exp_key[64] = {0};
            sprintf(exp_key, "0001000%d", found);
            int  exp_val = 10000 + found;

            if(strncmp(exp_key, key, 8) == 0 && exp_val == val) {
                printf("[done]: %s:%d\n", key, val);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       (char* )key, val,
                       exp_key, exp_val
                       );
            }
            ++found;
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }

}

/*
void random_insert()
{
    pagedb::db_file dbf("pagedb2.dat", 1 * 1024, 8, 4, NULL, false);

    std::vector<int > seqs;
    seqs.reserve(1000000);
    for(int i = 0; i < 1000000; ++i) {
        seqs.push_back(i);
    }
    std::random_shuffle(seqs.begin(), seqs.end());

    {
        pagedb::db_block* block = dbf.tail();
        char key[64];
        for(int i = 0; i < 1000000; ++i) {
            int v = seqs[i];
            sprintf(key, "%08d", v);
            while(!block->save(key, &v)) {
                block->sort();
                delete block;
                block = dbf.insert();
            }
        }
        block->sort();
        delete block;
        block = NULL;
    }

    {
        std::vector<std::pair<std::string, int > > res;
        pagedb::db_file_cursor* cursor = dbf.begin("0000000", 7);
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            if(memcmp(cursor->key(), "0000000", 7) > 0)
                break;
            char key[64] = {0};
            strncpy(key, (char* )cursor->key(), 8);
            int  val = *(int* )cursor->val();
            res.push_back(std::make_pair(key, val));
            ++found;
        }
        std::sort(res.begin(), res.end());

        for(int i = 0; i < found; ++i) {
            char exp_key[64];
            sprintf(exp_key, "0000000%d", i);
            int  exp_val = i;

            if(strncmp(exp_key, res[i].first.c_str(), 8) == 0 && exp_val == res[i].second) {
                printf("[done]: %s:%d\n", res[i].first.c_str(), res[i].second);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       res[i].first.c_str(), res[i].second,
                       exp_key, exp_val
                       );
            }
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }
    {
        std::vector<std::pair<std::string, int > > res;
        pagedb::db_file_cursor* cursor = dbf.begin("0000008", 7);
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            if(memcmp(cursor->key(), "0000008", 7) > 0)
                break;
            char key[64] = {0};
            strncpy(key, (char* )cursor->key(), 8);
            int  val = *(int* )cursor->val();
            res.push_back(std::make_pair(key, val));
            ++found;
        }
        std::sort(res.begin(), res.end());

        for(int i = 0; i < found; ++i) {
            char exp_key[64];
            sprintf(exp_key, "0000008%d", i);
            int  exp_val = 80 + i;

            if(strncmp(exp_key, res[i].first.c_str(), 8) == 0 && exp_val == res[i].second) {
                printf("[done]: %s:%d\n", res[i].first.c_str(), res[i].second);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       res[i].first.c_str(), res[i].second,
                       exp_key, exp_val
                       );
            }
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }

    {
        std::vector<std::pair<std::string, int > > res;
        pagedb::db_file_cursor* cursor = dbf.begin("0001000", 7);
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            if(memcmp(cursor->key(), "0001000", 7) > 0)
                break;
            if(memcmp(cursor->key(), "0001000", 7) > 0)
                break;
            char key[64] = {0};
            strncpy(key, (char* )cursor->key(), 8);
            int  val = *(int* )cursor->val();
            res.push_back(std::make_pair(key, val));
            ++found;
        }
        std::sort(res.begin(), res.end());

        for(int i = 0; i < found; ++i) {

            char exp_key[64];
            sprintf(exp_key, "0001000%d", i);
            int  exp_val = 10000 + i;

            if(strncmp(exp_key, res[i].first.c_str(), 8) == 0 && exp_val == res[i].second) {
                printf("[done]: %s:%d\n", res[i].first.c_str(), res[i].second);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       res[i].first.c_str(), res[i].second,
                       exp_key, exp_val
                       );
            }
        }
        if(found != 10) {
            printf("[fail]: found count expected 10, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }

}

void append_insert()
{
    std::vector<int > seqs;
    seqs.reserve(1000000);
    for(int i = 0; i < 1000000; ++i) {
        seqs.push_back(i);
    }
    std::random_shuffle(seqs.begin(), seqs.end());

    {
        pagedb::db_file dbf("pagedb3.dat", 1 * 1024, 8, 4, NULL, false);
        pagedb::db_block* block = dbf.tail();
        char key[64];
        for(int i = 0; i < 500000; ++i) {
            int v = seqs[i];
            sprintf(key, "%08d", v);
            while(!block->save(key, &v)) {
                block->sort();
                delete block;
                block = dbf.insert();
            }
        }
        block->sort();
        delete block;
        block = NULL;
    }
    {
        pagedb::db_file dbf("pagedb3.dat", 1 * 1024, 8, 4, NULL, false);
        pagedb::db_block* block = dbf.tail();
        char key[64];
        for(int i = 500000; i < 1000000; ++i) {
            int v = seqs[i];
            sprintf(key, "%08d", v);
            while(!block->save(key, &v)) {
                block->sort();
                delete block;
                block = dbf.insert();
            }
        }
        block->sort();
        delete block;
        block = NULL;
    }

    {
        pagedb::db_file dbf("pagedb3.dat", 1 * 1024, 8, 4, NULL, true);
        std::vector<std::pair<std::string, int > > res;
        pagedb::db_file_cursor* cursor = dbf.begin("0000000", 7);
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            if(memcmp(cursor->key(), "0000000", 7) > 0)
                break;
            char key[64] = {0};
            strncpy(key, (char* )cursor->key(), 8);
            int  val = *(int* )cursor->val();
            res.push_back(std::make_pair(key, val));
            ++found;
        }
        std::sort(res.begin(), res.end());

        for(int i = 0; i < found; ++i) {
            char exp_key[64];
            sprintf(exp_key, "0000000%d", i);
            int  exp_val = i;

            if(strncmp(exp_key, res[i].first.c_str(), 8) == 0 && exp_val == res[i].second) {
                printf("[done]: %s:%d\n", res[i].first.c_str(), res[i].second);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       res[i].first.c_str(), res[i].second,
                       exp_key, exp_val
                       );
            }
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }
    {
        pagedb::db_file dbf("pagedb3.dat", 1 * 1024, 8, 4, NULL, true);
        std::vector<std::pair<std::string, int > > res;
        pagedb::db_file_cursor* cursor = dbf.begin("0000008", 7);
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            if(memcmp(cursor->key(), "0000008", 7) > 0)
                break;
            char key[64] = {0};
            strncpy(key, (char* )cursor->key(), 8);
            int  val = *(int* )cursor->val();
            res.push_back(std::make_pair(key, val));
            ++found;
        }
        std::sort(res.begin(), res.end());

        for(int i = 0; i < found; ++i) {
            char exp_key[64];
            sprintf(exp_key, "0000008%d", i);
            int  exp_val = 80 + i;

            if(strncmp(exp_key, res[i].first.c_str(), 8) == 0 && exp_val == res[i].second) {
                printf("[done]: %s:%d\n", res[i].first.c_str(), res[i].second);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       res[i].first.c_str(), res[i].second,
                       exp_key, exp_val
                       );
            }
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }

    {
        pagedb::db_file dbf("pagedb3.dat", 1 * 1024, 8, 4, NULL, true);
        std::vector<std::pair<std::string, int > > res;
        pagedb::db_file_cursor* cursor = dbf.begin("0001000", 7);
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            if(memcmp(cursor->key(), "0001000", 7) > 0)
                break;
            if(memcmp(cursor->key(), "0001000", 7) > 0)
                break;
            char key[64] = {0};
            strncpy(key, (char* )cursor->key(), 8);
            int  val = *(int* )cursor->val();
            res.push_back(std::make_pair(key, val));
            ++found;
        }
        std::sort(res.begin(), res.end());

        for(int i = 0; i < found; ++i) {

            char exp_key[64];
            sprintf(exp_key, "0001000%d", i);
            int  exp_val = 10000 + i;

            if(strncmp(exp_key, res[i].first.c_str(), 8) == 0 && exp_val == res[i].second) {
                printf("[done]: %s:%d\n", res[i].first.c_str(), res[i].second);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       res[i].first.c_str(), res[i].second,
                       exp_key, exp_val
                       );
            }
        }
        if(found != 10) {
            printf("[fail]: found count expected 10, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }
}

void iterator()
{
    {
        pagedb::db_file dbf("pagedb4.dat", 1 * 1024, 8, 4, NULL, false);
        pagedb::db_block* block = dbf.tail();
        char key[64];
        for(int i = 0; i < 500000; ++i) {
            int v = i;
            sprintf(key, "%08d", v);
            while(!block->save(key, &v)) {
                block->sort();
                delete block;
                block = dbf.insert();
            }
        }
        block->sort();
        delete block;
        block = NULL;
    }
    {
        pagedb::db_file dbf("pagedb4.dat", 1 * 1024, 8, 4, NULL, false);
        pagedb::db_block* block = dbf.tail();
        char key[64];
        for(int i = 500000; i < 1000000; ++i) {
            int v = i;
            sprintf(key, "%08d", v);
            while(!block->save(key, &v)) {
                block->sort();
                delete block;
                block = dbf.insert();
            }
        }
        block->sort();
        delete block;
        block = NULL;
    }

    cpp::time::tick_count tc;
    {
        pagedb::db_file dbf("pagedb4.dat", 1 * 1024, 8, 4, NULL, true);
        pagedb::db_file_cursor* cursor = dbf.begin();
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            int  val = *(int* )cursor->val();
            if(val != found) {
                printf("[fail]: %d, expected: %d\n",
                       val, found
                       );
            }
            ++found;
        }
        if(found != 1000000) {
            printf("[failed]: found count expected 1000000, got %d\n", found);
        }
        delete cursor;
        cursor = NULL;
    }
    cpp::time::duration ts = tc.stop();
    printf("iterator: %d in %ld.%09ld sec\n",
           1000000,
           ts.as_timespec().tv_sec,
           ts.as_timespec().tv_nsec);
}

void insert_performance(const char* filename, uint32_t blocksize)
{
    size_t  count = 10000000;
    uint32_t klen = 20;
    uint32_t vlen = 4;

    pagedb::db_file dbf(filename, blocksize, klen, vlen, NULL, false);
    pagedb::db_block* block = dbf.tail();

    cpp::time::tick_count tc;
    {
        char key[64];
        for(size_t i = 0; i < count; ++i) {
            sprintf(key, "%020zd", i);
            if(!block->save(key, &i)) {
                block->sort();
                delete block;
                block = dbf.insert();
            }
        }
        block->sort();
        delete block;
        block = NULL;
    }
    cpp::time::duration ts = tc.stop();
    printf("insert performance: %zd MB in %ld.%09ld sec\n",
           count * (klen + vlen) / 1024 / 1024,
           ts.as_timespec().tv_sec,
           ts.as_timespec().tv_nsec);
}

void random_insert_performance(const char* filename, uint32_t blocksize)
{
    size_t  count = 10000000;
    uint32_t klen = 20;
    uint32_t vlen = 4;

    std::vector<int > seqs;
    seqs.reserve(count);
    for(int i = 0; i < count; ++i)
        seqs.push_back(i);

    std::random_shuffle(seqs.begin(), seqs.end());

    pagedb::db_file dbf(filename, blocksize, klen, vlen, NULL, false);
    pagedb::db_block* block = dbf.tail();

    cpp::time::tick_count tc;
    {
        char key[64];
        for(size_t i = 0; i < count; ++i) {
            int v = seqs[i];
            sprintf(key, "%020zd", v);
            if(!block->save(key, &v)) {
                block->sort();
                delete block;
                block = dbf.insert();
            }
        }
        block->sort();
        delete block;
        block = NULL;
    }
    cpp::time::duration ts = tc.stop();

    printf("insert performance: %zd MB in %ld.%09ld sec\n",
           count * (klen + vlen) / 1024 / 1024,
           ts.as_timespec().tv_sec,
           ts.as_timespec().tv_nsec);
}

void select_performance(const char* filename, uint32_t blocksize)
{
    uint32_t klen = 20;
    uint32_t vlen = 4;

    pagedb::db_file dbf(filename, blocksize, klen, vlen, NULL, true);

    std::vector<std::pair<std::string, int > > res;
    res.reserve(10);

    cpp::time::tick_count tc;
    {
        pagedb::db_file_cursor* cursor = dbf.begin("0000000000000005000", 19);
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            if(memcmp(cursor->key(), "0000000000000005000", 19) > 0)
                break;
            char key[64] = {0};
            strncpy(key, (char* )cursor->key(), 20);
            int  val = *(int* )cursor->val();
            res.push_back(std::make_pair(key, val));
            ++found;
        }
        delete cursor;
        cursor = NULL;
        cpp::time::duration ts = tc.stop();

        std::sort(res.begin(), res.end());
        for(int i = 0; i < found; ++i) {
            char exp_key[64];
            sprintf(exp_key, "0000000000000005000%d", i);
            int  exp_val = 50000 + i;

            if(strncmp(exp_key, res[i].first.c_str(), 20) == 0 && exp_val == res[i].second) {
                printf("[done]: %s:%d\n",
                       res[i].first.c_str(), res[i].second);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       res[i].first.c_str(), res[i].second,
                       exp_key, exp_val
                       );
            }
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }

        printf("select performance 1: in %ld.%09ld sec\n",
               ts.as_timespec().tv_sec,
               ts.as_timespec().tv_nsec);
    }
    res.clear();

    tc.resume();
    {
        pagedb::db_file_cursor* cursor = dbf.begin("0000000000000905000", 19);
        int found = 0;
        for(; cursor->valid(); cursor->next()) {
            if(memcmp(cursor->key(), "0000000000000905000", 19) > 0)
                break;
            char key[64] = {0};
            strncpy(key, (char* )cursor->key(), 20);
            int  val = *(int* )cursor->val();
            res.push_back(std::make_pair(key, val));
            ++found;
        }
        delete cursor;
        cursor = NULL;
        cpp::time::duration ts = tc.stop();

        std::sort(res.begin(), res.end());
        for(int i = 0; i < found; ++i) {
            char exp_key[64];
            sprintf(exp_key, "0000000000000905000%d", i);
            int  exp_val = 9050000 + i;

            if(strncmp(exp_key, res[i].first.c_str(), 20) == 0 && exp_val == res[i].second) {
                printf("[done]: %s:%d\n",
                       res[i].first.c_str(), res[i].second);
            }
            else {
                printf("[fail]: %s:%d, expected: %s:%d\n",
                       res[i].first.c_str(), res[i].second,
                       exp_key, exp_val
                       );
            }
        }
        if(found != 10) {
            printf("[failed]: found count expected 10, got %d\n", found);
        }

        printf("select performance 1: in %ld.%09ld sec\n",
               ts.as_timespec().tv_sec,
               ts.as_timespec().tv_nsec);
    }
}
*/

void writer()
{
    printf("%s writer thread::Put ....\n", cpp::time::datetime::now().format("hms.n").c_str());
    {
        pagedb::DB* pdb = NULL;
        pagedb::DB::Open(pagedb::Options().set_block_size(1 * 1024).set_read_only(false),
                         "pagedb_thread.pdb", 8, 4, &pdb);
        {
            char key[64];
            for(int i = 0; i < 1000000; ++i) {
                sprintf(key, "%08d", i);
                pdb->Put(pagedb::Slice(key), pagedb::Slice((const char* )&i, sizeof(i)));
            }
        }
        pdb->Sync();

        delete pdb;
        pdb = NULL;
    }
    printf("%s writer thread::Put done\n", cpp::time::datetime::now().format("hms.n").c_str());
}

void reader()
{
    printf("%s reader thread::Get ....\n", cpp::time::datetime::now().format("hms.n").c_str());
    {
        pagedb::DB* pdb = NULL;
        pagedb::DB::Open(pagedb::Options().set_block_size(1 * 1024).set_read_only(true),
                         "pagedb_thread.pdb", 8, 4, &pdb);

        int found = 0;
        do {
            found = 0;
            pagedb::Iterator* iterator = pdb->NewIterator();
            iterator->Seek(pagedb::Slice("0000000", 7));
            for(; iterator->Valid(); iterator->Next()) {
                if(memcmp(iterator->key().data(), "0000000", 7) > 0)
                    break;
                char key[64] = {0};
                strncpy(key, iterator->key().data(), 8);
                int  val = *(int* )iterator->val().data();

                char exp_key[64];
                sprintf(exp_key, "0000000%d", found);
                int  exp_val = found;

                if(strncmp(exp_key, key, 8) == 0 && exp_val == val) {
                    printf("[done]: %s:%d\n", (char* )key, *(int* )iterator->val().data());
                }
                else {
                    printf("[fail]: %s:%d, expected: %s:%d\n",
                           (char* )key, *(int* )iterator->val().data(),
                           exp_key, exp_val
                           );
                }
                ++found;
            }
            delete iterator;
        } while(found != 10);
        delete pdb;
    }

    {
        pagedb::DB* pdb = NULL;
        pagedb::DB::Open(pagedb::Options().set_block_size(1 * 1024).set_read_only(true),
                         "pagedb_thread.pdb", 8, 4, &pdb);

        int found = 0;
        do {
            found = 0;
            pagedb::Iterator* iterator = pdb->NewIterator();
            iterator->Seek(pagedb::Slice("0010000", 7));
            for(; iterator->Valid(); iterator->Next()) {
                if(memcmp(iterator->key().data(), "0010000", 7) > 0)
                    break;
                char key[64] = {0};
                strncpy(key, iterator->key().data(), 8);
                int  val = *(int* )iterator->val().data();

                char exp_key[64];
                sprintf(exp_key, "0010000%d", found);
                int  exp_val = 100000 + found;

                if(strncmp(exp_key, key, 8) == 0 && exp_val == val) {
                    printf("[done]: %s:%d\n", (char* )key, *(int* )iterator->val().data());
                }
                else {
                    printf("[fail]: %s:%d, expected: %s:%d\n",
                           (char* )key, *(int* )iterator->val().data(),
                           exp_key, exp_val
                           );
                }
                ++found;
            }
            delete iterator;
        } while(found != 10);
        delete pdb;
    }
    printf("%s reader thread::Get done\n", cpp::time::datetime::now().format("hms.n").c_str());
}

void thread_test()
{
    pagedb::DB::Destroy("pagedb_thread.pdb");

    cpp::sys::thread w(&writer);
    cpp::sys::this_thread::sleep(cpp::time::MilliSeconds(10));
    cpp::sys::thread r(&reader);
    w.join();
    r.join();
}

int main(int argc, char* argv[])
{
    pagedb::DB::Destroy("pagedb1.pdb");

    printf("============= sequence insert ===========\n");
    sequence_insert();
    printf("============= thread test ===============\n");
    thread_test();
//    printf("============= random insert ===========\n");
//    random_insert();
//    printf("============= append insert ===========\n");
//    append_insert();
//    printf("============= iterator ===========\n");
//    iterator();

//    printf("============= sequence insert performance ===========\n");
//    insert_performance("perf1.dat", 4 * 1024 * 1024);
//    printf("============= sequence select performance ===========\n");
//    select_performance("perf1.dat", 4 * 1024 * 1024);

//    printf("============= random insert performance ===========\n");
//    random_insert_performance("perf2.dat", 4 * 1024 * 1024);
//    printf("============= random select performance ===========\n");
//    select_performance("perf2.dat", 4 * 1024 * 1024);


    return 0;

}
