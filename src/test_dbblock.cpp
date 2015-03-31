/**
 * create: 2015-03-09
 * author: zigzed@gmail.com
 */
#include "db_block.hpp"
#include "db_file.hpp"
#include <string.h>
#include <stdlib.h>
#include <memory.h>
#include <stdio.h>

void test1()
{
    pagedb::db_file             dbf("test1.dat", 1 * 1024 * 1024, 3, sizeof(uint32_t), NULL, false);
    pagedb::db_block* block =   dbf.insert();

    const char* month[] = {
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    };

    for(int i = 0; i < 12; ++i) {
        block->save(month[i], &i);
    }
    block->sort();

    if(block->length() != 7 * 12 + block->hdrlen()) {
        printf("[fail]: length mismatched. expected: %zd, got: %zd\n",
               7 * 12 + block->hdrlen(),
               block->length());
        return;
    }

    {
        void* found_ptr = block->find("ABC", 3);
        if(found_ptr != NULL) {
            char        key[16] = {0};
            uint32_t    val = -1;

            memcpy(key, found_ptr, 3);
            memcpy(&val, (char* )found_ptr + 3, sizeof(val));
            printf("[fail]: found unexpected ABC: %s, %d\n",
                   key, val);
        }
    }
    {
        void* found_ptr = block->find("Apr", 3);
        if(found_ptr != NULL) {
            char        key[16] = {0};
            uint32_t    val = -1;

            memcpy(key, found_ptr, 3);
            memcpy(&val, (char* )found_ptr + 3, sizeof(val));
            printf("[done]: found expected Apr: %s, %d\n",
                   key, val);
        }
        else {
            printf("[fail]: unexpected missing: Apr\n");
        }
    }
    {
        void* found_ptr = block->find("Se", 2);
        if(found_ptr != NULL) {
            char        key[16] = {0};
            uint32_t    val = -1;

            memcpy(key, found_ptr, 3);
            memcpy(&val, (char* )found_ptr + 3, sizeof(val));
            printf("[done]: found expected Sep: %s, %d\n",
                   key, val);
        }
        else {
            printf("[fail]: unexpected missing: Sep\n");
        }
    }

    {
        pagedb::db_block_cursor* lower = block->lower_bound("J", 1);
        pagedb::db_block_cursor* upper = block->upper_bound("J", 1);
        if(lower->count(upper) != 3) {
            printf("[fail]: count mismatched. expected 3, got %zd\n", lower->count(upper));
        }
        int found = 0;
        for(; lower->valid() && !lower->equal(upper); lower->next()) {
            found++;
            const void* k = lower->key();
            const void* v = lower->val();
            char        key[16] = {0};
            uint32_t    val = -1;

            memcpy(key, k, 3);
            memcpy(&val, v, sizeof(val));
            printf("[done]: found: %d, %s, %d\n",
                   found, key, val);
        }
        if(found != 3) {
            printf("[fail]: found mismatched. expected 3, got %d\n", found);
        }
        delete lower;
        lower = NULL;
        delete upper;
        upper = NULL;
    }

    {
        pagedb::db_block_cursor* lower = block->lower_bound("J", 1);
        pagedb::db_block_cursor* upper = block->upper_bound("Jul", 3);
        if(lower->count(upper) != 2) {
            printf("[fail]: count mismatched. expected 2, got %zd\n", lower->count(upper));
        }
        int found = 0;
        for(; lower->valid() && !lower->equal(upper); lower->next()) {
            found++;
            const void* k = lower->key();
            const void* v = lower->val();
            char        key[16] = {0};
            uint32_t    val = -1;

            memcpy(key, k, 3);
            memcpy(&val, v, sizeof(val));
            printf("[done]: found: %d, %s, %d\n",
                   found, key, val);
        }
        if(found != 2) {
            printf("[fail]: found mismatched. expected 2, got %d\n", found);
        }
        delete lower;
        lower = NULL;
        delete upper;
        upper = NULL;
    }

    {
        pagedb::db_block_cursor* lower = block->lower_bound("S", 1);
        pagedb::db_block_cursor* upper = block->upper_bound("S", 1);
        if(lower->count(upper) != 1) {
            printf("[fail]: count mismatched. expected 1, got %zd\n", lower->count(upper));
        }
        int found = 0;
        for(; lower->valid() && !lower->equal(upper); lower->next()) {
            found++;
            const void* k = lower->key();
            const void* v = lower->val();
            char        key[16] = {0};
            uint32_t    val = -1;

            memcpy(key, k, 3);
            memcpy(&val, v, sizeof(val));
            printf("[done]: found: %d, %s, %d\n",
                   found, key, val);
        }
        if(found != 1) {
            printf("[fail]: found mismatched. expected 1, got %d\n", found);
        }
        delete lower;
        lower = NULL;
        delete upper;
        upper = NULL;
    }

    {
        pagedb::db_block_cursor* lower = block->lower_bound("X", 1);
        pagedb::db_block_cursor* upper = block->upper_bound("X", 1);
        if(lower->count(upper) != 0) {
            printf("[fail]: count mismatched. expected 0, got %zd\n", lower->count(upper));
        }
        int found = 0;
        for(; lower->valid() && !lower->equal(upper); lower->next()) {
            found++;
            const void* k = lower->key();
            const void* v = lower->val();
            char        key[16] = {0};
            uint32_t    val = -1;

            memcpy(key, k, 3);
            memcpy(&val, v, sizeof(val));
            printf("[done]: found: %d, %s, %d\n",
                   found, key, val);
        }
        if(found != 0) {
            printf("[fail]: found mismatched. expected 0, got %d\n", found);
        }
        delete lower;
        lower = NULL;
        delete upper;
        upper = NULL;
    }


    delete block;
}

int main(int argc, char* argv[])
{
    pagedb::db_file::remove("test1.dat");
    test1();

    return 0;
}
