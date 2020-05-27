// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "aes_gcm.h"

#define KEY_SIZE GCM_256_KEY_LEN

namespace leveldb {


Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    uint8_t key[KEY_SIZE];

    //20200407 key generator
    srand (time(NULL));

          for (int i=0; i<KEY_SIZE; i++)
          {
       	   char t,c;
       	   t = rand() % 26;   // generate a random number
       	   c = 'a' + t;
                key[i]=c;            // Convert to a character from a-z
          }

    memcpy(meta->key,key,KEY_SIZE);



    TableBuilder* builder = new TableBuilder(options, file, meta->key);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {

      meta->file_size = builder->FileSize();

      //memcpy(meta->key,builder->key,KEY_SIZE); //2020-03-11
      assert(meta->file_size > 0);
    }

   /* printf("readkey:");
        for(int i=0; i<KEY_SIZE; i++)
      	  printf("%uc",meta->key[i]);*/
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable


      /*Iterator* it;

      //memcpy(it->ekey,meta->key,KEY_SIZE);
      it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;*/
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
