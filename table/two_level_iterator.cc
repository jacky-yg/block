// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
#include <iostream>
#include "aes_gcm.h"

#define KEY_SIZE GCM_256_KEY_LEN


namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);
typedef Iterator* (*MyBlockFunction)(void*, const ReadOptions&, const Slice&, uint8_t* key); //20200422

class TwoLevelIterator : public Iterator {
 public:

  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);
  TwoLevelIterator(Iterator* index_iter,
                   void* arg, const ReadOptions& options, MyBlockFunction my_block_function,uint8_t* key);



  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void MySeek(const Slice& target,uint8_t* key) override;
  void MySeekToFirst(uint8_t* key) override;
  void MySeekToLast(uint8_t* key) override;
  void Next() override;
  void Prev() override;



  void GetKey();

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SkipEmptyDataBlocksForward(uint8_t* key);
  void SkipEmptyDataBlocksBackward(uint8_t* key);
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();
  void InitDataBlock(uint8_t* key);
  void InitMyDataBlock();


  BlockFunction block_function_;
  MyBlockFunction my_block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_;  // May be nullptr
  uint8_t key_[KEY_SIZE];
  uint8_t* tkey_;
  const unsigned char *mkey_;


  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter, void* arg,
                                   const ReadOptions& options,
                                   MyBlockFunction my_block_function,uint8_t* key)
    : my_block_function_(my_block_function),
      arg_(arg),
      tkey_(key),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}


TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::GetKey(){
	memcpy(key_,tkey_,sizeof(key_));
}

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);

  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  //GetKey();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();

  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

//----------------------------------------20200429----------------------------------------------------
void TwoLevelIterator::MySeek(const Slice& target,uint8_t* key) {
  index_iter_.Seek(target);
  //tkey_=key;
  InitDataBlock(key);
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward(key);
}

void TwoLevelIterator::MySeekToFirst(uint8_t* key) {
  index_iter_.SeekToFirst();
  //tkey_=key;
  InitDataBlock(key);
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward(key);
}

void TwoLevelIterator::MySeekToLast(uint8_t* key) {
  index_iter_.SeekToLast();
  //tkey_=key;
  InitDataBlock(key);
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward(key);
}
void TwoLevelIterator::SkipEmptyDataBlocksForward(uint8_t* key) {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();

    InitDataBlock(key);
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward(uint8_t* key) {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {void InitDataBlock();
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();

    InitDataBlock(key);
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}
//--------------------------------------------------------------------------------------------

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();

    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {void InitDataBlock();
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();

    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  std::cout<<"\ncall initdatablock"<<std::endl;
  if(tkey_==NULL)
	  std::cout<<"null"<<std::endl;
  for(int i=0; i<KEY_SIZE; i++)
	std::cout<<*(tkey_+i);
  std::cout<<""<<std::endl;

  if (!index_iter_.Valid()) {

    SetDataIterator(nullptr);
  } else {

    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {

      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      if(tkey_==NULL)
    	  std::cout<<"null"<<std::endl;

      Iterator* iter = (*my_block_function_)(arg_, options_, handle, tkey_);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

void TwoLevelIterator::InitDataBlock(uint8_t* tkey) {
  std::cout<<"\ncall initdatablock"<<std::endl;
  if(tkey_==NULL)
	  std::cout<<"null"<<std::endl;
  for(int i=0; i<KEY_SIZE; i++)
	std::cout<<*(tkey_+i);
  std::cout<<""<<std::endl;

  if (!index_iter_.Valid()) {

    SetDataIterator(nullptr);
  } else {

    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {

      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      if(tkey_==NULL)
    	  std::cout<<"null"<<std::endl;
      tkey_ = tkey;
      Iterator* iter = (*my_block_function_)(arg_, options_, handle, tkey_);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

/*void TwoLevelIterator::InitMyDataBlock() {
  if (!index_iter_.Valid()) {

    SetDataIterator(nullptr);
  } else {

    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {

      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {

      Iterator* iter = (*my_block_function_)(arg_, options_, handle, key_);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}*/

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

Iterator* MyNewTwoLevelIterator(Iterator* index_iter, void* arg,
                              const ReadOptions& options,
                              MyBlockFunction my_block_function,uint8_t* key) {

	//typedef Iterator* (*MyBlockFunction)(void*, const ReadOptions&, const Slice&, uint8_t* key);

	  std::cout<<"\ncall twoleveliteraror"<<std::endl;
	  for(int i=0; i<KEY_SIZE; i++)
		  std::cout<<*(key+i);
	  std::cout<<""<<std::endl;
  return new TwoLevelIterator(index_iter, arg, options, my_block_function, key);
}




}  // namespace leveldb
