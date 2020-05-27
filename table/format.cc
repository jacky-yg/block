// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "aes_gcm.h"
#include <iostream>
#include "table/two_level_iterator.h"
#include "leveldb/options.h"
#include "leveldb/comparator.h"

#define TXT_SIZE  8
#define AAD_SIZE 32
#define TAG_SIZE 16		/* Valid values are 16, 12, or 8 */
#define KEY_SIZE GCM_256_KEY_LEN
#define IV_SIZE  GCM_IV_DATA_LEN

std::string plaintext;
std::string i;
static int amounts;
namespace leveldb {




void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}


Status ReadTheBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kDataBlockTrailerSize];
  Slice contents;

  Status s = file->Read(handle.offset(), n + kDataBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kDataBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }
    size_t data_size;
    //const char* cdata = contents.data();
    memcpy(&data_size,contents.data() + n + 5,sizeof(size_t));
    //2019
    //std::cout<<"data_size:"<<data_size<<std::endl;

    //int data_size = a;
    //struct gcm_key_data* gkey = (struct gcm_key_data*)malloc(sizeof(struct gcm_key_data));
    //struct gcm_context_data* gctx = (struct gcm_context_data*)malloc(sizeof(struct gcm_context_data));
    struct gcm_key_data gkey;
    struct gcm_context_data gctx;

    uint8_t ct[data_size], pt[data_size];	// Cipher text and plain text
    //uint8_t *ct = new uint8_t[data_size];
    //uint8_t *pt = new uint8_t[data_size];
    uint8_t iv[IV_SIZE], aad[AAD_SIZE], key[KEY_SIZE];	// Key and authentication data
    uint8_t tag2[TAG_SIZE];	// Authentication tags for encode and decode
    memset(key, 0, KEY_SIZE);
    memset(iv, 0, IV_SIZE);
    memset(aad, 0, AAD_SIZE);

    memcpy(ct,contents.data(),data_size);
    //contents.remove_prefix(data_size);

    size_t re_size = n - data_size;
    //2019
    //std::cout<<"re_size:"<<re_size<<std::endl;


    aes_gcm_pre_256(key, &gkey);
    aes_gcm_dec_256(&gkey, &gctx, pt, ct, data_size, iv, aad, AAD_SIZE, tag2, TAG_SIZE);
    uint8_t info[re_size];

    //memcpy(&info,pt,data_size);
    memcpy(info,contents.data() + data_size, re_size);
    //contents.clear();
    plaintext.reserve(n);
    plaintext = std::string( reinterpret_cast<char const*>(pt),data_size);
    //std::string *string2 = (std::string *)malloc(n*sizeof())
    //std::cout<<p<<std::endl;
    i = std::string( reinterpret_cast<char const*>(info),re_size);
    //onst char* p = (const char *)info;
    //memcpy(const_cast<unsigned char *>(p),reinterpret_cast<char *>(pt),data_size);
    //memcpy(const_cast<unsigned char *>(p+data_size),reinterpret_cast<char *>(info),re_size);

    plaintext.append(i);

    //std::string *plaintext2 = new std::string(plaintext);
    //const char *str = (const char *)malloc(n*sizeof(char));

    //*str = p;
    //static std::string u = std::string(p);
    Slice plain(plaintext);
    //std::cout<<"plaintext:"<<plaintext<<std::endl;



    // Check the crc of the type and the block contents
    const char* data = plain.data();


  //const char* data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {

    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {

        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.

        delete[] buf;
        result->data = Slice(data, n);
        //std::cout<<"n:"<<n<<std::endl;
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } /*else {
        std::cout<<"cache"<<std::endl;
        result->data = Slice(data, n);
        result->heap_allocated = true;
        result->cachable = true;
      }*/

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }
  //delete(ct);
  //delete(pt);
  //free(gkey);
  //free(gctx);
  return Status::OK();
}

Status ReadTheInternalBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result,uint8_t* tkey) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kDataBlockTrailerSize];
  Slice contents;

  Status s = file->Read(handle.offset(), n + kDataBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kDataBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }
    size_t data_size;
    //const char* cdata = contents.data();
    memcpy(&data_size,contents.data() + n + 5,sizeof(size_t));
    //2019
    //std::cout<<"data_size:"<<data_size<<std::endl;

    //int data_size = a;
    //struct gcm_key_data* gkey = (struct gcm_key_data*)malloc(sizeof(struct gcm_key_data));
    //struct gcm_context_data* gctx = (struct gcm_context_data*)malloc(sizeof(struct gcm_context_data));
    struct gcm_key_data gkey;
    struct gcm_context_data gctx;

    uint8_t ct[data_size], pt[data_size];	// Cipher text and plain text
    //uint8_t *ct = new uint8_t[data_size];
    //uint8_t *pt = new uint8_t[data_size];
    uint8_t iv[IV_SIZE], aad[AAD_SIZE], key[KEY_SIZE];	// Key and authentication data
    uint8_t tag2[TAG_SIZE];	// Authentication tags for encode and decode
    //memset(key, 0, KEY_SIZE);
    memcpy(key,tkey,KEY_SIZE);
    memset(iv, 0, IV_SIZE);
    memset(aad, 0, AAD_SIZE);

    memcpy(ct,contents.data(),data_size);
    //contents.remove_prefix(data_size);

    size_t re_size = n - data_size;
    //2019
    //std::cout<<"re_size:"<<re_size<<std::endl;









    aes_gcm_pre_256(key, &gkey);
    aes_gcm_dec_256(&gkey, &gctx, pt, ct, data_size, iv, aad, AAD_SIZE, tag2, TAG_SIZE);
    uint8_t info[re_size];

    //memcpy(&info,pt,data_size);
    memcpy(info,contents.data() + data_size, re_size);
    //contents.clear();
    plaintext.reserve(n);
    plaintext = std::string( reinterpret_cast<char const*>(pt),data_size);
    //std::string *string2 = (std::string *)malloc(n*sizeof())
    //std::cout<<p<<std::endl;
    i = std::string( reinterpret_cast<char const*>(info),re_size);
    //onst char* p = (const char *)info;
    //memcpy(const_cast<unsigned char *>(p),reinterpret_cast<char *>(pt),data_size);
    //memcpy(const_cast<unsigned char *>(p+data_size),reinterpret_cast<char *>(info),re_size);

    plaintext.append(i);

    //std::string *plaintext2 = new std::string(plaintext);
    //const char *str = (const char *)malloc(n*sizeof(char));

    //*str = p;
    //static std::string u = std::string(p);
    Slice plain(plaintext);
    //std::cout<<"plaintext:"<<plaintext<<std::endl;



    // Check the crc of the type and the block contents
    const char* data = plain.data();


  //const char* data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {

    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {

        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.

        delete[] buf;
        result->data = Slice(data, n);
        //std::cout<<"n:"<<n<<std::endl;
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } /*else {
        std::cout<<"cache"<<std::endl;
        result->data = Slice(data, n);
        result->heap_allocated = true;
        result->cachable = true;
      }*/

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }
  //delete(ct);
  //delete(pt);
  //free(gkey);
  //free(gctx);
  return Status::OK();
}

Status ReadTheCompactBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result,uint8_t* tkey) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kDataBlockTrailerSize];
  Slice contents;

  Status s = file->Read(handle.offset(), n + kDataBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kDataBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }
    size_t data_size;
    //const char* cdata = contents.data();
    memcpy(&data_size,contents.data() + n + 5,sizeof(size_t));
    //2019
    //std::cout<<"data_size:"<<data_size<<std::endl;

    //int data_size = a;
    //struct gcm_key_data* gkey = (struct gcm_key_data*)malloc(sizeof(struct gcm_key_data));
    //struct gcm_context_data* gctx = (struct gcm_context_data*)malloc(sizeof(struct gcm_context_data));
    struct gcm_key_data gkey;
    struct gcm_context_data gctx;

    uint8_t ct[data_size], pt[data_size];	// Cipher text and plain text
    //uint8_t *ct = new uint8_t[data_size];
    //uint8_t *pt = new uint8_t[data_size];
    uint8_t iv[IV_SIZE], aad[AAD_SIZE], key[KEY_SIZE];	// Key and authentication data
    uint8_t tag2[TAG_SIZE];	// Authentication tags for encode and decode
    //memset(key, 0, KEY_SIZE);
    memcpy(key,tkey,KEY_SIZE);
    memset(iv, 0, IV_SIZE);
    memset(aad, 0, AAD_SIZE);

    memcpy(ct,contents.data(),data_size);
    //contents.remove_prefix(data_size);

    size_t re_size = n - data_size;
    //2019
    //std::cout<<"re_size:"<<re_size<<std::endl;









    aes_gcm_pre_256(key, &gkey);
    aes_gcm_dec_256(&gkey, &gctx, pt, ct, data_size, iv, aad, AAD_SIZE, tag2, TAG_SIZE);
    uint8_t info[re_size];

    //memcpy(&info,pt,data_size);
    memcpy(info,contents.data() + data_size, re_size);
    //contents.clear();
    plaintext.reserve(n);
    plaintext = std::string( reinterpret_cast<char const*>(pt),data_size);
    //std::string *string2 = (std::string *)malloc(n*sizeof())
    //std::cout<<p<<std::endl;
    i = std::string( reinterpret_cast<char const*>(info),re_size);
    //onst char* p = (const char *)info;
    //memcpy(const_cast<unsigned char *>(p),reinterpret_cast<char *>(pt),data_size);
    //memcpy(const_cast<unsigned char *>(p+data_size),reinterpret_cast<char *>(info),re_size);

    plaintext.append(i);

    //std::string *plaintext2 = new std::string(plaintext);
    //const char *str = (const char *)malloc(n*sizeof(char));

    //*str = p;
    //static std::string u = std::string(p);
    Slice plain(plaintext);
    //std::cout<<"plaintext:"<<plaintext<<std::endl;



    // Check the crc of the type and the block contents
    const char* data = plain.data();


  //const char* data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {

    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {

        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.

        delete[] buf;
        result->data = Slice(data, n);
        //std::cout<<"n:"<<n<<std::endl;
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } /*else {
        std::cout<<"cache"<<std::endl;
        result->data = Slice(data, n);
        result->heap_allocated = true;
        result->cachable = true;
      }*/

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }
  //delete(ct);
  //delete(pt);
  //free(gkey);
  //free(gctx);
  return Status::OK();
}

Status ReadIndexBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  const char* data = contents.data();  // Pointer to where Read put the data

  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {

        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}




Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kDataBlockTrailerSize];
  Slice contents;

  Status s = file->Read(handle.offset(), n + kDataBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kDataBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }
    size_t data_size;
    //const char* cdata = contents.data();
    memcpy(&data_size,contents.data() + n + 5,sizeof(size_t));
    //2019
    //std::cout<<"data_size:"<<data_size<<std::endl;

    //int data_size = a;
    //struct gcm_key_data* gkey = (struct gcm_key_data*)malloc(sizeof(struct gcm_key_data));
    //struct gcm_context_data* gctx = (struct gcm_context_data*)malloc(sizeof(struct gcm_context_data));
    struct gcm_key_data gkey;
    struct gcm_context_data gctx;

    uint8_t ct[data_size], pt[data_size];	// Cipher text and plain text
    //uint8_t *ct = new uint8_t[data_size];
    //uint8_t *pt = new uint8_t[data_size];
    uint8_t iv[IV_SIZE], aad[AAD_SIZE], key[KEY_SIZE];	// Key and authentication data
    uint8_t tag2[TAG_SIZE];	// Authentication tags for encode and decode
    memset(key, 0, KEY_SIZE);
    memset(iv, 0, IV_SIZE);
    memset(aad, 0, AAD_SIZE);

    memcpy(ct,contents.data(),data_size);
    //contents.remove_prefix(data_size);

    size_t re_size = n - data_size;
    //2019
    //std::cout<<"re_size:"<<re_size<<std::endl;









    aes_gcm_pre_256(key, &gkey);
    aes_gcm_dec_256(&gkey, &gctx, pt, ct, data_size, iv, aad, AAD_SIZE, tag2, TAG_SIZE);
    uint8_t info[re_size];

    //memcpy(&info,pt,data_size);
    memcpy(info,contents.data() + data_size, re_size);
    //contents.clear();
    plaintext.reserve(n);
    plaintext = std::string( reinterpret_cast<char const*>(pt),data_size);
    //std::string *string2 = (std::string *)malloc(n*sizeof())
    //std::cout<<p<<std::endl;
    i = std::string( reinterpret_cast<char const*>(info),re_size);
    //onst char* p = (const char *)info;
    //memcpy(const_cast<unsigned char *>(p),reinterpret_cast<char *>(pt),data_size);
    //memcpy(const_cast<unsigned char *>(p+data_size),reinterpret_cast<char *>(info),re_size);

    plaintext.append(i);

    std::string *plaintext2 = new std::string(plaintext);

    //const char *str = (const char *)malloc(n*sizeof(char));

    //*str = p;
    //static std::string u = std::string(p);
    Slice plain(*plaintext2);
    //std::cout<<"plaintext:"<<plaintext<<std::endl;



    // Check the crc of the type and the block contents
    const char* data = plain.data();


  //const char* data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {

    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {

        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.

        delete[] buf;
        result->data = Slice(data, n);
        //std::cout<<"n:"<<n<<std::endl;
        result->heap_allocated = true;
        result->cachable = false;  // Do not double-cache
      }/* else {
        std::cout<<"cache"<<std::endl;
        result->data = Slice(data, n);
        result->heap_allocated = true;
        result->cachable = true;
      }*/

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }
  //delete(ct);
  //delete(pt);
  //free(gkey);
  //free(gctx);

  return Status::OK();
}



}  // namespace leveldb
