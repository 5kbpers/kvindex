#pragma once
#include <string>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <shared_ptr>
#include <unorded_map>
#include "lrucache.hpp"

const int hNumShardBits = 4;
const int hNumShards = 1 << hNumShardBits;
const int hPageSize = 8192;
const int hNumPageNodes = (hPageSize - 128) / 128;

struct KVData {
  std::string key;
  std::string value;
}

struct IndexNode {
  uint64_t hash;
  uint64_t offset; // offset of data file
}

struct IndexPage {
  uint32_t bits;
  uint32_t number; // page number of index file
  uint64_t num; // amount of nodes
  IndexNode nodes[hNumPageNodes];
}

class HashIndex {
 public:
  HashIndex(int fd);

  std::string GetValue(const std::string &key);
  void SetOffset(const std::string &key, uint64_t offset);
 private:
  KVData LoadData(uint64_t offset);
  std::shared_ptr<IndexPage> LoadIndex(uint64_t offset);

  uint64_t Hash(const std::string &key);
  uint32_t IndexNumber(uint64_t hash) { return hash >> (64 - bits_); }

  void Resize(uint32_t number);
  void SplitPage(uint32_t number, std::shared_ptr<IndexPage>& page_ptr);
  void PageFlush(const uint32_t& number, std::shared_ptr<IndexPage>& data);
  
  uint32_t bits_;
  int index_fd_;
  int data_fd_;
  uint32_t* table_;
  ShardedLRUCache<uint32_t, IndexPage> lru_;
  uint32_t next_page_;
  std::shared_mutex mutex_;
};

class ShardedHashIndex {
 public:
  std::string GetValue(const std::string &key) {
    const size_t h = std::hash<std::string>{}(key);
    return shard_[Shard(h)].GetValue(key);
  }
  void SetOffset(const std::string &key, uint64_t offset) {
    const size_t h = std::hash<std::string>{}(key);
    return shard_[Shard(h)].SetOffset(key, offset);
  }
 private:
  HashIndex shard_[hNumShards];

  size_t Shard(size_t h) {
    return h >> (sizeof(size_t) * 8 - hNumShardBits);
  }
};
