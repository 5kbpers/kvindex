#pragma once
#include <mutex>
#include <shared_ptr>
#include <unordered_map>
#include <functional>
#include <list>
#include "handle.h"

const int kNumShardBits = 4;
const int kNumShards = 1 << kNumShardBits;

template <typename KeyType, typename DataType>
struct LRUNode {
  KeyType key;
  std::shared_ptr<DataType> data;
  size_t charge
};

template <typename KeyType, typename DataType>
class LRUCache {
 public:
  typedef std::shared_ptr<LRUNode<KeyType, DataType>> NodePtr;

  LRUCache() : deleter_(nullptr) {}
  explicit LRUCache(std::function<void(const KeyType&, std::shared_ptr<DataType>)> deleter) : deleter_(deleter) {}
  ~LRUCache();


  LRUCache(LRUCache &rhs) = delete;
  LRUCache operator=(LRUCache &rhs) = delete;

  NodePtr Lookup(const KeyType &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = table_.find(key);
    if (it == table_.end()) {
      return nullptr;
    }
    list_.remove(*it);
    list_.push_front(*it);
    return *it;
  }

  NodePtr Insert(const KeyType &key, const std::shared_ptr<DataType> &data, size_t charge) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto old_it = table.find(key);
    if (old_it != table.end()) {
      usage_ -= old_it->charge;
      table_.erase(key);
      list_.remove_if([&](NodePtr ptr){ return ptr->key == key; });
    }
    auto ptr = std::make_shared<NodePtr>();
    ptr->key = key;
    ptr->data = data;
    ptr->charge = charge;
    usage_ += charge;

    while (usage_ > capacity_ && list_.size() > 0) {
      NodePtr& tmp = list.back();
      usage_ -= tmp->charge;
      if (deleter_ != nullptr) {
        deleter_(tmp->key, tmp->data);
      }
      table_.erase(tmp->key);
      list_.pop_back();
    }
    list.push_front(ptr);
    table.insert({key, ptr});

    return ptr;
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    usage_ = 0;
    for (auto &l : list_) {
        deleter_(l->key, l->data);
    }
    table_.clear();
    list_.clear();
  }

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

 private:
  size_t capacity_;
  size_t usage_;
  std::function<void(const KeyType&, std::shared_ptr<DataType>)> deleter_;

  std::mutex mutex_;
  std::unordered_map<KeyType, NodePtr> table_;
  std::list<NodePtr> list_;
};

template <typename KeyType, typename DataType>
class ShardedLRUCache {
 public:
  typedef std::shared_ptr<LRUNode<KeyType, DataType>> NodePtr;

  explicit ShardedLRUCache(size_t capacity, std::function<void(const KeyType&, std::shared_ptr<DataType>)> deleter=nullptr) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int i = 0; i < kNumShards; i++) {
      shard_[i] = LRUCache(deleter);
      shard_[i].SetCapacity(per_shard);
    }
  }

  NodePtr Lookup(const KeyType &key) {
    const size_t h = std::hash<KeyType>{}(key);
    return reinterpret_cast<NodePtr>(shard_[Shard(h)].Lookup(key));
  }

  NodePtr Insert(const KeyType &key, const std::shared_ptr<DataType> &data, size_t charge) {
    const size_t h = std::hash<KeyType>{}(key);
    return reinterpret_cast<NodePtr>(shard_[Shard(h)].Insert(key, data, charge));
  }

  void Clear() {
    for (int i = 0; i < kNumShards; i++) {
      shard_[i].Clear();
    }
  }

 private:
  LRUCache<KeyType, DataType> shard_[kNumShards];

  size_t Shard(size_t h) {
    return h >> (sizeof(size_t) * 8 - kNumShardBits);
  }
};
