#include <string>
#include <memory>
#include <unistd.h>
#include "threadpool.hpp"
#include "kvindex.h"

using namespace std;

void KVIndex::Load(const char* filename) {
  // init
  data_fd_ = open(filename, O_RDWR);
  index_ = new ShardedHashIndex(data_fd_);
  ThreadPool* pool = new ThreadPool(10);
  uint32_t key_len = 0;
  uint32_t value_len = 0;
  uint64_t offset = 0;
  int count = 0;
  // read data
  while (read(data_fd_, &key_len, sizeof(uint32_t)) == sizeof(uint32_t)) {
    // read key
    char key_buf[key_len];
    read(data_fd_, key_buf, key_len);
    // read value
    read(data_fd_, &value_len, sizeof(uint32_t));
    char value_buf[value_len];
    read(data_fd_, value_buf, value_len);

    pool->enqueue(&ShardedHashIndex::SetOffset, index_, string(key_buf, key_len), offset);
    offset += sizeof(key_len) + key_len + sizeof(value_len) + value_len;
  }
  delete pool;
}

string KVIndex::Get(const string &key) {
  auto node = cache_->Lookup(key);
  if (node == nullptr) {
    auto value_ptr = make_shared<string>(index_->GetValue(key));
    cache_->Insert(key, value_ptr, value_ptr->size());
    return *(value_ptr);
  }
  return *(node->data);
}
