#include <string>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <shared_ptr>
#include <fcntl.h>
#include "hashindex.h"

using namespace std;

const hCacheCapacity = 38400 * hPageSize; // 300mB

HashIndex::HashIndex(int fd) : data_fd_(fd), bits_(10), next_page_(1 << bits_) {
  using namespace std::placeholders;
  lru_ = ShardedLRUCache<uint32_t, IndexPage>(hCacheCapacity, bind(PageFlush, this, _1, _2));
  index_fd_ = open("hash.idx", O_RDWR | O_CREAT);
  table = new uint32_t[1 << bits_];
  IndexPage page;
  page.bits = bits_;
  for (int i = 0; i < 1 << bits_; i++) {
    table[i] = i;
    page.number = i;
    write(index_fd_, &page, sizeof(IndexPage));
  }
}

uint64_t HashIndex::Hash(const string &key) {
  uint64_t seed = 13131;
  uint64_t hash = 0;
  for (auto c : key) {
    hash = hash * seed + c;
  }
  return hash;
}

void HashIndex::SetOffset(const string &key, uint64_t offset) {
  unique_lock<shared_mutex> lock(mutex_);

  IndexNode node;
  node.hash = Hash(key);
  node.offset = offset;
  uint32_t number = table_[IndexNumber(node.hash)];
  auto node_ptr = lru_.Lookup(number); // lookup LRU
  // if page is not in LRU, then loads it from file
  shared_ptr<IndexPage> page_ptr = (node_ptr != nullptr) ? node_ptr->data : LoadIndex(number);
  // if page is full, then resize table and try to insert item again;
  if (page_ptr->num >= hNumPageNodes) {
    if (page_ptr->bits >= bits_) {
      Resize(IndexNumber(node.hash));
      lock.unlock();
      SetOffset(key, offset);
      return;
    }
    SplitPage(IndexNumber(node.hash), page_ptr);
    lock.unlock();
    SetOffset(key, offset);
    return;
  }
  page_ptr->nodes[page_ptr->num] = node;
  (page_ptr->num)++;
  // update LRU
  lru_.Insert(number, page_ptr, sizeof(IndexPage));
}

string HashIndex::GetValue(const string &key) {
  shared_lock<shared_mutex> lock(mutex_);

  uint64_t h = Hash(key);
  uint32_t number = table_[IndexNumber(h)];
  auto node_ptr = lru_.Lookup(number); // lookup LRU
  // if page is not in LRU, then loads it from file
  shared_ptr<IndexPage> page_ptr;
  if (node_ptr != nullptr) {
    page_ptr = node_ptr->data;
  } else {
    page_ptr = LoadIndex(number);
    lru_.Insert(number, page_ptr, sizeof(IndexPage));
  }
  // lookup nodes, if node.hash equals to Hash(key), then load it from file
  for (int i = 0; i < page_ptr->num; i++) {
    if (page_ptr->nodes[i].hash == h) {
      auto data = std::move(LoadData(page_ptr->nodes[i].offset));
      // solve hash collision
      if (data.key == key) {
        return data.value;
      }
    }
  }
  return "";
}

void HashIndex::Resize(uint32_t index) {
  bits_++;
  uint32_t* new_table = new uint32_t[1 << bits_]();
  for (int i = 0; i < 1 << bits_; i++) {
    new_table[i] = table_[i >> 1];
  }
  auto old_page = LoadIndex(table[index]);
  IndexPage new_page1, new_page2;
  new_page1.bits = new_page2.bits = bits_;
  new_page1.number = old_page->number;
  new_page2.number = next_page_++;
  for (int i = 0; i < old_page->num; i++) {
    if (IndexNumber(old_page->nodes[i].hash) == (index << 1)) {
      new_page1.nodes[new_page1.num] = old_page->nodes[i];
    } else {
      new_page2.nodes[new_page1.num] = old_page->nodes[i];
    }
  }
  // write to index file
  PageFlush(new_page1.number, &new_page1);
  PageFlush(new_page2.number, &new_page2);
  new_table[(number << 1) + 1] = new_page2.number; // update new table
  lru_.Clear(); // clear LRU
  table_ = new_table; // switch to new table
  delete[] table_;
}

void HashIndex::SplitPage(uint32_t index, shared_ptr<IndexPage>& page_ptr) {
  uint32_t old_index = index >> page_ptr->bits;
  auto new_page1 = make_shared<IndexPage>();
  auto new_page2 = make_shared<IndexPage>();
  new_page1->bits = new_page2->bits = page_ptr->bits + 1;
  new_page1->number = old_page->number;
  new_page2->number = next_page_++;
  // split page
  for (int i = 0; i < page_ptr->num; i++) {
    if ((page_ptr->nodes[i].hash >> (64 - page_ptr->bits - 1)) == (old_index << 1)) {
      new_page1->nodes[new_page1.num] = page_ptr->nodes[i];
    } else {
      new_page2->nodes[new_page1.num] = page_ptr->nodes[i];
    }
  }
  // update table
  for (int i = 0; i < 1 << (bits_ - (page_ptr->bits + 1)); i++) {
    uint32_t base1 = (old_index << 1) << (bits_ - (page_ptr->bits + 1));
    uint32_t base2 = ((old_index << 1) + 1) << (bits_ - (page_ptr->bits + 1));
    table_[base1 + i] = new_page1->number;
    table_[base2 + i] = new_page2->number;
  }
  // write to index file
  PageFlush(number << 1, new_page1);
  PageFlush((number << 1) + 1, new_page2);
  // update LRU
  lru_.Insert(new_page1->number, new_page1, sizeof(IndexPage));
  lru_.Insert(new_page2->number, new_page2, sizeof(IndexPage));
}

void HashIndex::PageFlush(const uint32_t& number, shared_ptr<IndexPage>& data) {
  lseek(index_fd_, number * hPageSize, SEEK_SET);
  write(index_fd_, data.get(), sizeof(IndexPage));
}

KVData HashIndex::LoadData(uint64_t offset) {
}

shared_ptr<IndexPage> LoadIndex(uint32_t number) {
}
