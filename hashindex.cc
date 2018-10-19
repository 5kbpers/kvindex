#include <string>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <memory>
#include <unistd.h>
#include <fcntl.h>
#include "hashindex.h"

using namespace std;

const size_t hCacheCapacity = 38400 * hPageSize; // 300mB

HashIndex::HashIndex(int fd, const char* filename) : data_fd_(fd), bits_(10), next_page_(1 << bits_) {
  using namespace std::placeholders;
  std::function<void(const uint32_t&, shared_ptr<IndexPage>)> f = std::bind(&HashIndex::PageFlush, this, _1, _2);
  lru_ = new ShardedLRUCache<uint32_t, IndexPage>(hCacheCapacity, f);

  index_fd_ = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
  table_ = new uint32_t[1 << bits_];
  IndexPage page;
  for (int i = 0; i < 1 << bits_; i++) {
    page.bits = 10;
    table_[i] = i;
    page.number = i;
    write(index_fd_, &page, sizeof(IndexPage));
  }
  lseek(index_fd_, sizeof(IndexPage), SEEK_SET);
  read(index_fd_, &page, sizeof(IndexPage));
}

/*uint64_t HashIndex::Hash(const string &key) {
  uint64_t seed = 13131313;
  uint64_t hash = 0;
  for (auto c : key) {
    hash = hash * seed + c;
  }
  return hash;
}*/

uint64_t HashIndex::Hash(const string &k)
{
	const uint32_t m = 0x5bd1e995;
	const int r = 24;
  uint32_t seed = 0xEE6B27EB;
  int len = k.size();
  const char* key = k.c_str();

	uint32_t h1 = seed ^ len;
	uint32_t h2 = 0;
	const uint32_t* data = (const uint32_t *)key;

	while(len >= 8) {
		uint32_t k1 = *data++;
		k1 *= m; k1 ^= k1 >> r; k1 *= m;
		h1 *= m; h1 ^= k1;
		len -= 4;

		uint32_t k2 = *data++;
		k2 *= m; k2 ^= k2 >> r; k2 *= m;
		h2 *= m; h2 ^= k2;
		len -= 4;
	}

	if (len >= 4) {
		uint32_t k1 = *data++;
		k1 *= m; k1 ^= k1 >> r; k1 *= m;
		h1 *= m; h1 ^= k1;
		len -= 4;
	}

	switch (len) {
    case 3: h2 ^= ((unsigned char*)data)[2] << 16;
    case 2: h2 ^= ((unsigned char*)data)[1] << 8;
    case 1: h2 ^= ((unsigned char*)data)[0]; h2 *= m;
	};
	h1 ^= h2 >> 18; h1 *= m;
	h2 ^= h1 >> 22; h2 *= m;
	h1 ^= h2 >> 17; h1 *= m;
	h2 ^= h1 >> 19; h2 *= m;

	uint64_t h = h1;
	h = (h << 32) | h2;
	return h;
}

void HashIndex::SetOffset(const string &key, uint64_t offset) {
  unique_lock<shared_mutex> lock(mutex_);

  IndexNode node;
  node.hash = Hash(key);
  node.offset = offset;
  uint32_t number = table_[IndexNumber(node.hash)];
  auto node_ptr = lru_->Lookup(number); // lookup LRU
  // if page is not in LRU, then loads it from file
  shared_ptr<IndexPage> page_ptr = (node_ptr != nullptr) ? node_ptr->data : LoadIndex(number);
  // if page is full, then resize table and try to insert item again;
  if (page_ptr->num >= hNumPageNodes) {
    if (page_ptr->bits >= bits_) {
      Resize(IndexNumber(node.hash), page_ptr);
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
  lru_->Insert(number, page_ptr, sizeof(IndexPage));
}

string HashIndex::GetValue(const string &key) {
  shared_lock<shared_mutex> lock(mutex_);

  uint64_t h = Hash(key);
  uint32_t number = table_[IndexNumber(h)];
  auto node_ptr = lru_->Lookup(number); // lookup LRU
  // if page is not in LRU, then loads it from file
  shared_ptr<IndexPage> page_ptr;
  if (node_ptr != nullptr) {
    page_ptr = node_ptr->data;
  } else {
    page_ptr = LoadIndex(number);
    lru_->Insert(number, page_ptr, sizeof(IndexPage));
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

void HashIndex::Resize(uint32_t index, shared_ptr<IndexPage> old_page) {
  bits_++;
  uint32_t* new_table = new uint32_t[1 << bits_]();
  for (int i = 0; i < 1 << bits_; i++) {
    new_table[i] = table_[i >> 1];
  }
  auto new_page1 = make_shared<IndexPage>();
  auto new_page2 = make_shared<IndexPage>();
  new_page1->num = new_page2->num = 0;
  new_page1->bits = new_page2->bits = bits_;
  new_page1->number = old_page->number;
  new_page2->number = next_page_++;
  for (int i = 0; i < old_page->num; i++) {
    if (IndexNumber(old_page->nodes[i].hash) == (index << 1)) {
      new_page1->nodes[(new_page1->num)++] = old_page->nodes[i];
    } else {
      new_page2->nodes[(new_page2->num)++] = old_page->nodes[i];
    }
  }
  // update table
  new_table[index << 1] = new_page1->number;
  new_table[(index << 1) + 1] = new_page2->number;
  // write to index file
  PageFlush(new_page1->number, new_page1);
  PageFlush(new_page2->number, new_page2);
  // update LRU
  lru_->Insert(new_page1->number, new_page1, sizeof(IndexPage));
  lru_->Insert(new_page2->number, new_page2, sizeof(IndexPage));
  // switch to new table
  delete[] table_;
  table_ = new_table;
}

void HashIndex::SplitPage(uint32_t index, shared_ptr<IndexPage> page_ptr) {
  uint32_t old_index = index >> (bits_ - page_ptr->bits);
  auto new_page1 = make_shared<IndexPage>();
  auto new_page2 = make_shared<IndexPage>();
  new_page1->num = new_page2->num = 0;
  new_page1->bits = new_page2->bits = page_ptr->bits + 1;
  new_page1->number = page_ptr->number;
  new_page2->number = next_page_++;
  // split page
  for (int i = 0; i < page_ptr->num; i++) {
    if ((page_ptr->nodes[i].hash >> (64 - page_ptr->bits - 1)) == (old_index << 1)) {
      new_page1->nodes[(new_page1->num)++] = page_ptr->nodes[i];
    } else {
      new_page2->nodes[(new_page2->num)++] = page_ptr->nodes[i];
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
  PageFlush(new_page1->number, new_page1);
  PageFlush(new_page2->number, new_page2);
  // update LRU
  lru_->Insert(new_page1->number, new_page1, sizeof(IndexPage));
  lru_->Insert(new_page2->number, new_page2, sizeof(IndexPage));
}

void HashIndex::PageFlush(const uint32_t& number, shared_ptr<IndexPage> data) {
  lseek(index_fd_, number * hPageSize, SEEK_SET);
  write(index_fd_, data.get(), sizeof(IndexPage));
}

KVData HashIndex::LoadData(uint64_t offset) {
  uint32_t key_len = 0;
  uint32_t value_len = 0;
  KVData ret;
  // read key
  lseek(data_fd_, offset, SEEK_SET);
  read(data_fd_, &key_len, sizeof(uint32_t));
  char key_buf[key_len];
  read(data_fd_, key_buf, key_len);
  ret.key = string(key_buf, key_len);
  // read value
  read(data_fd_, &value_len, sizeof(uint32_t));
  char value_buf[value_len];
  read(data_fd_, value_buf, value_len);
  ret.value = string(value_buf, value_len);
  return ret;
}

shared_ptr<IndexPage> HashIndex::LoadIndex(uint32_t number) {
  auto ret = make_shared<IndexPage>();
  uint64_t offset = static_cast<uint64_t>(number) * sizeof(IndexPage);
  lseek(index_fd_, offset, SEEK_SET);
  read(index_fd_, ret.get(), sizeof(IndexPage));
  return ret;
}
