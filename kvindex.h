#include <string>
#include <fcntl.h>
#include "lrucache.hpp"
#include "hashindex.h"

const int kvCacheCapacity = 1024 * 1024 * 1024; // 1gb

class KVIndex {
 public:
   KVIndex() : cache_(new ShardedLRUCache<std::string, std::string>(kvCacheCapacity)), data_fd_(-1) {}
   
   void Load(const char* filename);
   std::string Get(const std::string& key);

 private:
   int data_fd_;
   ShardedHashIndex* index_;
   ShardedLRUCache<std::string, std::string>* cache_;
};
