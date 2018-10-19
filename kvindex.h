#include <string>
#include "lrucache.hpp"
#include "hashindex.h"

class KVIndex {
 public:
   KVIndex();
   
   void Load(const string& filename);
   std::string Get(const string& key);

 private:
   ShardedHashIndex index_;
   ShardedLRUCache cache_;
};
