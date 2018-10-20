# kvindex
## example
```c++
KVIndex index;
index.Load(filename);
std::string value = index.Get(key);
```
## 实现说明
### 总体结构
可扩展哈希表 + lru cache  
### 可扩展哈希表的实现
为了减少rehash的代价，因此采用了可扩展哈希表作为索引的数据结构。    
桶数组内存储了对应数据块的编号，数据库存储了各个键值对的哈希值以其在数据文件中的偏移量，结构如下：
```c++
struct IndexNode {
  uint64_t hash;
  uint64_t offset; // offset of data file
};

struct IndexPage {
  uint32_t bits;
  uint32_t number; // page number of index file
  uint64_t num; // current amount of nodes
  IndexNode nodes[hNumPageNodes];
};
```
每次查找数据时，首先需要读取对应的数据块，在数据块内遍历查找哈希值等于输入key的哈希值所对应的偏移量（IndexNode.offset），读取对应数据。对于哈希值存在冲突的情况，可将其对应的数据都读取出来，通过比较key的值来确定对应的value。
### 一些优化
1. 对索引的数据块进行了缓存
2. 将哈希表和lru cache进行shard，减少锁的粒度
3. 采用简单的线程池来处理读取数据时对哈希表的插入，加快初始化索引的速度
