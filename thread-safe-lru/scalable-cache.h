/*
 * Copyright (c) 2014 Tim Starling
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef incl_tstarling_SCALABLE_CACHE_H
#define incl_tstarling_SCALABLE_CACHE_H

#include "thread-safe-lru/lru-cache.h"
#include <rai/core.h>
#include <rai/collections/thread_context.h>
#include <thread>
#include <chrono>
#include <limits>
#include <memory>
#include <queue>

namespace tstarling {

/**
 * ThreadSafeScalableCache is a thread-safe sharded hashtable with limited
 * size. When it is full, it evicts a rough approximation to the least recently
 * used item.
 *
 * The find() operation fills a ConstAccessor object, which is a smart pointer
 * similar to TBB's const_accessor. After eviction, destruction of the value is
 * deferred until all ConstAccessor objects are destroyed.
 * 
 * Since the hash value of each key is requested multiple times, you should use
 * a key with a memoized hash function. ThreadSafeStringKey is provided for
 * this purpose.
 */
template <class TKey, class TValue, class THashMapMap=junction::ConcurrentMap_Grampa<TKey, HashMapValue<TKey, TValue>*> >
struct ThreadSafeScalableCache {
  using Shard = ThreadSafeLRUCache<TKey, TValue, THashMapMap>;

  /**
   * Constructor
   *   - maxSize: the maximum number of items in the container
   *   - numShards: the number of child containers. If this is zero, the
   *     "hardware concurrency" will be used (typically the logical processor
   *     count).
   */
  explicit ThreadSafeScalableCache(size_t maxSize, size_t gcIntervalMS = 100, size_t numShards = 0);

  ThreadSafeScalableCache(const ThreadSafeScalableCache&) = delete;
  ThreadSafeScalableCache& operator=(const ThreadSafeScalableCache&) = delete;

  ~ThreadSafeScalableCache() {
    m_isTerminated = true;
    m_gcThread.join();
    m_gcThreadContext.unregisterThread();
    clear();
  }

  /**
   * Find a value by key, and return it by filling the ConstAccessor, which
   * can be default-constructed. Returns true if the element was found, false
   * otherwise. Updates the eviction list, making the element the
   * most-recently used.
   */
  TValue get(const TKey& key);

  /**
   * Insert a value into the container. Both the key and value will be copied.
   * The new element will put into the eviction list as the most-recently
   * used.
   *
   * If there was already an element in the container with the same key, it
   * will not be updated, and false will be returned. Otherwise, true will be
   * returned.
   */
  bool insert(const TKey& key, const TValue& value, size_t value_size = 1);

  /**
   * Get the approximate size of the container. May be slightly too low when
   * insertion is in progress.
   */
  size_t size() const;

private:
  /**
   * Clear the container. NOT THREAD SAFE -- do not use while other threads
   * are accessing the container.
   */
  void clear();

  /**
   * Collects the garbage.
   */
  void gc();

  /**
   * Get the child container for a given key
   */
  Shard& getShard(const TKey& key);

  /**
   * The maximum size of elements in the container.
   */
  size_t m_maxSize;

  /**
   * The child containers
   */
  size_t m_numShards;
  typedef std::shared_ptr<Shard> ShardPtr;
  std::vector<ShardPtr> m_shards;

  /*
   * Garbage collector thread
   */
  std::thread m_gcThread;

  /*
   * Is the collection being destructed?
   */
  std::atomic<bool> m_isTerminated;

  std::chrono::milliseconds m_gcIntervalMS;

  Rai::Collections::ThreadContext m_gcThreadContext;
};

template <class TKey, class TValue, class THashMap>
void ThreadSafeScalableCache<TKey, TValue, THashMap>::
gc() {
  while(!m_isTerminated) {
    size_t size = 0;
    for (size_t i = 0; i < m_numShards; i++) {
      size += m_shards[i]->size();
    }

    if(size >= m_maxSize) {
      auto cmp = [](ShardPtr left, ShardPtr right) { return left->size() < right->size();};
      std::priority_queue<ShardPtr, std::vector<ShardPtr>, decltype(cmp)> q(cmp);
      for (size_t i = 0; i < m_numShards; i++) {
        q.push(m_shards[i]);
      }

      while(size >= m_maxSize && !q.empty()) {
        auto shard = q.top();
        q.pop();
        size_t deleted_size = shard->evict();
        if(deleted_size != 0) {
          q.push(shard);
        }

        size -= deleted_size;
      }
    }

    m_gcThreadContext.update();
    std::this_thread::sleep_for(m_gcIntervalMS);
  }
};

template <class TKey, class TValue, class THashMap>
ThreadSafeScalableCache<TKey, TValue, THashMap>::
ThreadSafeScalableCache(size_t maxSize, size_t gcIntervalMS, size_t numShards)
  : m_maxSize(maxSize), m_numShards(numShards), m_isTerminated(false), m_gcIntervalMS(gcIntervalMS)
{
  if (m_numShards == 0) {
    m_numShards = std::thread::hardware_concurrency() * 4;
  }
  for (size_t i = 0; i < m_numShards; i++) {
    m_shards.emplace_back(Rai::MakeShared<Shard>("new_Shard_in_ThreadSafeScalableCache"));
  }

  m_gcThreadContext.registerThread();

  m_gcThread = std::thread(&ThreadSafeScalableCache<TKey, TValue, THashMap>::gc, this);
}

template <class TKey, class TValue, class THashMap>
typename ThreadSafeScalableCache<TKey, TValue, THashMap>::Shard&
ThreadSafeScalableCache<TKey, TValue, THashMap>::
getShard(const TKey& key) {
  typename THashMap::KeyTraits hashObj;
  constexpr int shift = std::numeric_limits<size_t>::digits - 16;
  size_t h = (hashObj.hash(key) >> shift) % m_numShards;
  return *m_shards.at(h);
}

template <class TKey, class TValue, class THashMap>
TValue ThreadSafeScalableCache<TKey, TValue, THashMap>::
get(const TKey& key) {
  return getShard(key).get(key);
}

template <class TKey, class TValue, class THashMap>
bool ThreadSafeScalableCache<TKey, TValue, THashMap>::
insert(const TKey& key, const TValue& value, size_t value_size) {
  return getShard(key).insert(key, value, value_size);
}

template <class TKey, class TValue, class THashMap>
void ThreadSafeScalableCache<TKey, TValue, THashMap>::
clear() {
  for (size_t i = 0; i < m_numShards; i++) {
    m_shards[i]->clear();
  }
}

template <class TKey, class TValue, class THash>
size_t ThreadSafeScalableCache<TKey, TValue, THash>::
size() const {
  size_t size = 0;
  for (size_t i = 0; i < m_numShards; i++) {
    size += m_shards[i]->size();
  }
  return size;
}

} // namespace tstarling

#endif
