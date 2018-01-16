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

#ifndef incl_tstarling_LRU_CACHE_H
#define incl_tstarling_LRU_CACHE_H

#include <mutex>
#include <thread>
#include <vector>
#include <rai/core.h>
#include <junction/ConcurrentMap_Grampa.h>

namespace tstarling {
/**
 * The LRU list node.
 *
 * We make a copy of the key in the list node, allowing us to find the
 * hash-table element from the list node.
 */
template <class TKey>
struct ListNode {
    ListNode()
            : m_prev(OutOfListMarker), m_next(nullptr)
    {}

    ListNode(const TKey& key)
            : m_key(key), m_prev(OutOfListMarker), m_next(nullptr)
    {}

    TKey m_key;
    ListNode<TKey>* m_prev;
    ListNode<TKey>* m_next;

    bool isInList() const {
      return m_prev != OutOfListMarker;
    }

    static ListNode<TKey>* const OutOfListMarker;

    void destroy() {
      Rai::Delete(this);
    }
};

/**
 * The value that we store in the hashtable. The list node is allocated from
 * an internal object_pool. The ListNode* is owned by the list.
 */
template <class TKey, class TValue>
struct HashMapValue {
    HashMapValue()
            : m_listNode(nullptr)
    {}

    HashMapValue(const TValue& value, ListNode<TKey>* node)
            : m_value(value), m_listNode(node)
    {}

    TValue m_value;
    ListNode<TKey>* m_listNode;

    void destroy() {
      Rai::Delete(this);
    }
};

/**
 * ThreadSafeLRUCache is a thread-safe LRU hashtable.
 */
template <class TKey, class TValue, class THashMap=junction::ConcurrentMap_Grampa<TKey, HashMapValue<TKey, TValue>*>, int NUM_INSERT_MUTEX = 256 >
class ThreadSafeLRUCache {

  typedef THashMap HashMap;
  static const TValue NullValue;
public:

  /**
   * Create a container with a given maximum size
   */
  explicit ThreadSafeLRUCache();

  ThreadSafeLRUCache(const ThreadSafeLRUCache& other) = delete;
  ThreadSafeLRUCache& operator=(const ThreadSafeLRUCache&) = delete;

  ~ThreadSafeLRUCache() {
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
  size_t size() const {
    return m_size.load();
  }

  /**
   * Evict the least-recently used item from the container. This function does
   * its own locking.
   */
  size_t evict();

  /**
   * Clear the container. NOT THREAD SAFE -- do not use while other threads
   * are accessing the container.
   */
  void clear();

private:

  /**
   * Unlink a node from the list. The caller must lock the list mutex while
   * this is called.
   */
  void delink(ListNode<TKey>* node);

  /**
   * Add a new node to the list in the most-recently used position. The caller
   * must lock the list mutex while this is called.
   */
  void pushFront(ListNode<TKey>* node);

  /**
   * This atomic variable is used to signal to all threads whether or not
   * eviction should be done on insert. It is approximately equal to the
   * number of elements in the container.
   */
  std::atomic<size_t> m_size;

  /** 
   * The underlying hash map.
   */
  HashMap m_map;

  /**
   * The linked list. The "head" is the most-recently used node, and the
   * "tail" is the least-recently used node. The list mutex must be held
   * during both read and write.
   */
  ListNode<TKey> m_head;
  ListNode<TKey> m_tail;
  typedef std::mutex ListMutex;
  ListMutex m_listMutex;
  typedef turf::Mutex InsertMutex;
  InsertMutex m_insertMutex[NUM_INSERT_MUTEX];
};

template <class TKey>
ListNode<TKey>* const
ListNode<TKey>::OutOfListMarker = (ListNode<TKey>*)-1;

template <class TKey, class TValue, class THashMap, int NUM_INSERT_MUTEX>
ThreadSafeLRUCache<TKey, TValue, THashMap, NUM_INSERT_MUTEX>::
ThreadSafeLRUCache() : m_size(0)
{
  m_head.m_prev = nullptr;
  m_head.m_next = &m_tail;
  m_tail.m_prev = &m_head;
}

template <class TKey, class TValue, class THashMap, int NUM_INSERT_MUTEX>
const TValue
ThreadSafeLRUCache<TKey, TValue, THashMap, NUM_INSERT_MUTEX>::NullValue = TValue(/*THashMap::ValueTraits::NullValue*/);

template <class TKey, class TValue, class THashMap, int NUM_INSERT_MUTEX>
TValue ThreadSafeLRUCache<TKey, TValue, THashMap, NUM_INSERT_MUTEX>::
get(const TKey& key) {
  auto res = m_map.get(key);
  if (res == nullptr) {
    return NullValue;
  }
  auto resListNode = res->m_listNode;
  auto resValue = res->m_value;

  // Acquire the lock, but don't block if it is already held
  std::unique_lock<ListMutex> lock(m_listMutex, std::try_to_lock);
  if (lock) {
    ListNode<TKey>* node = resListNode;
    // The list node may be out of the list if it is in the process of being
    // inserted or evicted. Doing this check allows us to lock the list for
    // shorter periods of time.
    if (node->isInList()) {
      delink(node);
      pushFront(node);
    }
    lock.unlock();
  }
  return resValue;
}

template <class TKey, class TValue, class THashMap, int NUM_INSERT_MUTEX>
bool ThreadSafeLRUCache<TKey, TValue, THashMap, NUM_INSERT_MUTEX>::
insert(const TKey& key, const TValue& value, size_t value_size) {
  // Insert into the CHM
  ListNode<TKey>* node = Rai::New<ListNode<TKey> >("new_Node", key);
  auto newValue = Rai::New<HashMapValue<TKey, TValue> >("shared_ptr_HashMapValue", value, node);

  auto oldValue = m_map.exchange(key, newValue);
  ListNode<TKey>* oldNode = nullptr;
  if (oldValue) {
    oldNode = oldValue->m_listNode;
    m_size.fetch_sub(oldValue->m_value->length());
    junction::DefaultQSBR.enqueue(&HashMapValue<TKey, TValue>::destroy, oldValue);
  }

  m_size.fetch_add(value_size);

  std::unique_lock<ListMutex> lock(m_listMutex);
  pushFront(node);
  if(oldNode != nullptr) {
    delink(oldNode);
    lock.unlock();

    junction::DefaultQSBR.enqueue(&ListNode<TKey>::destroy, oldNode);
    return false;
  } else {
    lock.unlock();
    return true;
  }
}

template <class TKey, class TValue, class THashMap, int NUM_INSERT_MUTEX>
void ThreadSafeLRUCache<TKey, TValue, THashMap, NUM_INSERT_MUTEX>::
clear() {
  ListNode<TKey>* node = m_head.m_next;
  ListNode<TKey>* next;
  while (node != &m_tail) {
    next = node->m_next;
    auto node_in_map = m_map.erase(node->m_key);
    if(node_in_map) {
      junction::DefaultQSBR.enqueue(&HashMapValue<TKey, TValue>::destroy, node_in_map);
    }
    junction::DefaultQSBR.enqueue(&ListNode<TKey>::destroy, node);
    node = next;
  }
  m_head.m_next = &m_tail;
  m_tail.m_prev = &m_head;
  m_size = 0;
}

template <class TKey, class TValue, class THashMap, int NUM_INSERT_MUTEX>
inline void ThreadSafeLRUCache<TKey, TValue, THashMap, NUM_INSERT_MUTEX>::
delink(ListNode<TKey>* node) {
  ListNode<TKey>* prev = node->m_prev;
  ListNode<TKey>* next = node->m_next;
  prev->m_next = next;
  next->m_prev = prev;
  node->m_prev = ListNode<TKey>::OutOfListMarker;
}

template <class TKey, class TValue, class THashMap, int NUM_INSERT_MUTEX>
inline void ThreadSafeLRUCache<TKey, TValue, THashMap, NUM_INSERT_MUTEX>::
pushFront(ListNode<TKey>* node) {
  ListNode<TKey>* oldRealHead = m_head.m_next;
  node->m_prev = &m_head;
  node->m_next = oldRealHead;
  oldRealHead->m_prev = node;
  m_head.m_next = node;
}

template <class TKey, class TValue, class THashMap, int NUM_INSERT_MUTEX>
size_t ThreadSafeLRUCache<TKey, TValue, THashMap, NUM_INSERT_MUTEX>::
evict() {
  std::unique_lock<ListMutex> lock(m_listMutex);
  ListNode<TKey>* moribund = m_tail.m_prev;
  if (moribund == &m_head) {
    // List is empty, can't evict
    return 0;
  }
  delink(moribund);
  lock.unlock();

  auto deleted_Res = m_map.erase(moribund->m_key);
  if (deleted_Res == nullptr) {
    // Presumably unreachable
    return 0;
  }

  size_t res_size = deleted_Res->m_value->length();

  junction::DefaultQSBR.enqueue(&HashMapValue<TKey, TValue>::destroy, deleted_Res);
  junction::DefaultQSBR.enqueue(&ListNode<TKey>::destroy, moribund);
  return res_size;
}

} // namespace tstarling

#endif
