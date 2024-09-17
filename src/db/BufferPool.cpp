#include <db/BufferPool.hpp>
#include <db/Database.hpp>
#include <numeric>
#include <iostream>
#include <cassert>
#include <vector>
#include <unordered_set>

using namespace db;

BufferPool::BufferPool()
// TODO pa1: add initializations if needed
{
  // TODO pa1: additional initialization if needed
}

BufferPool::~BufferPool() {
  // TODO pa1: flush any remaining dirty pages
  std::vector<PageId> dirty_pages({});
  for (auto pair : lru_is_dirty) {
    if (pair.second == true) continue; // Skip if clean
    dirty_pages.push_back(pair.first);
  }

  for (const PageId& pid : dirty_pages) {
    flushPage(pid);
  }
}
/// In my solution, lru_queue, lru_page_mapping, lru_hashing, lru_is_dirty is designed to always have the same size
Page &BufferPool::getPage(const PageId &pid) {
  // TODO pa1: If already in buffer pool, make it the most recent page and return it

  // TODO pa1: If there are no available pages, evict the least recently used page. If it is dirty, flush it to disk

  // TODO pa1: Read the page from disk to one of the available slots, make it the most recent page

  if (this->contains(pid)) {                      // 1. If already in buffer pool
//    std::cout << "case 1" << std::endl;
    std::list<PageId>::iterator& iter = lru_hashing.at(pid);
    // Make it the most recent page
    lru_queue.erase(iter);
    lru_queue.push_back(pid);
    lru_hashing[pid] = std::prev(lru_queue.end());
    assert(lru_queue.size() == lru_hashing.size() && lru_hashing.size() == lru_is_dirty.size() && lru_is_dirty.size() == lru_page_mapping.size());
    return *(lru_page_mapping[pid]);
  } else if (lru_hashing.size() < DEFAULT_NUM_PAGES) {  // 2. If not in buffer pool && buffer pool not full
//    std::cout << "case 2" << std::endl;
    // Add the pid to the LRU-cache
    lru_queue.push_back(pid);
    lru_hashing[pid] = std::prev(lru_queue.end());
    lru_page_mapping[pid] = std::make_unique<Page>(); // Using smart pointer here to avoid memory leak. Note that make_unique returns a rvalue
    lru_is_dirty[pid] = false;

    // Fetch pid from the disk. (Is it possible that the file doesn't exist in the catalog???)
    db:DbFile& file = db::getDatabase().get(pid.file);
    file.readPage(*(lru_page_mapping[pid]), pid.page); // fetch data from the disk
    assert(lru_queue.size() == lru_hashing.size() && lru_hashing.size() == lru_is_dirty.size() && lru_is_dirty.size() == lru_page_mapping.size());
    return *(lru_page_mapping[pid]);
  } else {                                               // 3. If not in buffer pool && buffer pool full
//    std::cout << "case 3" << std::endl;
    // Evict the least recently used page.
    const PageId& page_to_evict = lru_queue.front(); // The front is the least recently used
    if (this->isDirty(page_to_evict)) {
      this->flushPage(page_to_evict);
    }
    this->discardPage(page_to_evict);

    // Add the pid to the LRU-cache
    lru_queue.push_back(pid);
    lru_hashing[pid] = std::prev(lru_queue.end());
    lru_is_dirty[pid] = false;
    lru_page_mapping[pid] = std::make_unique<Page>(); // Using smart pointer here to avoid memory leak

    // Fetch pid from the disk. (Is it possible that the file doesn't exist in the catalog???)
    db::DbFile& file = db::getDatabase().get(pid.file);
    file.readPage(*(lru_page_mapping[pid]), pid.page); // fetch data from the disk
    assert(lru_queue.size() == lru_hashing.size() && lru_hashing.size() == lru_is_dirty.size() && lru_is_dirty.size() == lru_page_mapping.size());
    return *(lru_page_mapping[pid]);
  }
}

void BufferPool::markDirty(const PageId &pid) {
  // TODO pa1: Mark the page as dirty. Note that the page must already be in the buffer pool
  if (!this->contains(pid)) {
    throw std::logic_error("Page not found");
  }
  lru_is_dirty[pid] = true;
}

bool BufferPool::isDirty(const PageId &pid) const {
  // TODO pa1: Return whether the page is dirty. Note that the page must already be in the buffer pool
  if (!this->contains(pid)) {
    throw std::logic_error("BufferPool::isDirty - Page not found");
  }
  assert(lru_queue.size() == lru_hashing.size() && lru_hashing.size() == lru_is_dirty.size() && lru_is_dirty.size() == lru_page_mapping.size());
  return lru_is_dirty.at(pid) == true;
}

bool BufferPool::contains(const PageId &pid) const {
  // TODO pa1: Return whether the page is in the buffer pool
  if (!(lru_hashing.contains(pid) == lru_page_mapping.contains(pid) && lru_page_mapping.contains(pid) == lru_is_dirty.contains(pid))) {
    throw std::logic_error("BufferPool::contains - Inconsistency detected!");
  }
  assert(lru_queue.size() == lru_hashing.size() && lru_hashing.size() == lru_is_dirty.size() && lru_is_dirty.size() == lru_page_mapping.size());
  return lru_hashing.contains(pid);
}

void BufferPool::discardPage(const PageId &pid) {
  // TODO pa1: Discard the page from the buffer pool. Note that the page must already be in the buffer pool
  if (!this->contains(pid)) {
    throw std::logic_error("BufferPool::discardPage - Page not found");
  }
  // Remove from lru_queue, lru_hashing, lru_page_mapping, lru_is_dirty
  auto iter = lru_hashing.at(pid);
  lru_hashing.erase(pid);
  lru_queue.erase(iter);
  lru_page_mapping.erase(pid);
  lru_is_dirty.erase(pid);
  assert(lru_queue.size() == lru_hashing.size() && lru_hashing.size() == lru_is_dirty.size() && lru_is_dirty.size() == lru_page_mapping.size());
}

void BufferPool::flushPage(const PageId &pid) {
  // TODO pa1: Flush the page to disk. Note that the page must already be in the buffer pool
  if (!this->contains(pid)) {
    throw std::logic_error("BufferPool::flushPage - Page not found");
  }
  // If not dirty, no need to flush.
  // I didn't notice this until reading test case 8.
  if (lru_is_dirty[pid] == false) {
    return;
  }

  // Mark clean
  lru_is_dirty[pid] = false;

  // Write on the disk
  db:DbFile& file = db::getDatabase().get(pid.file);
  file.writePage(*(lru_page_mapping[pid]), pid.page);
  assert(lru_queue.size() == lru_hashing.size() && lru_hashing.size() == lru_is_dirty.size() && lru_is_dirty.size() == lru_page_mapping.size());
}

void BufferPool::flushFile(const std::string &file) {
  // TODO pa1: Flush all pages of the file to disk
  for (auto pair : lru_hashing) {
    const PageId& pid = pair.first;
    std::list<PageId>::iterator iter = pair.second;
    if (pid.file == file) { // If pid belongs to the file
      flushPage(pid);
    }
  }
  assert(lru_queue.size() == lru_hashing.size() && lru_hashing.size() == lru_is_dirty.size() && lru_is_dirty.size() == lru_page_mapping.size());
}
