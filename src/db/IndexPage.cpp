#include <db/IndexPage.hpp>
#include <stdexcept>
#include <algorithm>
#include <cstring>
#include <iostream>

using namespace db;

IndexPage::IndexPage(Page &page) {
  // TODO pa2: implement

  // 1. this->capacity
  // keys: size * 4
  // children: (size + 1) * 8
  // size * 4 + (size + 1) * 8 < DEFAULT_PAGE_SIZE
  capacity = (db::DEFAULT_PAGE_SIZE - sizeof(size_t)) / (sizeof(size_t) + sizeof(int));

  // 2. this->header
  this->header = std::make_unique<IndexPageHeader>(IndexPageHeader({.size = 0}));

  // 3. this->keys
  // keys[size] will occupy page[0, size * 4)
  this->keys = reinterpret_cast<int *>(page.data());

  // 4. this->children
  // children[size+1] will occupy page[size * 4, capacity)
  this->children = reinterpret_cast<size_t *>(page.data() + header->size * sizeof(int));
}

bool IndexPage::insert(int key, size_t child) {
  // std::cout << "Inserting " << key << " & " << child << std::endl;
  const int keys_length = header->size * sizeof(int);
  const int children_length = header->size * sizeof(size_t);

  /* 1. Shift & Insert the child */
  // memmove(), or manually swap in the size_t[], either is OK.
  const size_t child_index = std::distance(children, std::lower_bound(children, children + header->size + 1, child));
  for (size_t i = header->size + 1; i > child_index; i--) {
    children[i] = children[i-1]; // Note that children is size_t*. It's not char*
  }
  // std::cout << "child_index: " << child_index << std::endl;
  children[child_index] = child;


  /* 2. Shift & Insert the key */
  const size_t key_index = std::distance(keys, std::lower_bound(keys, keys + header->size, key));
  std::memmove(
    keys + key_index + 1,
    keys + key_index,
    (header->size - key_index) * sizeof(int) + (header->size + 1 + 1) * sizeof(size_t)
  );
  // Why (header->size + 1 + 1):
  // (header->size + 1) is children.size() before insertion. (header->size + 1 + 1) is after insertion.

  // std::cout << "key_index: " << key_index << std::endl;
  keys[key_index] = key;

  /* 3. Update header->size */
  header->size ++;

  /* Update pointers (keys unchanged) */
  // this->keys = reinterpret_cast<int *>(page.data());
  this->children = reinterpret_cast<size_t *>(keys + header->size);

  return header->size >= capacity;
}
int IndexPage::split(IndexPage &new_page) {
  // std::cout << "before split " << header->size <<  std::endl;
  /* 1. Calculate the expected size of this_page and new_page */
  const int keys_size_1 = (header->size - 1 + 1) / 2; // [-1]: propagate 1 key to the parent. [+1]: Make sure this.size() >= new.size()
  int key_to_propagate = this->keys[keys_size_1]; // 1
  const int keys_size_2 = header->size - 1 - keys_size_1;

  const int children_size_1 = (header->size + 1 + 1) / 2; // [+1]: children.size() == header->size + 1. [+1]: Make sure this.size() >= new.size()
  const int children_size_2 = header->size + 1 - children_size_1;


  /* 2. Move the children first. */
  std::memcpy(
    new_page.keys + keys_size_2, // Reserve some space for new_page.keys
    this->children + children_size_1,
    children_size_2 * sizeof(size_t)
  );

  /* 3. Move the keys */
  std::memcpy(
    new_page.keys,
    this->keys + keys_size_1 + 1,
    keys_size_2 * sizeof(int)
  );

  /* 4. Update the pointers (keys unchanged) */
  this->children = reinterpret_cast<size_t *>(this->keys + keys_size_1);
  new_page.children = reinterpret_cast<size_t *>(new_page.keys + keys_size_2);

  /* Update header->size, header->index_children */
  this->header->size = keys_size_1;
  // std::cout << "this header size: " << this->header->size << std::endl;
  new_page.header->size = keys_size_2;
  // std::cout << "new header size: " << new_page.header->size << std::endl;
  new_page.header->index_children = this->header->index_children;

  return key_to_propagate;
}
