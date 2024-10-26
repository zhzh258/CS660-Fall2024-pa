#include <db/LeafPage.hpp>
#include <stdexcept>
#include <memory>
#include <cstring>
#include <iostream>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index) : td(td), key_index(key_index) {
  // this->capacity
  this->capacity = db::DEFAULT_PAGE_SIZE / td.length(); // How many tuples can be stored in page[]?  page.size() / td.length()

  // this->header
  this->header = std::make_unique<LeafPageHeader>(LeafPageHeader{.size = 0});

  // this->data
  this->data = page.data(); // A reference to page[], where data is stored
}

bool LeafPage::insertTuple(const Tuple &t) {


  /* 1. Get tuple.key */
  if (!std::holds_alternative<int>(t.get_field(this->key_index))) {
    throw std::logic_error("tuple[key_index] does not holds an integer! Failed to find tuple.key");
  }
  const int t_key = std::get<int>(t.get_field(this->key_index)); // A key of the tuple. like 200, 250, 300, 400 in (img/split_leaf.svg)

  /* 2. Where to insert? */
  // For simplicity, I am using linear search O(n) now. TODO: replace it with binary search O(log n) 
  int insert_at = 0;
  bool replace_flag = false;
  for (int i = 0; i < header->size; i++, insert_at++) {
    Tuple ti = td.deserialize(data + i * td.length()); // Read the i-th tuple
    if (!std::holds_alternative<int>(ti.get_field(key_index))) {
      throw std::logic_error("tuple[key_index] does not holds an integer! Failed to find tuple.key");
    }
    const int ti_key = std::get<int>(ti.get_field(key_index));
    if (t_key <= ti_key) {
      replace_flag = t_key == ti_key;
      break;
    }
  }
  if (insert_at > this->capacity) { // This should not happen
    throw std::out_of_range("insert_at out of range!!!"); // This should not happen
  }

  /* 3. Insert */
  if (insert_at == header->size) { // case 1. Append
    // std::cout << "Append" << std::endl;
    td.serialize(data + insert_at * td.length(), t);
  } else if (replace_flag == true) { // case 2. Replace
    // std::cout << "Replace" << std::endl;
    td.serialize(data + insert_at * td.length(), t);
  } else if (replace_flag == false) { // case 3. Shift & Insert
    // NOTE: Can NOT use std::memcpy here due to overlapping memory.
    // std::cout << "Shift & Insert" << std::endl;
    std::memmove(
      data + (insert_at + 1) * td.length(),
      data + insert_at * td.length(),
      (header->size - insert_at) * td.length()
    );
    td.serialize(data + insert_at * td.length(), t);
  }

  /* 4. Update header */
  if (!replace_flag) this->header->size ++;
  // std::cout << "The size after: " << this->header->size << std::endl;
  return this->header->size == this->capacity; // If after size ++ the LeafPage is full, return true.
}

int LeafPage::split(LeafPage &new_page) {
  // TODO: If this->header->size is odd, how to split????? Which page should be bigger, this or new_page?
  /* Calculate the expected size */
  const int size_1 = this->header->size / 2; // Size the this_page should have
  const int size_2 = this->header->size - size_1; // Size the new_page should have

  /* Move data */
  std::memcpy(new_page.data, this->data + size_1 * td.length(), size_2 * td.length()); // Move size_2 tuples from this to new_page
  this->header->size = size_1;
  new_page.header->size = size_2;

  /* Set up new_page.header->next_leaf */
  new_page.header->next_leaf = this->header->next_leaf;

  /* Find and return the 1st key in new_page */
  Tuple t = td.deserialize(new_page.data + 0); // Now t is the first tuple in new_page
  if (!std::holds_alternative<int>(t.get_field(this->key_index))) {
    throw std::logic_error("tuple[key_index] does not holds an integer! Failed to find tuple.key");
  }
  return std::get<int>(t.get_field(this->key_index));
}

Tuple LeafPage::getTuple(size_t slot) const {
  return td.deserialize(this->data + slot * td.length());
}
