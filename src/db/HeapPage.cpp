#include <chrono>
#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <iostream>
#include <stdexcept>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
  // TODO pa2: initialize private members
  // NOTE: header and data should point to locations inside the page buffer. Do not allocate extra memory.

  const size_t P = page.size(); // The length of each page (in byte)
  const size_t T = td.length(); // The length of each tuple (in byte)

  this->capacity = (__CHAR_BIT__ * P) / (__CHAR_BIT__ * T + 1); // floor((8*p)/(8*t+1)) is the number of tuple to be stored in this page
  this->header = page.data();
  this->data = page.data() + P - capacity * T;
  // THE FOLLOWING IS INCORRECT!!!!
  // this->data = page.data() + (this->capacity + __CHAR_BIT__ - 1) / __CHAR_BIT__; // ceil(c/8) is the number of bytes of the header <= INCORRECT
  // THE LENGTH OF HEADER IS DECIDED BY   P - capacity*T
  // NOT BY                               ceil(capacity/8)
  // i.e. put all unused space in the padding, rather than minimizing the padding using ceil(capacity/8)

  // std::cout << "P " << P << std::endl;
  // std::cout << "T " << T << std::endl;
  // std::cout << "capacity: " << this->capacity << std::endl;
  // std::cout << "0 ~ data: " << P - capacity * T << std::endl;
}

size_t HeapPage::begin() const {
  // TODO pa2: implement
  for (size_t slot = 0; slot < capacity; slot ++) {
    const size_t header_slot_index = slot / 8;
    const size_t header_slot_offset = slot - 8 * header_slot_index;
    if (header[header_slot_index] & (1 << (__CHAR_BIT__ - 1 - header_slot_offset))) { // If occupied (bit is '1')
      // std::cout << "The begin slot is " << slot << std::endl;
      return slot;
    }
  }
  return capacity; // Can't find any occupied slot. The page is empty.
}

size_t HeapPage::end() const {
  // TODO pa2: implement
  return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  bool isFull = true;
  for (size_t slot = 0; slot < this->capacity; slot++) {
    if (this->empty(slot)) {
      isFull = false;
      // serialize data
      td.serialize(&this->data[slot * td.length()], t);
      // Mark occupied in the header
      const size_t header_slot_index = slot / 8;
      const size_t header_slot_offset = slot - 8 * header_slot_index;
      header[header_slot_index] |= (1 << (__CHAR_BIT__ - 1 - header_slot_offset)); // mark the bit as '1'
    }
  }
  return !isFull;
}

void HeapPage::deleteTuple(size_t slot) {
  // TODO pa2: implement
  if (this->empty(slot)) {
    throw std::invalid_argument("HeapPage::deleteTuple() - Cannot delete an empty slot!");
  }

  // Mark free
  const size_t header_slot_index = slot / 8;
  const size_t header_slot_offset = slot - 8 * header_slot_index;
  header[header_slot_index] &= ~(1 << (__CHAR_BIT__ - 1 - header_slot_offset)); // mark the bit as '0'

}

Tuple HeapPage::getTuple(size_t slot) const {
  // TODO pa2: implement
  // std::cout << "getTuple(" << slot << ")" << std::endl;
  if (this->empty(slot)) {
    throw std::invalid_argument("HeapPage::getTuple() - Cannot get Tuple from an empty slot!");
  }
  return this->td.deserialize(&this->data[slot * td.length()]);
}

void HeapPage::next(size_t &slot) const {
  // TODO pa2: implement
  while(slot < this->end()) {
    slot ++;
    if (!empty(slot)) {
      break;
    }
  }
}

bool HeapPage::empty(size_t slot) const {
  // TODO pa2: implement
  // this->begin() is the number of bytes in header
  const size_t header_slot_index = slot / 8;
  const size_t header_slot_offset = slot - 8 * header_slot_index;
  // std::cout << "header_slot_index " << header_slot_index << std::endl;
  // std::cout << "header_slot_offset " << header_slot_offset << std::endl;
  const bool bit_is_1 = header[header_slot_index] & (1 << (__CHAR_BIT__ - 1 - header_slot_offset));
  return !bit_is_1;
}
