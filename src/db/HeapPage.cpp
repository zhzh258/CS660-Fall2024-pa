#include <chrono>
#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
  // TODO pa2: initialize private members
  // NOTE: header and data should point to locations inside the page buffer. Do not allocate extra memory.

  const size_t P = page.size(); // The length of each page (in byte)
  const size_t T = td.length(); // The length of each tuple (in byte)
  this->capacity = (__CHAR_BIT__ * P) / (__CHAR_BIT__ * T + 1); // floor((8*p)/(8*t+1)) is the number of tuple to be stored in this page
  this->header = page.data();
  this->data = page.data() + (this->capacity + __CHAR_BIT__ - 1) / __CHAR_BIT__; // ceil(c/8) is the number of bytes of the header
}

size_t HeapPage::begin() const {
  // TODO pa2: implement
  return (this->capacity + __CHAR_BIT__ - 1) / __CHAR_BIT__;
}

size_t HeapPage::end() const {
  // TODO pa2: implement
  return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  for (size_t slot = 0; slot < this->capacity; slot++) {
    if (this->empty(slot)) {
      td.serialize(this->header + this->begin() + slot, t);
    }
  }
}

void HeapPage::deleteTuple(size_t slot) {
  // TODO pa2: implement
  // Mark free
  const size_t header_slot_index = (slot - 1) / 8;
  const size_t header_slot_offset = (slot - 1) - 8 * header_slot_index;
  header[header_slot_index] &= ~(1 << header_slot_offset); // mark the bit as '0'

}

Tuple HeapPage::getTuple(size_t slot) const {
  // TODO pa2: implement
  return this->td.deserialize(this->header + this->begin() + slot);
}

void HeapPage::next(size_t &slot) const {
  // TODO pa2: implement
  size_t header_slot_index = (slot - 1) / 8;
  size_t header_slot_offset = (slot - 1) - 8 * header_slot_index;
  for (size_t i = 0; i < this->capacity; i++) {
    slot ++;
    slot %= this->capacity;
    if (!this->empty(slot)) { // if occupied, break
      break;
    }
  }
  throw std::logic_error("HeapPage::next() - Cannot move slot to the next occupied because the entire page is empty.");
}

bool HeapPage::empty(size_t slot) const {
  // TODO pa2: implement
  // this->begin() is the number of bytes in header
  const size_t header_slot_index = (slot - 1) / 8;
  const size_t header_slot_offset = (slot - 1) - 8 * header_slot_index;
  return header[header_slot_index] & (1 << header_slot_offset);
}
