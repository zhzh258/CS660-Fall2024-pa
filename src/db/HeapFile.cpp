#include <iostream>
#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  // std::cout << "insertTuple. The current numPage is " << this->numPages <<  std::endl;
  if (this->numPages == 0) { // If no pages exist
    // std::cout << "Inserting tuple... case A" << std::endl;
    this->numPages ++;
    std::unique_ptr<Page> np = std::make_unique<Page>(); // np is a new empty page
    HeapPage nhp(*np, this->td); // nhp is the heap wrapper of the new empty page
    nhp.insertTuple(t);
    this->writePage(*np, 0);
  } else { // At least 1 page
    std::unique_ptr<Page> p = std::make_unique<Page>();
    this->readPage(*p, this->numPages - 1); // assign the last page to p
    HeapPage hp(*p, this->td);
    if (hp.insertTuple(t) == true) { // successfully inserted into the last page
      // std::cout << "Inserting tuple... case B" << std::endl;
      this->writePage(*p, this->numPages - 1);
      return;
    } else { // The last page is full
      // std::cout << "Inserting tuple... case C" << std::endl;
      this->numPages ++;
      std::unique_ptr<Page> np = std::make_unique<Page>(); // np is a new empty page
      HeapPage nhp(*np, this->td); // nhp is the heap wrapper of the new empty page
      if (nhp.insertTuple(t) == false) {
        throw std::runtime_error("HeapFile::insertTuple() failed when the last page is full");
      }
      this->writePage(*np, this->numPages - 1);
    }
  }
}

void HeapFile::deleteTuple(const Iterator &it) {
  // TODO pa2: implement
  // std::cout << "deleteTuple. numPage is " << this->numPages << "it is " << it.page << " ~ " << it.slot << std::endl;
  std::unique_ptr<Page> p = std::make_unique<Page>();
  this->readPage(*p, it.page);
  HeapPage hp(*p, this->td);
  hp.deleteTuple(it.slot);
  this->writePage(*p, it.page);
}

Tuple HeapFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement
  // std::cout << "getTuple. numPage is " << this->numPages << "it is " << it.page << " ~ " << it.slot << std::endl;
  std::unique_ptr<Page> p = std::make_unique<Page>();
  this->readPage(*p, it.page);
  HeapPage hp(*p, this->td);
  return hp.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
  // TODO pa2: implement
  // std::cout << "next()" << std::endl;
  std::unique_ptr<Page> p = std::make_unique<Page>();
  this->readPage(*p, it.page);
  HeapPage hp(*p, this->td);
  hp.next(it.slot);
  // If the current page has no occupied slot left, do the following
  // std::cout << "while... page: " << it.page << " slot: " << it.slot << std::endl;
  while (it.slot == hp.end() && it.page < this->numPages) { // 'it' is currently at the end of a page.
    // Looking for occupied slot on the next page, starting with (page+1) ~ (0)
    it.page ++;
    if (it.page == this->numPages) { // it.page reaches numPage. !!! Note that page with index of numPage does NOT exist
      it.slot = 0;
      return;
    }
    std::unique_ptr<Page> np = std::make_unique<Page>();
    this->readPage(*np, it.page);
    HeapPage nhp(*np, this->td);
    it.slot = nhp.begin();
    // std::cout << "iterating... page: " << it.page << " slot: " << it.slot << std::endl;
  }
}

Iterator HeapFile::begin() const {
  // TODO pa2: implement

  for (size_t page = 0; page < this->numPages; page++) {
    std::unique_ptr<Page> p = std::make_unique<Page>();
    this->readPage(*p, page);
    HeapPage hp(*p, this->td);
    if (hp.begin() == hp.end()) { // if the page is empty
      continue;
    } else { // A non-empty page found
      return {*this, page, hp.begin()};
    }
  }
  // All pages are empty
  return {*this, numPages, 0};
}

Iterator HeapFile::end() const {
  // TODO pa2: implement
  return {*this, numPages, 0};
}
