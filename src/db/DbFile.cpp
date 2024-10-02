#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
  // TODO pa2: open file and initialize numPages
  // Hint: use open, fstat
  this->fd = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd == -1) {
    throw std::runtime_error("Could not open file");
  }

  struct stat fileStat{};
  if (fstat(fd, &fileStat) == -1) {
    throw std::runtime_error("Could not stat file");
  }

  long int file_size = fileStat.st_size; // size of the file, in bytes
  this->numPages = (file_size + DEFAULT_PAGE_SIZE - 1) / DEFAULT_PAGE_SIZE; // ceil (file_size / DEFAULT_PAGE_SIZE)
  // std::cout << "numPages: " << numPages << std::endl;
}

DbFile::~DbFile() {
  // TODO pa2: close file
  // Hind: use close
  close(fd);
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
  reads.push_back(id);
  // TODO pa2: read page
  // Hint: use pread
  ssize_t res = pread(this->fd, &page, DEFAULT_PAGE_SIZE, id * DEFAULT_PAGE_SIZE);
  if (res == -1) {
    // std::cout  << " " << id << std::endl;
    throw std::runtime_error("Could not read page");
  }
}

void DbFile::writePage(const Page &page, const size_t id) const {
  writes.push_back(id);
  // TODO pa2: write page
  // Hint: use pwrite
  ssize_t res = pwrite(this->fd, &page, DEFAULT_PAGE_SIZE, id * DEFAULT_PAGE_SIZE);
  if (res == -1) {
    throw std::runtime_error("Could not write page");
  }
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }
