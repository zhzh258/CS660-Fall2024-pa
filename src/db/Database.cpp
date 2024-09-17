#include <db/Database.hpp>

using namespace db;

BufferPool &Database::getBufferPool() { return bufferPool; }

Database &db::getDatabase() {
  static Database instance;
  return instance;
}

void Database::add(std::unique_ptr<DbFile> file) {
  // TODO pa1: add the file to the catalog. Note that the file must not exist.

  // pa1_solution
  if (this->catalog.contains(file->getName())) { // Error: File already exist.
    throw std::logic_error("File name '" + file->getName() + "' already exists!");
  }
  this->catalog[file->getName()] = std::move(file);

}

std::unique_ptr<DbFile> Database::remove(const std::string &name) {
  // TODO pa1: remove the file from the catalog. Note that the file must exist.

  // pa1_solution
  if (!this->catalog.contains(name)) { // Error: File does not exist.
    throw std::logic_error("File name '" + name + "' does not exists!");
  }

  this->getBufferPool().flushFile(name); // Flush all pages related to this file before removing it from the DB.

  std::unique_ptr<DbFile> ptr = std::move(catalog[name]);
  this->catalog.erase(name);
  return ptr;
}

DbFile &Database::get(const std::string &name) const {
  // TODO pa1: get the file from the catalog. Note that the file must exist.

  // pa1_solution
  if (!this->catalog.contains(name)) { // Error: File does not exist.
    throw std::logic_error("File name '" + name + "' does not exists!");
  }
  return *(this->catalog.at(name));
}
