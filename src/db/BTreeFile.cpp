#include <cstring>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
    : DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
  return;
  const int tk = std::get<int>(t.get_field(key_index));
  /* Find the correct leafPage to insert */
  BufferPool &bufferPool = getDatabase().getBufferPool();
  Page &p = bufferPool.getPage(PageId{name, 0}); // The root. It's always an IndexPage
  IndexPage node_page(p);
  while (node_page.header->index_children == true) { // repeat until node_page points to leaves
    int index; // keys[index] is the first key greater than tk. If all < tk, index = keys.size()
    for (index = 0; index < node_page.header->size; index ++) {
      if (node_page.keys[index] > tk) {
        break;
      }
    }
    // The child should be children[index]
    Page &np = bufferPool.getPage(PageId{name, node_page.children[index]});
    node_page = IndexPage{np};
  }
  // TODO: handle the edge case: an empty B+ tree with no leaf
  int index; // keys[index] is the first key greater than tk. If all < tk, index = keys.size()
  for (index = 0; index < node_page.header->size; index ++) {
    if (node_page.keys[index] > tk) {
      break;
    }
  }
  // The child should be children[index]
  Page &lp = bufferPool.getPage(PageId{name, node_page.children[index]});
  LeafPage leaf_page{lp, td, key_index}; // This is the leaf_page to insert

  if (leaf_page.insertTuple(t) == true) {
    const size_t some_page_id = 123456789;// TODO: What should be the id of the newly created new_page???
    // TODO: I think I need a numbering policy for pageId. e.g Keep a counter. Whenever a new_node is created, counter++
    Page &np = bufferPool.getPage(PageId{name, some_page_id}); // new page
    LeafPage new_page{np, td, key_index}; // This is the leaf_page to insert
    leaf_page.split(new_page);
    leaf_page.header->next_leaf = some_page_id; // link new_node to leaf_node

    // TODO: find its parent and recursively do parent.insertTuple(), until false or reaching the root
    // TODO: To backtrack the parent, probably need a stack/queue to store the id (in the previous code)
  }
}

void BTreeFile::deleteTuple(const Iterator &it) {
  // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
  BufferPool &bufferPool = getDatabase().getBufferPool();
  Page &p = bufferPool.getPage(PageId{name, it.page});

  LeafPage leaf_page(p, td, key_index); // The leafPage
  return leaf_page.getTuple(it.slot);
}

void BTreeFile::next(Iterator &it) const {
  BufferPool &bufferPool = getDatabase().getBufferPool();
  Page &p = bufferPool.getPage(PageId{name, it.page});

  LeafPage leaf_page(p, td, key_index); // The current LeafPage
  if (it.slot < leaf_page.header->size) { // next tuple
    it.slot ++;
  } else { // next leaf
    it.page = leaf_page.header->next_leaf;
  }
  // TODO: handle the edge case: an empty B+ tree with no leaf
}

Iterator BTreeFile::begin() const {
  BufferPool &bufferPool = getDatabase().getBufferPool();
  Page &p = bufferPool.getPage(PageId{name, 0}); // The root. It's always an IndexPage

  IndexPage node_page(p);
  while (node_page.header->index_children == true) { // repeat until node_page points to leaves
    Page &np = bufferPool.getPage(PageId{name, node_page.children[0]});
    node_page = IndexPage{np};
  }
  // left-most leaf id: node_page.children[0]
  return Iterator{*this, node_page.children[0], 0};
  // TODO: handle the edge case: an empty B+ tree with no leaf
}

Iterator BTreeFile::end() const {
  BufferPool &bufferPool = getDatabase().getBufferPool();
  Page &p = bufferPool.getPage(PageId{name, 0}); // The root. It's always an IndexPage

  IndexPage node_page(p);
  while (node_page.header->index_children == true) { // repeat until node_page points to leaves
    Page &np = bufferPool.getPage(PageId{name, node_page.children[node_page.header->size]});
    node_page = IndexPage{np};
  }
  // right-most leaf id: node_page.children[node_page.header->size]
  Page &rmp = bufferPool.getPage(PageId{name, node_page.children[node_page.header->size]});
  LeafPage leaf_page(rmp, td, key_index); // This is the right-most leafPage
  return Iterator{*this, leaf_page.header->next_leaf, 0};
  // TODO: handle the edge case: an empty B+ tree with no leaf
}
