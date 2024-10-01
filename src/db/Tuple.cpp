#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
  const field_t &field = fields.at(i);
  if (std::holds_alternative<int>(field)) {
    return type_t::INT;
  }
  if (std::holds_alternative<double>(field)) {
    return type_t::DOUBLE;
  }
  if (std::holds_alternative<std::string>(field)) {
    return type_t::CHAR;
  }
  throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names)
  // TODO pa2: add initializations if needed
{
  // TODO pa2: implement
  if (types.size() != names.size()) {
    throw std::invalid_argument("TupleDesc::TupleDesc() - types and names must have the same size!");
  }
  std::unordered_set<std::string> names_set(names.begin(), names.end());
  if (names_set.size() != names.size()) {
    throw std::invalid_argument("TupleDesc::TupleDesc() - names must be unique!");
  }
  this->types = types;
  this->names = names;
}

bool TupleDesc::compatible(const Tuple &tuple) const {
  // TODO pa2: implement

  // Check the same number of fields
  if (tuple.size() != this->types.size()) {
    return false;
  }

  // Check the type of each field
  for (size_t i = 0; i < tuple.size(); i++) {
    if (tuple.field_type(i) != this->types[i]) {
      return false;
    }
  }
  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  // TODO pa2: implement
  auto iter = std::find(this->names.begin(), this->names.end(), name);
  if (iter == this->names.end()) { // Not found
    throw std::invalid_argument("TupleDesc::index_of - name not found in TupleDesc!");
  } else {
    return std::distance(this->names.begin(), iter);
  }
}

size_t TupleDesc::offset_of(const size_t &index) const {
  // TODO pa2: implement
  if (index > this->types.size() - 1) {
    throw std::invalid_argument("TupleDesc::offset_of() - index out of range!");
  }

  // Calculate the offset needed
  return std::accumulate(this->types.begin(), this->types.begin() + index, (size_t)0, [](size_t acc, type_t type) {
    switch (type)
    {
    case type_t::INT:
      return acc + db::INT_SIZE;
    case type_t::DOUBLE:
      return acc + db::DOUBLE_SIZE;
    case type_t::CHAR:
      return acc + db::CHAR_SIZE;
    default:
      return acc + 0;
    }
  });
}

size_t TupleDesc::length() const {
  // TODO pa2: implement
  return std::accumulate(this->types.begin(), this->types.end(), (size_t)0, [](size_t acc, type_t type) {
    switch (type)
    {
    case type_t::INT:
      return acc + db::INT_SIZE;
    case type_t::DOUBLE:
      return acc + db::DOUBLE_SIZE;
    case type_t::CHAR:
      return acc + db::CHAR_SIZE;
    default:
      return acc + 0;
    }
  });
}

size_t TupleDesc::size() const {
  // TODO pa2: implement
  return this->types.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
  // TODO pa2: implement
  std::vector<field_t> fields(this->size());
  for (size_t i = 0; i < this->size(); i++) { // i-th field
    switch (this->types[i]) {
      case type_t::INT: {
        int field_int = 0; // initially 0x0 0x0 0x0 0x0
        std::memcpy(&field_int, data, db::INT_SIZE);
        data += db::INT_SIZE;
        fields[i] = field_int;
        break;
      }
      case type_t::DOUBLE: {
        double field_double = 0.0; // initially 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0
        std::memcpy(&field_double, data, db::DOUBLE_SIZE);
        data += db::DOUBLE_SIZE;
        fields[i] = field_double;
        break;
      }
      case type_t::CHAR: {
        std::string field_string = "";
        for (size_t byte = 0; byte < db::CHAR_SIZE; byte ++) { // 64 char a string???? If this is what types.hpp means
          field_string.push_back(*data);
          data ++;
        }
        fields[i] = field_string;
        break;
      }
    }
  }
  return Tuple(fields);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
  // TODO pa2: implement
  if (!this->compatible(t)) {
    throw std::logic_error("TupleDesc::serialize() - Tuple t is NOT compatible to the TupleDesc!");
  }
  for (size_t i = 0; i < t.size(); i++) {
    switch (this->types[i]) {
      case type_t::INT: {
        int field_int = std::get<int>(t.get_field(i));
        memcpy(data, &field_int, db::INT_SIZE);
        data += db::INT_SIZE;
        break;
      }
      case type_t::DOUBLE: {
        double field_double = std::get<double>(t.get_field(i));
        memcpy(data, &field_double, db::DOUBLE_SIZE);
        data += db::DOUBLE_SIZE;
        break;
      }
      case type_t::CHAR: {
        std::string field_string = std::get<std::string>(t.get_field(i));
        if (field_string.size() < db::CHAR_SIZE) {
          field_string.append(CHAR_SIZE - field_string.size(), '\0');
        } else if (field_string.size() > CHAR_SIZE) {
          field_string = field_string.substr(0, CHAR_SIZE);
        }
        for (size_t byte = 0; byte < db::CHAR_SIZE; byte ++) {
          (*data) = field_string[byte];
          data ++;
        }
        break;
      }
    }
  }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  // TODO pa2: implement
  std::vector<type_t> types = td1.types;
  std::vector<std::string> names = td1.names;
  types.insert(types.end(), td2.types.begin(), td2.types.end());
  names.insert(names.end(), td2.names.begin(), td2.names.end());
  return {types, names};
}
