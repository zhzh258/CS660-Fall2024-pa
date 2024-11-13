#include <db/Query.hpp>
#include <unordered_map>
#include <stdexcept>
using namespace db;
#include <map>

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  const TupleDesc &in_td = in.getTupleDesc();
  const TupleDesc &out_td = out.getTupleDesc();

  // Ensure that the output TupleDesc has the correct number of fields
  if (field_names.size() != out_td.size()) {
    throw std::runtime_error("Output TupleDesc does not match the number of projected fields");
  }

  // Prepare a vector of input indices corresponding to the field names
  std::vector<size_t> indices_in;
  indices_in.reserve(field_names.size());

  for (const auto &field_name : field_names) {
    try {
      size_t index_in = in_td.index_of(field_name);
      indices_in.push_back(index_in);
    } catch (const std::out_of_range &) {
      throw std::runtime_error("Field name not found in input TupleDesc: " + field_name);
    }
  }

  // Iterate over each tuple in the input file
  for (auto it = in.begin(); it != in.end(); ++it) {
    const Tuple &t_in = *it;

    // Build a vector of fields for the output tuple
    std::vector<field_t> out_fields;
    out_fields.reserve(indices_in.size());

    for (size_t index_in : indices_in) {
      out_fields.push_back(t_in.get_field(index_in));
    }

    // Create a new Tuple with the output fields
    Tuple t_out(out_fields);

    // Ensure the new tuple is compatible with the output TupleDesc
    if (!out_td.compatible(t_out)) {
      throw std::runtime_error("Incompatible tuple in projection operation");
    }

    // Insert the new tuple into the output file
    out.insertTuple(t_out);
  }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  const TupleDesc &in_td = in.getTupleDesc();
  const TupleDesc &out_td = out.getTupleDesc();

  // Ensure that the output TupleDesc matches the input TupleDesc
  if (in_td.size() != out_td.size()) {
    throw std::runtime_error("Output TupleDesc does not match input TupleDesc");
  }

  // Iterate over each tuple in the input file
  for (auto it = in.begin(); it != in.end(); ++it) {
    const Tuple &t_in = *it;
    bool satisfies_all = true;

    // Check all predicates
    for (const auto &p : pred) {
      // Get the index of the field
      size_t field_index;
      try {
        field_index = in_td.index_of(p.field_name);
      } catch (const std::out_of_range &) {
        throw std::runtime_error("Field name not found in input TupleDesc: " + p.field_name);
      }

      // Get the field value from the tuple
      const field_t &field_value = t_in.get_field(field_index);

      // Only numeric fields are supported
      if (!std::holds_alternative<int>(field_value) && !std::holds_alternative<double>(field_value)) {
        throw std::runtime_error("FilterPredicate only supports numeric fields");
      }

      // Get the value from the predicate
      const field_t &predicate_value = p.value;

      // Convert both to double for comparison
      double field_num_value;
      double pred_num_value;

      if (std::holds_alternative<int>(field_value)) {
        field_num_value = static_cast<double>(std::get<int>(field_value));
      } else {
        field_num_value = std::get<double>(field_value);
      }

      if (std::holds_alternative<int>(predicate_value)) {
        pred_num_value = static_cast<double>(std::get<int>(predicate_value));
      } else if (std::holds_alternative<double>(predicate_value)) {
        pred_num_value = std::get<double>(predicate_value);
      } else {
        throw std::runtime_error("Predicate value must be numeric");
      }

      // Perform comparison based on the operator
      bool comparison_result = false;
      switch (p.op) {
        case PredicateOp::EQ:
          comparison_result = (field_num_value == pred_num_value);
          break;
        case PredicateOp::NE:
          comparison_result = (field_num_value != pred_num_value);
          break;
        case PredicateOp::LT:
          comparison_result = (field_num_value < pred_num_value);
          break;
        case PredicateOp::LE:
          comparison_result = (field_num_value <= pred_num_value);
          break;
        case PredicateOp::GT:
          comparison_result = (field_num_value > pred_num_value);
          break;
        case PredicateOp::GE:
          comparison_result = (field_num_value >= pred_num_value);
          break;
        default:
          throw std::runtime_error("Unknown PredicateOp");
      }

      if (!comparison_result) {
        satisfies_all = false;
        break; // No need to check other predicates
      }
    }

    if (satisfies_all) {
      // Ensure the tuple is compatible with the output TupleDesc
      if (!out_td.compatible(t_in)) {
        throw std::runtime_error("Incompatible tuple in filter operation");
      }

      // Insert the tuple into the output file
      out.insertTuple(t_in);
    }
  }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  const TupleDesc &in_td = in.getTupleDesc();
  const TupleDesc &out_td = out.getTupleDesc();

  // Get indices of the aggregate field and the group field (if any)
  size_t agg_field_index;
  try {
    agg_field_index = in_td.index_of(agg.field);
  } catch (const std::out_of_range &) {
    throw std::runtime_error("Aggregate field name not found in input TupleDesc: " + agg.field);
  }

  size_t group_field_index;
  bool has_group = agg.group.has_value();
  if (has_group) {
    try {
      group_field_index = in_td.index_of(agg.group.value());
    } catch (const std::out_of_range &) {
      throw std::runtime_error("Group field name not found in input TupleDesc: " + agg.group.value());
    }
  }

  // Define a map from group key to aggregate values
  struct FieldComparator {
    bool operator()(const field_t &lhs, const field_t &rhs) const {
      if (lhs.index() != rhs.index()) {
        return lhs.index() < rhs.index();
      }

      if (std::holds_alternative<int>(lhs)) {
        return std::get<int>(lhs) < std::get<int>(rhs);
      } else if (std::holds_alternative<double>(lhs)) {
        return std::get<double>(lhs) < std::get<double>(rhs);
      } else if (std::holds_alternative<std::string>(lhs)) {
        return std::get<std::string>(lhs) < std::get<std::string>(rhs);
      }
      return false;
    }
  };

  struct AggValue {
    int count = 0;
    double sum = 0.0;
    double min = 0.0;
    double max = 0.0;
  };

  std::map<field_t, AggValue, FieldComparator> group_agg_map;

  // Variable to store the type of the aggregate field
  type_t agg_field_type;
  bool agg_field_type_set = false;

  // Iterate over each tuple in the input file
  for (auto it = in.begin(); it != in.end(); ++it) {
    const Tuple &t_in = *it;

    // Get group key
    field_t group_key;
    if (has_group) {
      group_key = t_in.get_field(group_field_index);
    } else {
      group_key = {}; // Empty field_t for no group
    }

    // Get aggregate field value
    const field_t &agg_field_value = t_in.get_field(agg_field_index);

    // On the first iteration, set the agg_field_type
    if (!agg_field_type_set) {
      if (std::holds_alternative<int>(agg_field_value)) {
        agg_field_type = type_t::INT;
      } else if (std::holds_alternative<double>(agg_field_value)) {
        agg_field_type = type_t::DOUBLE;
      } else {
        throw std::runtime_error("Aggregate field must be numeric");
      }
      agg_field_type_set = true;
    }

    // Initialize group in map if not present
    auto &agg_value = group_agg_map[group_key];

    // Update aggregate values
    agg_value.count += 1;

    // For SUM, AVG, MIN, MAX, process numeric fields
    double numeric_value;
    if (std::holds_alternative<int>(agg_field_value)) {
      numeric_value = static_cast<double>(std::get<int>(agg_field_value));
    } else if (std::holds_alternative<double>(agg_field_value)) {
      numeric_value = std::get<double>(agg_field_value);
    } else {
      throw std::runtime_error("Aggregate field must be numeric");
    }

    agg_value.sum += numeric_value;
    if (agg_value.count == 1) {
      agg_value.min = numeric_value;
      agg_value.max = numeric_value;
    } else {
      if (numeric_value < agg_value.min) {
        agg_value.min = numeric_value;
      }
      if (numeric_value > agg_value.max) {
        agg_value.max = numeric_value;
      }
    }
  }

  // Now, for each group, compute the aggregate result and insert into 'out'
  for (const auto &entry : group_agg_map) {
    const field_t &group_key = entry.first;
    const AggValue &agg_value = entry.second;

    // Compute aggregate result
    field_t agg_result;

    switch (agg.op) {
      case AggregateOp::COUNT:
        agg_result = agg_value.count;
        break;
      case AggregateOp::SUM:
        if (agg_field_type == type_t::INT) {
          agg_result = static_cast<int>(agg_value.sum);
        } else {
          agg_result = agg_value.sum;
        }
        break;
      case AggregateOp::AVG:
        agg_result = agg_value.sum / agg_value.count; // Always return double
        break;
      case AggregateOp::MIN:
        if (agg_field_type == type_t::INT) {
          agg_result = static_cast<int>(agg_value.min);
        } else {
          agg_result = agg_value.min;
        }
        break;
      case AggregateOp::MAX:
        if (agg_field_type == type_t::INT) {
          agg_result = static_cast<int>(agg_value.max);
        } else {
          agg_result = agg_value.max;
        }
        break;
      default:
        throw std::runtime_error("Unknown AggregateOp");
    }

    // Build output tuple
    std::vector<field_t> out_fields;
    if (has_group) {
      // Output tuple has two fields: group field and aggregate result
      out_fields.push_back(group_key);
      out_fields.push_back(agg_result);
    } else {
      // Output tuple has one field: aggregate result
      out_fields.push_back(agg_result);
    }

    // Create a new Tuple with the output fields
    Tuple t_out(out_fields);

    // Ensure the new tuple is compatible with the output TupleDesc
    if (!out_td.compatible(t_out)) {
      throw std::runtime_error("Incompatible tuple in aggregate operation");
    }

    // Insert the new tuple into the output file
    out.insertTuple(t_out);
  }
}



//////////////////////////////////////////////////////
namespace {
  // Helper function to compare field values based on PredicateOp
  bool compare_field_values(const field_t &left_value, const field_t &right_value, PredicateOp op) {
    // Ensure the types are the same
    if (left_value.index() != right_value.index()) {
      throw std::runtime_error("Cannot compare fields of different types");
    }

    if (std::holds_alternative<int>(left_value)) {
      int left_int = std::get<int>(left_value);
      int right_int = std::get<int>(right_value);
      switch (op) {
        case PredicateOp::EQ: return left_int == right_int;
        case PredicateOp::NE: return left_int != right_int;
        case PredicateOp::LT: return left_int < right_int;
        case PredicateOp::LE: return left_int <= right_int;
        case PredicateOp::GT: return left_int > right_int;
        case PredicateOp::GE: return left_int >= right_int;
      }
    } else if (std::holds_alternative<double>(left_value)) {
      double left_double = std::get<double>(left_value);
      double right_double = std::get<double>(right_value);
      switch (op) {
        case PredicateOp::EQ: return left_double == right_double;
        case PredicateOp::NE: return left_double != right_double;
        case PredicateOp::LT: return left_double < right_double;
        case PredicateOp::LE: return left_double <= right_double;
        case PredicateOp::GT: return left_double > right_double;
        case PredicateOp::GE: return left_double >= right_double;
      }
    } else if (std::holds_alternative<std::string>(left_value)) {
      const std::string &left_str = std::get<std::string>(left_value);
      const std::string &right_str = std::get<std::string>(right_value);
      switch (op) {
        case PredicateOp::EQ: return left_str == right_str;
        case PredicateOp::NE: return left_str != right_str;
        case PredicateOp::LT: return left_str < right_str;
        case PredicateOp::LE: return left_str <= right_str;
        case PredicateOp::GT: return left_str > right_str;
        case PredicateOp::GE: return left_str >= right_str;
      }
    }
    throw std::runtime_error("Unknown field type in comparison");
  }

  // Hash and equality functions for field_t
  struct FieldHash {
    std::size_t operator()(const field_t &field) const {
      if (std::holds_alternative<int>(field)) {
        return std::hash<int>{}(std::get<int>(field));
      } else if (std::holds_alternative<double>(field)) {
        return std::hash<double>{}(std::get<double>(field));
      } else if (std::holds_alternative<std::string>(field)) {
        return std::hash<std::string>{}(std::get<std::string>(field));
      } else {
        return 0;
      }
    }
  };

  struct FieldEqual {
    bool operator()(const field_t &lhs, const field_t &rhs) const {
      if (lhs.index() != rhs.index()) {
        return false;
      }
      if (std::holds_alternative<int>(lhs)) {
        return std::get<int>(lhs) == std::get<int>(rhs);
      } else if (std::holds_alternative<double>(lhs)) {
        return std::get<double>(lhs) == std::get<double>(rhs);
      } else if (std::holds_alternative<std::string>(lhs)) {
        return std::get<std::string>(lhs) == std::get<std::string>(rhs);
      } else {
        return false;
      }
    }
  };
}
void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  const TupleDesc &left_td = left.getTupleDesc();
  const TupleDesc &right_td = right.getTupleDesc();
  const TupleDesc &out_td = out.getTupleDesc();

  // Get indices of the join fields
  size_t left_field_index;
  try {
    left_field_index = left_td.index_of(pred.left);
  } catch (const std::out_of_range &) {
    throw std::runtime_error("Left field name not found in left TupleDesc: " + pred.left);
  }

  size_t right_field_index;
  try {
    right_field_index = right_td.index_of(pred.right);
  } catch (const std::out_of_range &) {
    throw std::runtime_error("Right field name not found in right TupleDesc: " + pred.right);
  }

  // For EQ joins, we can build a hash map from right join field value to tuples
  if (pred.op == PredicateOp::EQ) {
    // Build a hash map from right join field value to vector of tuples
    std::unordered_map<field_t, std::vector<Tuple>, FieldHash, FieldEqual> right_hash_map;

    // Build the hash map
    for (auto right_it = right.begin(); right_it != right.end(); ++right_it) {
      const Tuple &t_right = *right_it;
      const field_t &right_field_value = t_right.get_field(right_field_index);
      right_hash_map[right_field_value].push_back(t_right);
    }

    // Iterate over left tuples and probe the hash map
    for (auto left_it = left.begin(); left_it != left.end(); ++left_it) {
      const Tuple &t_left = *left_it;
      const field_t &left_field_value = t_left.get_field(left_field_index);

      auto it = right_hash_map.find(left_field_value);
      if (it != right_hash_map.end()) {
        const auto &matching_right_tuples = it->second;
        for (const Tuple &t_right : matching_right_tuples) {
          // Build output tuple
          std::vector<field_t> out_fields;

          // Add fields from left tuple
          for (size_t i = 0; i < left_td.size(); ++i) {
            out_fields.push_back(t_left.get_field(i));
          }

          // Add fields from right tuple, except the join field
          for (size_t i = 0; i < right_td.size(); ++i) {
            if (i != right_field_index) {
              out_fields.push_back(t_right.get_field(i));
            }
          }

          // Create a new Tuple with the output fields
          Tuple t_out(out_fields);

          // Ensure the new tuple is compatible with the output TupleDesc
          if (!out_td.compatible(t_out)) {
            throw std::runtime_error("Incompatible tuple in join operation");
          }

          // Insert the new tuple into the output file
          out.insertTuple(t_out);
        }
      }
    }
  } else {
    // For non-equality joins, perform nested loop join
    for (auto left_it = left.begin(); left_it != left.end(); ++left_it) {
      const Tuple &t_left = *left_it;
      const field_t &left_field_value = t_left.get_field(left_field_index);

      for (auto right_it = right.begin(); right_it != right.end(); ++right_it) {
        const Tuple &t_right = *right_it;
        const field_t &right_field_value = t_right.get_field(right_field_index);

        // Compare left_field_value op right_field_value
        bool comparison_result = compare_field_values(left_field_value, right_field_value, pred.op);

        if (comparison_result) {
          // Build output tuple
          std::vector<field_t> out_fields;

          // Add fields from left tuple
          for (size_t i = 0; i < left_td.size(); ++i) {
            out_fields.push_back(t_left.get_field(i));
          }

          // Add fields from right tuple
          for (size_t i = 0; i < right_td.size(); ++i) {
            out_fields.push_back(t_right.get_field(i));
          }

          // Create a new Tuple with the output fields
          Tuple t_out(out_fields);

          // Ensure the new tuple is compatible with the output TupleDesc
          if (!out_td.compatible(t_out)) {
            throw std::runtime_error("Incompatible tuple in join operation");
          }

          // Insert the new tuple into the output file
          out.insertTuple(t_out);
        }
      }
    }
  }
}
