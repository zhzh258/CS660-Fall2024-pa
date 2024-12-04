#include <db/ColumnStats.hpp>
#include <algorithm>
#include <cmath>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
// TODO pa4: some code goes here
    : m_buckets(buckets), m_min(min), m_max(max), m_total_values(0),
      m_histogram(buckets, 0) {}

void ColumnStats::addValue(int v) {
  // TODO pa4: some code goes here
   // Ignore values outside [min, max]
  if (v < m_min || v > m_max)
    return;

  // Compute the bucket index i
  double bucket_width = (double)(m_max - m_min + 1) / m_buckets;
  int i = (int)((v - m_min) / bucket_width);

  if (i >= (int)m_buckets)
    i = m_buckets - 1;

  // Increment the bucket count
  m_histogram[i]++;
  // Increment total values count
  m_total_values++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
 // TODO pa4: some code goes here
  if (m_total_values == 0)
    return 0;

  // Handle values outside [min, max]
  if (v < m_min) {
    if (op == PredicateOp::LT || op == PredicateOp::LE)
      return 0;
    else if (op == PredicateOp::GT || op == PredicateOp::GE || op == PredicateOp::NE)
      return m_total_values;
    else if (op == PredicateOp::EQ)
      return 0;
  } else if (v > m_max) {
    if (op == PredicateOp::GT || op == PredicateOp::GE)
      return 0;
    else if (op == PredicateOp::LT || op == PredicateOp::LE || op == PredicateOp::NE)
      return m_total_values;
    else if (op == PredicateOp::EQ)
      return 0;
  }

  // Compute bucket index i
  double bucket_width = (double)(m_max - m_min + 1) / m_buckets;
  int i = (int)((v - m_min) / bucket_width);

  if (i >= (int)m_buckets)
    i = m_buckets - 1;

  // Bucket height h
  int h = m_histogram[i];

  // Compute bucket start and end
  double bucket_start = m_min + i * bucket_width;
  double bucket_end = bucket_start + bucket_width;

  // Adjust bucket_end for the last bucket to include m_max
  if (i == (int)m_buckets - 1)
    bucket_end = m_max + 1;

  // Bucket width (may not be exactly equal to bucket_width due to integer ranges)
  double actual_bucket_width = bucket_end - bucket_start;

  // Adjust estimation for buckets with width less than 1
  bool bucket_width_less_than_one = actual_bucket_width < 1.0;

  switch (op) {
  case PredicateOp::EQ: {
    if (bucket_width_less_than_one) {
      // Bucket represents a single value
      return h;
    } else {
      double est = h / actual_bucket_width;
      return (size_t)est;
    }
  }
  case PredicateOp::NE: {
    if (bucket_width_less_than_one) {
      return m_total_values - h;
    } else {
      double est_eq = h / actual_bucket_width;
      size_t est_ne = m_total_values - (size_t)est_eq;
      return est_ne;
    }
  }
  case PredicateOp::LT: {
    double sum = 0;
    for (int j = 0; j < i; ++j)
      sum += m_histogram[j];

    if (bucket_width_less_than_one) {
      if (v > bucket_start)
        sum += h;
    } else {
      double fraction = (v - bucket_start) / actual_bucket_width;
      fraction = std::max(0.0, std::min(fraction, 1.0));
      sum += h * fraction;
    }

    return (size_t)sum;
  }
  case PredicateOp::LE: {
    double sum = 0;
    for (int j = 0; j < i; ++j)
      sum += m_histogram[j];

    if (bucket_width_less_than_one) {
      if (v >= bucket_start)
        sum += h;
    } else {
      double fraction = (v - bucket_start + 1) / actual_bucket_width;
      fraction = std::max(0.0, std::min(fraction, 1.0));
      sum += h * fraction;
    }

    return (size_t)sum;
  }
  case PredicateOp::GT: {
    double sum = 0;
    for (int j = i + 1; j < (int)m_buckets; ++j)
      sum += m_histogram[j];

    if (bucket_width_less_than_one) {
      if (v < bucket_start)
        sum += h;
    } else {
      double fraction = (bucket_end - v - 1) / actual_bucket_width;
      fraction = std::max(0.0, std::min(fraction, 1.0));
      sum += h * fraction;
    }

    return (size_t)sum;
  }
  case PredicateOp::GE: {
    double sum = 0;
    for (int j = i + 1; j < (int)m_buckets; ++j)
      sum += m_histogram[j];

    if (bucket_width_less_than_one) {
      if (v <= bucket_start)
        sum += h;
    } else {
      double fraction = (bucket_end - v) / actual_bucket_width;
      fraction = std::max(0.0, std::min(fraction, 1.0));
      sum += h * fraction;
    }

    return (size_t)sum;
  }
  default:
    return 0;
  }
}
