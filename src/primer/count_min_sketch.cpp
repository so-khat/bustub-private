//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <algorithm>
#include <atomic>
#include <stdexcept>
#include <string>
#include <vector>

namespace bustub {
using std::vector;

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  // width, w = cols in hash matrix
  // depth, d = hash fns in hash matrix
  if (width_ == 0 || depth_ == 0) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for CountMinSketch initialisation.");
  }

  /** @TODO(student) Implement this function! */

  // initialise hash matrix as an array of atomic counters
  hash_matrix_ = std::make_unique<std::atomic<uint32_t>[]>(width_ * depth_);
  for (size_t r = 0; r < depth_; r++) {
    for (size_t c = 0; c < width_; c++) {
      // zero out values
      Cell(r, c).store(0, std::memory_order_relaxed);
    }
  }

  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept
    : width_(other.width_), depth_(other.depth_), hash_matrix_(std::move(other.hash_matrix_)) {
  /** @TODO(student) Implement this function! */
  // move the hash fucntions
  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  other.width_ = 0;
  other.depth_ = 0;
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */

  if (this == &other) {
    return *this;
  }

  width_ = other.width_;
  depth_ = other.depth_;
  hash_matrix_ = std::move(other.hash_matrix_);
  // initialize seeded hash functions
  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  other.width_ = 0;
  other.depth_ = 0;
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  // loop through the rows in the matrix
  for (size_t row = 0; row < depth_; row++) {
    // pass the item through the relevant hash function
    size_t col = hash_functions_[row](item);
    // atomically increment the counter at the correct idx
    Cell(row, col).fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  // combine the 2 matrices,
  // adding the values from the respective cells from other to curr matrix
  for (size_t row = 0; row < depth_; row++) {
    for (size_t col = 0; col < width_; col++) {
      auto temp = other.Cell(row, col).load(std::memory_order_relaxed);
      Cell(row, col).fetch_add(temp, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min_val = std::numeric_limits<uint32_t>::max();
  // loop through the rows in the matrix
  for (uint32_t row = 0; row < depth_; row++) {
    // pass the item through the relevant hash function
    uint32_t col = hash_functions_[row](item);
    // get the value in the matrix at the correct idx
    auto curr_val = static_cast<uint32_t>(Cell(row, col).load(std::memory_order_relaxed));
    // take min of curr val n min val
    min_val = std::min(min_val, curr_val);
  }
  // return min_val
  return min_val;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  // zero out values
  for (size_t row = 0; row < depth_; row++) {
    for (size_t col = 0; col < width_; col++) {
      Cell(row, col).store(0, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */

  // let k be min of k and candidates size
  k = std::min(k, static_cast<uint16_t>(candidates.size()));

  // create a 2d list with (freq, elem)
  std::vector<std::pair<KeyType, uint32_t>> data = {};
  for (auto &c : candidates) {
    size_t freq = Count(c);
    std::pair<KeyType, uint32_t> temp = {c, freq};
    data.push_back(temp);
  }

  // sort the 2d list by 2nd elem of each pairt in desc order
  std::sort(data.begin(), data.end(),
            [](const std::pair<KeyType, uint32_t> &lhs, const std::pair<KeyType, uint32_t> &rhs) {
              return lhs.second > rhs.second;
            });

  // slice and return first k elems
  std::vector<std::pair<KeyType, uint32_t>> topk(data.begin(), data.begin() + k);
  return topk;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
