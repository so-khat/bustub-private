// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <cstddef>
#include <iterator>
#include <list>
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock<std::mutex> lock(latch_);

  auto erase_from_ghosts = [&](page_id_t pid) {
    if (auto it = mru_ghost_pos_.find(pid); it != mru_ghost_pos_.end()) {
      mru_ghost_.erase(it->second);
      mru_ghost_pos_.erase(it);
    }
    if (auto it = mfu_ghost_pos_.find(pid); it != mfu_ghost_pos_.end()) {
      mfu_ghost_.erase(it->second);
      mfu_ghost_pos_.erase(it);
    }
  };

  auto add_to_ghost = [&](bool from_mru, page_id_t pid) {
    erase_from_ghosts(pid);
    if (from_mru) {
      // dedup
      if (auto it = mru_ghost_pos_.find(pid); it != mru_ghost_pos_.end()) {
        mru_ghost_.erase(it->second);
        mru_ghost_pos_.erase(it);
      }
      mru_ghost_.push_front(pid);
      mru_ghost_pos_[pid] = mru_ghost_.begin();
    } else {
      if (auto it = mfu_ghost_pos_.find(pid); it != mfu_ghost_pos_.end()) {
        mfu_ghost_.erase(it->second);
        mfu_ghost_pos_.erase(it);
      }
      mfu_ghost_.push_front(pid);
      mfu_ghost_pos_[pid] = mfu_ghost_.begin();
    }
  };

  auto try_evict_from = [&](bool from_mru) -> std::optional<frame_id_t> {
    auto &lst = from_mru ? mru_ : mfu_;
    auto &pos = from_mru ? mru_pos_ : mfu_pos_;

    // walk from LRU end -> MRU front (using reverse iterator)
    for (auto rit = lst.rbegin(); rit != lst.rend(); ++rit) {
      frame_id_t fid = std::get<1>(*rit);

      auto it_meta = meta_.find(fid);
      if (it_meta == meta_.end()) {
        // invariant broken; skip or crash. I'd crash:
        throw std::runtime_error("meta_ missing for tracked frame during Evict");
      }
      if (!it_meta->second.evictable_) {
        continue;  // pinned, can't evict
      }

      page_id_t pid = std::get<0>(*rit);

      // erase node from list
      auto victim_it = std::next(rit).base();
      lst.erase(victim_it);

      // erase iterator map entry
      pos.erase(fid);

      // move page_id to corresponding ghost list
      add_to_ghost(from_mru, pid);

      // remove metadata for this frame (no longer tracked live)
      meta_.erase(it_meta);

      // update evictable count
      curr_size_--;

      return fid;
    }
    return std::nullopt;
  };

  // Preferred side per spec
  bool prefer_mfu = (mru_.size() < mru_target_size_);

  if (prefer_mfu) {
    if (auto v = try_evict_from(/*from_mru=*/false); v.has_value()) {
      return v;
    }
    return try_evict_from(/*from_mru=*/true);
  }

  // else prefer MRU
  if (auto v = try_evict_from(/*from_mru=*/true); v.has_value()) {
    return v;
  }
  return try_evict_from(/*from_mru=*/false);
}

/**
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::runtime_error("invalid frame_id");
  }

  // Helper: preserve existing evictable flag if meta_ already has this frame.
  auto get_evictable = [&]() -> bool {
    auto it = meta_.find(frame_id);
    return (it == meta_.end()) ? false : it->second.evictable_;
  };

  // Helpers to adjust target size safely (all integer math)
  auto inc_target = [&](size_t delta) { mru_target_size_ = std::min(replacer_size_, mru_target_size_ + delta); };
  auto dec_target = [&](size_t delta) {
    mru_target_size_ = (mru_target_size_ >= delta) ? (mru_target_size_ - delta) : 0;
  };

  // Helpers to remove page ids from other ghosts
  auto erase_from_ghosts = [&](page_id_t pid) {
    if (auto it = mru_ghost_pos_.find(pid); it != mru_ghost_pos_.end()) {
      mru_ghost_.erase(it->second);
      mru_ghost_pos_.erase(it);
    }
    if (auto it = mfu_ghost_pos_.find(pid); it != mfu_ghost_pos_.end()) {
      mfu_ghost_.erase(it->second);
      mfu_ghost_pos_.erase(it);
    }
  };

  // -------------------------
  // Case 1: hit in MRU or MFU (by frame_id)
  // -------------------------

  // 1a) Hit in MRU: move that node to front of MFU.
  if (auto it_mru = mru_pos_.find(frame_id); it_mru != mru_pos_.end()) {
    auto node_it = it_mru->second;
    page_id_t pid = std::get<0>(*node_it);

    // Move node from MRU to front of MFU in O(1)
    mfu_.splice(mfu_.begin(), mru_, node_it);

    // Update iterator maps
    mru_pos_.erase(it_mru);
    mfu_pos_[frame_id] = mfu_.begin();

    // Update meta (preserve evictable)
    bool ev = get_evictable();
    meta_[frame_id] = FrameStatus(pid, frame_id, ev, ArcStatus::MFU);
    return;
  }

  // 1b) Hit in MFU: move that node to front of MFU.
  if (auto it_mfu = mfu_pos_.find(frame_id); it_mfu != mfu_pos_.end()) {
    auto node_it = it_mfu->second;
    page_id_t pid = std::get<0>(*node_it);

    // Move within same list to front
    mfu_.splice(mfu_.begin(), mfu_, node_it);
    mfu_pos_[frame_id] = mfu_.begin();

    // Update meta (preserve evictable)
    bool ev = get_evictable();
    meta_[frame_id] = FrameStatus(pid, frame_id, ev, ArcStatus::MFU);
    return;
  }

  // -------------------------
  // Case 2: hit in MRU ghost (by page_id)
  // -------------------------
  if (auto it_g = mru_ghost_pos_.find(page_id); it_g != mru_ghost_pos_.end()) {
    // Adapt target upward
    if (mru_ghost_.size() >= mfu_ghost_.size()) {
      inc_target(1);
    } else {
      // integer division already floors
      // mru_ghost_.size() > 0 because page_id is in it
      inc_target(mfu_ghost_.size() / mru_ghost_.size());
    }

    // Remove from ghost lists + maps
    erase_from_ghosts(page_id);

    // Insert into front of MFU
    mfu_.emplace_front(page_id, frame_id);
    mfu_pos_[frame_id] = mfu_.begin();

    // Update meta (preserve evictable)
    bool ev = get_evictable();
    meta_[frame_id] = FrameStatus(page_id, frame_id, ev, ArcStatus::MFU);
    return;
  }

  // -------------------------
  // Case 3: hit in MFU ghost (by page_id)
  // -------------------------
  if (auto it_g = mfu_ghost_pos_.find(page_id); it_g != mfu_ghost_pos_.end()) {
    // Adapt target downward
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      dec_target(1);
    } else {
      // mfu_ghost_.size() > 0 because page_id is in it
      dec_target(mru_ghost_.size() / mfu_ghost_.size());
    }

    // Remove from ghost lists + maps
    erase_from_ghosts(page_id);

    // Insert into front of MFU
    mfu_.emplace_front(page_id, frame_id);
    mfu_pos_[frame_id] = mfu_.begin();

    // Update meta (preserve evictable)
    bool ev = get_evictable();
    meta_[frame_id] = FrameStatus(page_id, frame_id, ev, ArcStatus::MFU);
    return;
  }

  // -------------------------
  // Case 4: miss in all lists
  // -------------------------

  // Defensive: ensure the incoming page_id isn't lingering in either ghost list
  erase_from_ghosts(page_id);

  // 4(a) If |MRU| + |MRU_ghost| == capacity: kill LRU of MRU_ghost (if it exists)
  if (mru_.size() + mru_ghost_.size() == replacer_size_) {
    if (!mru_ghost_.empty()) {
      page_id_t victim_pid = mru_ghost_.back();  // LRU of MRU_ghost
      mru_ghost_.pop_back();
      mru_ghost_pos_.erase(victim_pid);
    }
  } else {
    // 4(b) Else (|MRU| + |MRU_ghost| < capacity):
    // If total size == 2*capacity: kill LRU of MFU_ghost (if it exists)
    if (mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
      if (!mfu_ghost_.empty()) {
        page_id_t victim_pid = mfu_ghost_.back();  // LRU of MFU_ghost
        mfu_ghost_.pop_back();
        mfu_ghost_pos_.erase(victim_pid);
      }
    }
  }

  // Finally, insert into front of MRU
  mru_.emplace_front(page_id, frame_id);
  mru_pos_[frame_id] = mru_.begin();

  // Update meta (preserve evictable)
  bool ev = get_evictable();
  meta_[frame_id] = FrameStatus(page_id, frame_id, ev, ArcStatus::MRU);
}

/**
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // lock the latch
  std::scoped_lock<std::mutex> lock(latch_);

  // if frame id is invalid, throw an exception
  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::runtime_error("invalid frame_id");
  }

  // check if frame is in mru or mfu
  bool in_mru = (mru_pos_.count(frame_id) != 0);
  bool in_mfu = (mfu_pos_.count(frame_id) != 0);
  if (!in_mru && !in_mfu) {
    return;
  }

  // ensure that frame is tracked in meta
  auto it = meta_.find(frame_id);
  if (it == meta_.end()) {
    throw std::runtime_error("meta_ missing for tracked frame");
  }

  // check prev val of evictable field of frame
  bool prev_evc_val = it->second.evictable_;

  // if new val is diff, set and update curr size
  if (prev_evc_val != set_evictable) {
    it->second.evictable_ = set_evictable;
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
}

/**
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always removes the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  // lock the latch
  std::scoped_lock<std::mutex> lock(latch_);

  // if frame id is invalid, throw an exception
  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::runtime_error("invalid frame_id");
  }

  // check if frame is in mru or mfu
  auto it_mru = mru_pos_.find(frame_id);
  auto it_mfu = mfu_pos_.find(frame_id);
  if (it_mru == mru_pos_.end() && it_mfu == mfu_pos_.end()) {
    return;
  }

  // check if frame is tracked in meta
  auto it_meta = meta_.find(frame_id);
  if (it_meta == meta_.end()) {
    throw std::runtime_error("meta_ missing for tracked frame");
  }

  // ensure frame is evictable
  bool evc_val = it_meta->second.evictable_;
  if (!evc_val) {
    throw std::runtime_error("attempting to remove frame that is not evictable");
  }

  // remove from mru/mfu list n map
  if (it_mru != mru_pos_.end()) {
    auto it_mru_list = it_mru->second;
    mru_.erase(it_mru_list);
    mru_pos_.erase(it_mru);
  } else {
    auto it_mfu_list = it_mfu->second;
    mfu_.erase(it_mfu_list);
    mfu_pos_.erase(it_mfu);
  }

  // remove from meta
  meta_.erase(it_meta);
  curr_size_--;
}

/**
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
