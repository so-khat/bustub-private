
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.h
//
// Identification: src/include/buffer/arc_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

enum class ArcStatus { MRU, MFU, MRU_GHOST, MFU_GHOST };

struct FrameStatus {
  page_id_t page_id_{INVALID_PAGE_ID};
  frame_id_t frame_id_{INVALID_FRAME_ID};
  bool evictable_{false};
  ArcStatus arc_status_{ArcStatus::MRU};
  FrameStatus() = default;
  FrameStatus(page_id_t pid, frame_id_t fid, bool ev, ArcStatus st)
      : page_id_(pid), frame_id_(fid), evictable_(ev), arc_status_(st) {}
};

/**
 * ArcReplacer implements the ARC replacement policy.
 */
class ArcReplacer {
 public:
  explicit ArcReplacer(size_t num_frames);

  DISALLOW_COPY_AND_MOVE(ArcReplacer);

  /**
   *
   * @brief Destroys the LRUReplacer.
   */
  ~ArcReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;
  void RecordAccess(frame_id_t frame_id, page_id_t page_id, AccessType access_type = AccessType::Unknown);
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  void Remove(frame_id_t frame_id);
  auto Size() -> size_t;

 private:
  std::list<std::tuple<page_id_t, frame_id_t>> mru_;
  std::list<std::tuple<page_id_t, frame_id_t>> mfu_;
  std::list<page_id_t> mru_ghost_;
  std::list<page_id_t> mfu_ghost_;

  /* alive, evictable entries count */
  [[maybe_unused]] size_t curr_size_{0};
  /* p as in original paper */
  [[maybe_unused]] size_t mru_target_size_{0};
  /* c as in original paper */
  [[maybe_unused]] size_t replacer_size_;
  std::mutex latch_;

  // live pages: find the node for a frame in MRU/MFU in O(1)
  std::unordered_map<frame_id_t, std::list<std::tuple<page_id_t, frame_id_t>>::iterator> mru_pos_;
  std::unordered_map<frame_id_t, std::list<std::tuple<page_id_t, frame_id_t>>::iterator> mfu_pos_;

  // ghost pages: find the node for a page_id in ghost lists in O(1)
  std::unordered_map<page_id_t, std::list<page_id_t>::iterator> mru_ghost_pos_;
  std::unordered_map<page_id_t, std::list<page_id_t>::iterator> mfu_ghost_pos_;

  // track the status of frames in mru and mfu lists
  std::unordered_map<frame_id_t, FrameStatus> meta_;
};
}  // namespace bustub
