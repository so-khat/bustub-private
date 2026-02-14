//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
  page_id_ = INVALID_PAGE_ID;  
}


/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  bool need_flush = false;
  page_id_t flush_pid = INVALID_PAGE_ID;
  std::vector<char> flush_buf;

  std::shared_ptr<FrameHeader> frame;
  frame_id_t fid = INVALID_FRAME_ID;

  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);

    // Choose a frame.
    if (!free_frames_.empty()) {
      fid = free_frames_.front();
      free_frames_.pop_front();
      frame = frames_[fid];

      // Lock frame under bpm latch to avoid reuse races.
      frame->rwlatch_.lock();
    } else {
      auto victim_opt = replacer_->Evict();
      if (!victim_opt.has_value()) {
        return INVALID_PAGE_ID;  // IMPORTANT: no pid allocated yet
      }
      fid = victim_opt.value();
      frame = frames_[fid];

      // Lock victim frame under bpm latch.
      frame->rwlatch_.lock();

      // Find + remove victim mapping.
      page_id_t victim_pid = frame->page_id_;
      BUSTUB_ENSURE(victim_pid != INVALID_PAGE_ID, "Evicted frame had no page id");
      page_table_.erase(victim_pid);

      // Copy dirty bytes for flush outside locks.
      if (frame->is_dirty_) {
        need_flush = true;
        flush_pid = victim_pid;
        flush_buf.assign(frame->GetData(), frame->GetData() + BUSTUB_PAGE_SIZE);
        frame->is_dirty_ = false;  // we are logically flushing it now
      }
    }

    // Allocate new page id only after we secured a frame.
    const page_id_t new_pid = next_page_id_.fetch_add(1);

    // Initialize frame for new page.
    frame->Reset();
    frame->page_id_ = new_pid;
    frame->pin_count_.store(0);
    frame->is_dirty_ = false;

    // Install new mapping.
    page_table_[new_pid] = fid;

    // Replacer bookkeeping.
    replacer_->RecordAccess(fid, new_pid);
    replacer_->SetEvictable(fid, true);

    // Unlock frame BEFORE doing any I/O.
    frame->rwlatch_.unlock();

    // Release bpm latch and do I/O below.
    lock.unlock();

    if (need_flush) {
      DiskRequest req;
      req.is_write_ = true;
      req.data_ = flush_buf.data();
      req.page_id_ = flush_pid;

      auto promise = disk_scheduler_->CreatePromise();
      auto fut = promise.get_future();
      req.callback_ = std::move(promise);

      std::vector<DiskRequest> requests;
      requests.push_back(std::move(req));
      disk_scheduler_->Schedule(requests);
      (void)fut.get();
    }

    return new_pid;
  }
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places that a page or a page's metadata could be, and use that to guide you on implementing
 * this function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  const frame_id_t fid = it->second;
  auto frame = frames_[fid];

  if (frame->pin_count_.load() > 0) {
    return false;
  }

  // Remove mapping
  page_table_.erase(it);

  // Make sure ARC no longer tracks this frame.
  // ARC::Remove throws if the frame is tracked but non-evictable.
  // When pin_count == 0, it SHOULD be evictable; enforce it to be safe.
  replacer_->SetEvictable(fid, true);
  replacer_->Remove(fid);

  // Reset + free
  frame->Reset();
  free_frames_.push_back(fid);

  disk_scheduler_->DeallocatePage(page_id);
  return true;
}



/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are three main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type)
    -> std::optional<WritePageGuard> {
  bool need_flush = false;
  page_id_t flush_pid = INVALID_PAGE_ID;
  std::vector<char> flush_buf;

  bool need_read = false;

  std::shared_ptr<FrameHeader> frame;
  frame_id_t fid = INVALID_FRAME_ID;

  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);

    // Case 1: hit
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
      fid = it->second;
      frame = frames_[fid];

      frame->pin_count_.fetch_add(1);
      replacer_->RecordAccess(fid, page_id, access_type);
      replacer_->SetEvictable(fid, false);

      lock.unlock();
      return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_, false);
    }

    // Case 2: pick frame (free or evict)
    if (!free_frames_.empty()) {
      fid = free_frames_.front();
      free_frames_.pop_front();
      frame = frames_[fid];
      frame->rwlatch_.lock();
    } else {
      auto victim_opt = replacer_->Evict();
      if (!victim_opt.has_value()) {
        return std::nullopt;
      }
      fid = victim_opt.value();
      frame = frames_[fid];

      // Lock frame under bpm latch BEFORE inspecting/copying data.
      frame->rwlatch_.lock();

      page_id_t victim_pid = frame->page_id_;
      BUSTUB_ENSURE(victim_pid != INVALID_PAGE_ID, "Evicted frame had no page id");
      page_table_.erase(victim_pid);

      if (frame->is_dirty_) {
        need_flush = true;
        flush_pid = victim_pid;
        flush_buf.assign(frame->GetData(), frame->GetData() + BUSTUB_PAGE_SIZE);
      }
    }

    frame->Reset();
    frame->page_id_ = page_id;
    page_table_[page_id] = fid;

    frame->pin_count_.store(1);
    frame->is_dirty_ = false;

    replacer_->RecordAccess(fid, page_id, access_type);
    replacer_->SetEvictable(fid, false);

    need_read = true;
  }

  if (need_flush) {
    DiskRequest req;
    req.is_write_ = true;
    req.data_ = flush_buf.data();
    req.page_id_ = flush_pid;

    auto promise = disk_scheduler_->CreatePromise();
    auto fut = promise.get_future();
    req.callback_ = std::move(promise);

    std::vector<DiskRequest> requests;
    requests.push_back(std::move(req));
    disk_scheduler_->Schedule(requests);
    (void)fut.get();
  }

  if (need_read) {
    DiskRequest req;
    req.is_write_ = false;
    req.data_ = frame->GetDataMut();
    req.page_id_ = page_id;

    auto promise = disk_scheduler_->CreatePromise();
    auto fut = promise.get_future();
    req.callback_ = std::move(promise);

    std::vector<DiskRequest> requests;
    requests.push_back(std::move(req));
    disk_scheduler_->Schedule(requests);
    (void)fut.get();
  }

  // We still hold the write latch here.
  return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_, true);
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type)
    -> std::optional<ReadPageGuard> {
  bool need_flush = false;
  page_id_t flush_pid = INVALID_PAGE_ID;
  std::vector<char> flush_buf;

  std::shared_ptr<FrameHeader> frame;
  frame_id_t fid = INVALID_FRAME_ID;

  // -----------------------
  // 1) Try hit / choose frame under bpm_latch_
  // -----------------------
  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);

    // Case 1: HIT
    if (auto it = page_table_.find(page_id); it != page_table_.end()) {
      fid = it->second;
      frame = frames_[fid];

      frame->pin_count_.fetch_add(1);
      replacer_->RecordAccess(fid, page_id, access_type);
      replacer_->SetEvictable(fid, false);

      lock.unlock();
      // Guard will take shared latch itself.
      return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_, /*already_latched=*/false);
    }

    // Case 2: MISS -> choose a frame (free or evict)
    if (!free_frames_.empty()) {
      fid = free_frames_.front();
      free_frames_.pop_front();
      frame = frames_[fid];
      frame->rwlatch_.lock();
    } else {
      auto victim_opt = replacer_->Evict();
      if (!victim_opt.has_value()) {
        return std::nullopt;
      }
      fid = victim_opt.value();
      frame = frames_[fid];
      frame->rwlatch_.lock();

      // Find victim pid + remove old mapping
      page_id_t victim_pid = frame->page_id_;
      BUSTUB_ENSURE(victim_pid != INVALID_PAGE_ID, "Evicted frame had no page id");
      page_table_.erase(victim_pid);

      // If dirty, copy victim bytes now; flush later without holding locks.
      if (frame->is_dirty_) {
        need_flush = true;
        flush_pid = victim_pid;
        flush_buf.assign(frame->GetData(), frame->GetData() + BUSTUB_PAGE_SIZE);
        // Optional: mark clean since we're logically going to flush it
        // frame->is_dirty_ = false;
      }
    }
  

    // Re-purpose frame for requested page and publish mapping
    frame->Reset();
    frame->page_id_ = page_id;
    page_table_[page_id] = fid;

    frame->pin_count_.store(1);
    frame->is_dirty_ = false;

    replacer_->RecordAccess(fid, page_id, access_type);
    replacer_->SetEvictable(fid, false);

    // Release bpm latch; keep frame exclusively latched across I/O
  }

  // -----------------------
  // 2) Flush victim outside bpm_latch_ (no frame lock needed; using copied buffer)
  // -----------------------
  if (need_flush) {
    DiskRequest req;
    req.is_write_ = true;
    req.data_ = flush_buf.data();
    req.page_id_ = flush_pid;

    auto promise = disk_scheduler_->CreatePromise();
    auto fut = promise.get_future();
    req.callback_ = std::move(promise);

    std::vector<DiskRequest> requests;
    requests.push_back(std::move(req));
    disk_scheduler_->Schedule(requests);
    (void)fut.get();
  }

  // -----------------------
  // 3) Read requested page into frame while holding exclusive frame latch
  // -----------------------
  {
    DiskRequest req;
    req.is_write_ = false;
    req.data_ = frame->GetDataMut();
    req.page_id_ = page_id;

    auto promise = disk_scheduler_->CreatePromise();
    auto fut = promise.get_future();
    req.callback_ = std::move(promise);

    std::vector<DiskRequest> requests;
    requests.push_back(std::move(req));
    disk_scheduler_->Schedule(requests);
    (void)fut.get();
  }

  // -----------------------
  // 4) Downgrade exclusive -> shared WITHOUT a race window:
  // reacquire bpm_latch_ so no other thread can find/act on this mapping during downgrade.
  // -----------------------
  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);
    frame->rwlatch_.unlock();        // drop exclusive
    frame->rwlatch_.lock_shared();   // acquire shared
  }

  // Return guard that DOES NOT re-lock (we already hold shared).
  return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_, /*already_latched=*/true);
}


/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool { 
  // take latch
  std::unique_lock<std::mutex> lock(*bpm_latch_);

  // if page not in page_table, return false
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    lock.unlock();
    return false;
  }

  // get frame id
  frame_id_t fid = it->second;

  // if page not dirty, return true
  if (!frames_[fid]->is_dirty_) {
    lock.unlock();
    return true;
  }

  // write to disk
  DiskRequest req;
  req.is_write_ = true;
  std::vector<char> flush_buf(frames_[fid]->GetData(), frames_[fid]->GetData() + BUSTUB_PAGE_SIZE);
  lock.unlock();
  req.data_ = flush_buf.data();
  req.page_id_ = page_id;

  auto promise = disk_scheduler_->CreatePromise();
  auto fut = promise.get_future();
  req.callback_ = std::move(promise);

  std::vector<DiskRequest> requests;
  requests.push_back(std::move(req));
  disk_scheduler_->Schedule(requests);
  (void)fut.get();

  // take latch again
  lock.lock();

  // set dirty false
  frames_[fid]->is_dirty_ = false;

  lock.unlock();

  // return true
  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  // var to store page data
  std::vector<char> flush_buf;

  // take global latch
  std::unique_lock<std::mutex> lock(*bpm_latch_);

  // if page not in page_table, return false
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  // get frame id
  frame_id_t fid = it->second;

  // lock frame latch
  frames_[fid]->rwlatch_.lock();

  // if page not dirty, return true
  if (!frames_[fid]->is_dirty_) {
    // unlock latches
    frames_[fid]->rwlatch_.unlock();
    lock.unlock();
    // return true
    return true;
  }

  // copy over data
  flush_buf.assign(frames_[fid]->GetData(), frames_[fid]->GetData() + BUSTUB_PAGE_SIZE);
  frames_[fid]->is_dirty_ = false;  // we are logically flushing it now
  
  // release latches
  frames_[fid]->rwlatch_.unlock();
  lock.unlock();

  // write to disk
  DiskRequest req;
  req.is_write_ = true;
  req.data_ = flush_buf.data();
  req.page_id_ = page_id;
  auto promise = disk_scheduler_->CreatePromise();
  auto fut = promise.get_future();
  req.callback_ = std::move(promise);
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(req));
  disk_scheduler_->Schedule(requests);
  (void)fut.get();

  // return true
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 */
void BufferPoolManager::FlushAllPagesUnsafe() { 
  std::vector<page_id_t> pids;
  {
    std::scoped_lock<std::mutex> lk(*bpm_latch_);
    pids.reserve(page_table_.size());
    for (auto &kv : page_table_) {
      pids.push_back(kv.first);
    }
  }
  for (auto pid : pids) {
    FlushPageUnsafe(pid);
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 */
void BufferPoolManager::FlushAllPages() { 
  std::vector<page_id_t> pids;
  {
    std::scoped_lock<std::mutex> lk(*bpm_latch_);
    pids.reserve(page_table_.size());
    for (auto &kv : page_table_) {
      pids.push_back(kv.first);
    }
  }
  for (auto pid : pids) {
    FlushPage(pid);
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock<std::mutex> lk(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }

  const frame_id_t fid = it->second;
  return frames_[fid]->pin_count_.load();
}

}  // namespace bustub
