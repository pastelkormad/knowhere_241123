#pragma once

#include <mutex>
#include <queue>
#include <libaio.h>
#include <liburing.h>
#include <condition_variable>
#include "utils.h"
#include "concurrent_queue.h"
#include "aux_utils.h"
#include "diskann/ann_exception.h"

constexpr size_t default_max_nr = 65536;
constexpr size_t default_max_events = diskann::MAX_N_SECTOR_READS / 2;
constexpr size_t default_max_queue_size = diskann::MAX_N_SECTOR_READS / 2;
constexpr size_t default_pool_size = default_max_nr / default_max_events;


class AioContextPool {
 public:
  AioContextPool(const AioContextPool&) = delete;

  AioContextPool& operator=(const AioContextPool&) = delete;

  AioContextPool(AioContextPool&&) noexcept = delete;

  AioContextPool& operator==(AioContextPool&&) noexcept = delete;

  size_t max_events_per_ctx() {
    return max_events_;
  }

  void push(io_context_t ctx) {
    {
      std::scoped_lock lk(ctx_mtx_);
      ctx_q_.push(ctx);
    }
    ctx_cv_.notify_one();
  }

  io_context_t pop() {
    std::unique_lock lk(ctx_mtx_);
    if (stop_) {
      return nullptr;
    }
    ctx_cv_.wait(lk, [this] { return ctx_q_.size(); });
    if (stop_) {
      return nullptr;
    }
    auto ret = ctx_q_.front();
    ctx_q_.pop();
    return ret;
  }

  static bool InitGlobalAioPool(size_t num_ctx, size_t max_events) {
    if (num_ctx <= 0) {
      LOG(ERROR) << "num_ctx should be bigger than 0";
      return false;
    }
    if (max_events > default_max_events) {
      LOG(ERROR) << "max_events " << max_events << " should not be larger than " << default_max_events;
      return false;
    }
    if (global_aio_pool_size == 0) {
      std::scoped_lock lk(global_aio_pool_mut);
      if (global_aio_pool_size == 0) {
        global_aio_pool_size = num_ctx;
        global_aio_max_events = max_events;
        return true;
      }
    }
    LOG(WARNING)
        << "Global AioContextPool has already been inialized with context num: "
        << global_aio_pool_size;
        return true;
  }

  static std::shared_ptr<AioContextPool> GetGlobalAioPool() {
    if (global_aio_pool_size == 0) {
      std::scoped_lock lk(global_aio_pool_mut);
      if (global_aio_pool_size == 0) {
        global_aio_pool_size = default_pool_size;
        global_aio_max_events = default_max_events;
        LOG(WARNING)
            << "Global AioContextPool has not been inialized yet, init "
               "it now with context num: "
            << global_aio_pool_size;
      }
    }
    static auto pool = std::shared_ptr<AioContextPool>(
        new AioContextPool(global_aio_pool_size, global_aio_max_events));
    return pool;
  }

  ~AioContextPool() {
    stop_ = true;
    for (auto ctx : ctx_bak_) {
      io_destroy(ctx);
    }
    ctx_cv_.notify_all();
  }

 private:
  std::vector<io_context_t> ctx_bak_;
  std::queue<io_context_t>  ctx_q_;
  std::mutex                ctx_mtx_;
  std::condition_variable   ctx_cv_;
  bool                      stop_ = false;
  size_t                    num_ctx_;
  size_t                    max_events_;
  inline static size_t           global_aio_pool_size = 0;
  inline static size_t           global_aio_max_events = 0;
  inline static std::mutex       global_aio_pool_mut;

  AioContextPool(size_t num_ctx, size_t max_events)
      : num_ctx_(num_ctx), max_events_(max_events) {
    for (size_t i = 0; i < num_ctx_; ++i) {
      io_context_t ctx = 0;
      int          ret = -1;
      for (int retry = 0; (ret = io_setup(max_events, &ctx)) != 0 && retry < 5;
           ++retry) {
        if (-ret != EAGAIN) {
          LOG(ERROR) << "Unknown error occur in io_setup, errno: " << -ret
                     << ", " << strerror(-ret);
        }
      }
      if (ret != 0) {
        LOG(ERROR) << "io_setup() failed; returned " << ret
                   << ", errno=" << -ret << ":" << ::strerror(-ret);
      } else {
        LOG_KNOWHERE_DEBUG_ << "allocating ctx: " << ctx;
        ctx_q_.push(ctx);
        ctx_bak_.push_back(ctx);
      }
    }
  }
};

// created to manage io_uring instances, like AioContextPool, but for liunx_aligned_file_reader
class IOUringPool {
 public:
  IOUringPool(const IOUringPool&) = delete;

  IOUringPool& operator=(const IOUringPool&) = delete;

  IOUringPool(IOUringPool&&) noexcept = delete;

  IOUringPool& operator==(IOUringPool&&) noexcept = delete;

  size_t max_queue_size_per_ring() {
    return max_queue_size_;
  }

  void push(struct io_uring* ring) {
    {
      std::scoped_lock lk(uring_mtx_);
      uring_q_.push(ring);
    }
    uring_cv_.notify_one();
  }

  struct io_uring* pop() {
    std::unique_lock lk(uring_mtx_);
    if (stop_) {
      return nullptr;
    }
    uring_cv_.wait(lk, [this] { return uring_q_.size(); });
    if (stop_) {
      return nullptr;
    }
    auto ret = uring_q_.front();
    uring_q_.pop();
    return ret;
  }

  static bool InitGlobalUringPool(size_t num_urings, size_t max_queue_size) {
    if (num_urings <= 0) {
      LOG(ERROR) << "num_urings should be bigger than 0";
      return false;
    }
    if (max_queue_size > default_max_queue_size) {
      LOG(ERROR) << "max_queue_size " << max_queue_size << " should not be larger than " << default_max_queue_size;
      return false;
    }
    if (global_uring_pool_size == 0) {
      std::scoped_lock lk(global_uring_pool_mut);
      if (global_uring_pool_size == 0) {
        global_uring_pool_size = num_urings;
        global_uring_max_queue_size = max_queue_size;
        return true;
      }
    }
    LOG(WARNING)
        << "Global IOUringPool has already been inialized with uring num: "
        << global_uring_pool_size;
        return true;
  }

  static std::shared_ptr<IOUringPool> GetGlobalIOUringPool() {
    if (global_uring_pool_size == 0) {
      std::scoped_lock lk(global_uring_pool_mut);
      if (global_uring_pool_size == 0) {
        global_uring_pool_size = default_pool_size;
        global_uring_max_queue_size = default_max_queue_size;
        LOG(WARNING)
            << "Global IOUringPool has not been inialized yet, init "
               "it now with uring num: "
            << global_uring_pool_size;
      }
    }
    static auto pool = std::shared_ptr<IOUringPool>(
        new IOUringPool(global_uring_pool_size, global_uring_max_queue_size));
    return pool;
  }

  ~IOUringPool() {
    stop_ = true;
    for (auto uring : uring_bak_) {
      io_uring_queue_exit(uring);
      delete uring;
    }
    uring_cv_.notify_all();
  }

 private:
  std::vector<struct io_uring*> uring_bak_;
  std::queue<struct io_uring*>  uring_q_;
  std::mutex                uring_mtx_;
  std::condition_variable   uring_cv_;
  bool                      stop_ = false;
  size_t                    num_uring_;
  size_t                    max_queue_size_;
  inline static size_t           global_uring_pool_size = 0;
  inline static size_t           global_uring_max_queue_size = 0;
  inline static std::mutex       global_uring_pool_mut;
  static constexpr unsigned int queue_init_flags = IORING_SETUP_SQE128 | IORING_SETUP_CQE32; 
  IOUringPool(size_t num_uring, size_t max_queue_size)
      : num_uring_(num_uring), max_queue_size_(max_queue_size) {
    for (size_t i = 0; i < num_uring_; ++i) {
      struct io_uring* uring = new struct io_uring;
      int          ret = -1;
      for (int retry = 0; (ret = io_uring_queue_init(max_queue_size, uring, queue_init_flags)) != 0 && retry < 5;
           ++retry) {
        if (-ret != EAGAIN) {
          LOG(ERROR) << "Unknown error occur in io_uring_queue_init, errno: " << -ret
                     << ", " << strerror(-ret);
        }
      }
      if (ret != 0) {
        LOG(ERROR) << "io_setup() failed; returned " << ret
                   << ", errno=" << -ret << ":" << ::strerror(-ret);
      } else {
        LOG_KNOWHERE_DEBUG_ << "allocating uring: " << uring;
        uring_q_.push(uring);
        uring_bak_.push_back(uring);
      }
    }
  }
};
