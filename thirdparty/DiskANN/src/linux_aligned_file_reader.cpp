// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "diskann/linux_aligned_file_reader.h"
#include <libaio.h>

#include <cassert>
#include <vector>
#include <cstdio>
#include <iostream>
#include <sstream>
#include "tsl/robin_map.h"
#include "diskann/utils.h"

namespace {
  static constexpr uint64_t n_retries = 10;
  static constexpr uint64_t PAGE_SIZE = 4096;
  static constexpr uint64_t pages_per_cmd = 8;
  static constexpr uint64_t maxnr_cap = 128;

  typedef struct io_event io_event_t;
  typedef struct iocb     iocb_t;

  void execute_io(io_context_t ctx, uint64_t maxnr_, int fd,
                  const std::vector<AlignedRead> &read_reqs, struct io_uring *&ring) {
#ifdef DEBUG
    for (auto &req : read_reqs) {
      assert(IS_ALIGNED(req.len, 512));
      // std::cout << "request:"<<req.offset<<":"<<req.len << std::endl;
      assert(IS_ALIGNED(req.offset, 512));
      assert(IS_ALIGNED(req.buf, 512));
      // assert(malloc_usable_size(req.buf) >= req.len);
    }
#endif

  const int queue_size = maxnr_cap;
  const int maxnr = std::min(maxnr_cap, maxnr_);
  // the size of actual array in ring.sq.sqes is a power of 2
  const int actual_queue_size = [](int q){int r=1; while(r<q) r<<=1; return r;}(queue_size);
  const int actual_queue_mask = actual_queue_size - 1;

  int reqs_size = read_reqs.size();
  int n_iters = (reqs_size + maxnr - 1) / maxnr;
  int reqs_done = 0;

  for (int iter=0; iter<n_iters; iter++){

    int real_n_ops = std::min((int)maxnr, reqs_size - reqs_done);
    // using 
    for(int i=0; i<real_n_ops; i++){
      struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
      if(sqe == nullptr){
        std::stringstream err;
        err << "io_uring_get_sqe: " << strerror(-errno);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
      }
      io_uring_prep_read(sqe, fd, read_reqs[reqs_done + i].buf, read_reqs[reqs_done + i].len, read_reqs[reqs_done + i].offset);
    }

    int ret = io_uring_submit(ring);
    if(ret < 0){
      std::stringstream err;
      err << "io_uring_submit: " << strerror(-ret);
      throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    struct io_uring_cqe *cqe;
    ret = 0;
    for(int i=0; i<real_n_ops; i++){
      ret=io_uring_wait_cqe(ring, &cqe);
      if (ret < 0) {
        std::stringstream err;
        err << "io_uring_wait_cqe: " << strerror(-ret);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
      }
      io_uring_cqe_seen(ring, cqe);
    }
    reqs_done += real_n_ops;
  }

  }

}  // namespace

LinuxAlignedFileReader::LinuxAlignedFileReader() {
  this->file_desc = -1;
  this->ctx_pool_ = AioContextPool::GetGlobalAioPool();
  this->uring_pool_ = IOUringPool::GetGlobalIOUringPool();
}

LinuxAlignedFileReader::~LinuxAlignedFileReader() {
  int64_t ret;
  // check to make sure file_desc is closed
  ret = ::fcntl(this->file_desc, F_GETFD);
  if (ret == -1) {
    if (errno != EBADF) {
      std::cerr << "close() not called" << std::endl;
      // close file desc
      ret = ::close(this->file_desc);
      // error checks
      if (ret == -1) {
        std::cerr << "close() failed; returned " << ret << ", errno=" << errno
                  << ":" << ::strerror(errno) << std::endl;
      }
    }
  }
}

void LinuxAlignedFileReader::open(const std::string &fname) {
  int flags = O_RDONLY | O_LARGEFILE;
  this->file_desc = ::open(fname.c_str(), flags);
  // error checks
  assert(this->file_desc != -1);
  LOG_KNOWHERE_INFO_ << "Opened file : " << fname;
  // ring initialized in LinuxAlignedFileReader::LinuxAlignedFileReader
}

void LinuxAlignedFileReader::close() {
  //  int64_t ret;

  // check to make sure file_desc is closed
  ::fcntl(this->file_desc, F_GETFD);
  //  assert(ret != -1);

  ::close(this->file_desc);
  //  assert(ret != -1);
}

void LinuxAlignedFileReader::read(std::vector<AlignedRead> &read_reqs,
                                  io_context_t &ctx, struct io_uring *&ring, bool async) {
  if (async == true) {
    diskann::cout << "Async currently not supported in linux." << std::endl;
  }
  assert(this->file_desc != -1);

  // not using outside function because class members are used
  execute_io(ctx, this->ctx_pool_->max_events_per_ctx(), this->file_desc,
             read_reqs, ring);

}

void LinuxAlignedFileReader::submit_req(io_context_t             &ctx,
                                        std::vector<AlignedRead> &read_reqs) {
  const auto maxnr = this->ctx_pool_->max_events_per_ctx();
  if (read_reqs.size() > maxnr) {
    std::stringstream err;
    err << "Async does not support number of read requests ("
        << read_reqs.size() << ") exceeds max number of events per context ("
        << maxnr << ")";
    throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
  }
  const auto n_ops = read_reqs.size();
  const int  fd = this->file_desc;

  std::vector<iocb_t *>    cbs(n_ops, nullptr);
  std::vector<struct iocb> cb(n_ops);
  for (size_t j = 0; j < n_ops; j++) {
    io_prep_pread(cb.data() + j, fd, read_reqs[j].buf, read_reqs[j].len,
                  read_reqs[j].offset);
  }
  for (uint64_t i = 0; i < n_ops; i++) {
    cbs[i] = cb.data() + i;
  }

  int64_t ret;
  uint64_t num_submitted = 0, submit_retry = 0;
  while (num_submitted < n_ops) {
    while ((ret = io_submit(ctx, n_ops - num_submitted,
                            cbs.data() + num_submitted)) < 0) {
      if (-ret != EINTR) {
        std::stringstream err;
        err << "Unknown error occur in io_submit, errno: " << -ret << ", "
            << strerror(-ret);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
      }
    }
    num_submitted += ret;
    if (num_submitted < n_ops) {
      submit_retry++;
      if (submit_retry <= n_retries) {
        LOG(WARNING) << "io_submit() failed; submit: " << num_submitted
                     << ", expected: " << n_ops << ", retry: " << submit_retry;
      } else {
        std::stringstream err;
        err << "io_submit failed after retried " << n_retries << " times";
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
      }
    }
  }
}

void LinuxAlignedFileReader::get_submitted_req(io_context_t &ctx, size_t n_ops) {
  if (n_ops > this->ctx_pool_->max_events_per_ctx()) {
    std::stringstream err;
    err << "Async does not support getting number of read requests (" << n_ops
        << ") exceeds max number of events per context ("
        << this->ctx_pool_->max_events_per_ctx() << ")";
    throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
  }

  int64_t ret;
  uint64_t                 num_read = 0, read_retry = 0;
  std::vector<io_event_t> evts(n_ops);
  while (num_read < n_ops) {
    while ((ret = io_getevents(ctx, n_ops - num_read, n_ops - num_read,
                               evts.data() + num_read, nullptr)) < 0) {
      if (-ret != EINTR) {
        std::stringstream err;
        err << "Unknown error occur in io_getevents, errno: " << -ret << ", "
            << strerror(-ret);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
      }
    }
    num_read += ret;
    if (num_read < n_ops) {
      read_retry++;
      if (read_retry <= n_retries) {
        LOG(WARNING) << "io_getevents() failed; read: " << num_read
                     << ", expected: " << n_ops << ", retry: " << read_retry;
      } else {
        std::stringstream err;
        err << "io_getevents failed after retried " << n_retries << " times";
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
      }
    }
  }
}