// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "aligned_file_reader.h"
#include "aio_context_pool.h"

// PASTEL: custom batch modified
#include <liburing.h>
#include <linux/nvme_ioctl.h>
#include <algorithm>
#include <linux/fs.h>
#include <linux/fiemap.h>
#include <sys/ioctl.h>
#undef BLOCK_SIZE // defined in linux/fs.h, causes conflicts in PQFlashIndex
// end


class LinuxAlignedFileReader : public AlignedFileReader {
private:
  uint64_t     file_sz;
  FileHandle   file_desc;
  io_context_t bad_ctx = (io_context_t) -1;
  std::shared_ptr<AioContextPool> ctx_pool_;

  // struct io_uring ring;
  std::shared_ptr<IOUringPool> uring_pool_;

 public:
  LinuxAlignedFileReader();
  ~LinuxAlignedFileReader();

  io_context_t get_ctx() override {
    return ctx_pool_->pop();
  }

  void put_ctx(io_context_t ctx) override {
    ctx_pool_->push(ctx);
  }

  struct io_uring* get_uring() {
    return uring_pool_->pop();
  }

  void put_uring(struct io_uring* ring) {
    uring_pool_->push(ring);
  }

  // Open & close ops
  // Blocking calls
  void open(const std::string &fname) override;
  void close() override;

  // process batch of aligned requests in parallel
  // NOTE :: blocking call
  void read(std::vector<AlignedRead> &read_reqs,
    io_context_t &ctx, struct io_uring *&ring, bool async = false) override;

  // async reads
  void get_submitted_req (io_context_t &ctx, size_t n_ops) override;
  void submit_req(io_context_t &ctx, std::vector<AlignedRead> &read_reqs) override;
};
