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

struct Extent {
    uint64_t logical_start;
    uint64_t physical_start;
    uint64_t length;
};

class LinuxAlignedFileReader : public AlignedFileReader {
private:
  uint64_t     file_sz;
  FileHandle   file_desc;
  io_context_t bad_ctx = (io_context_t) -1;
  std::shared_ptr<AioContextPool> ctx_pool_;

  FileHandle   disk_fd;
  uint64_t     partition_start;
  uint64_t     logical_block_size;
  
  // struct io_uring ring;
  std::shared_ptr<IOUringPool> uring_pool_;
  
  // filefrag extents to tree and search
    class IntervalTree {
    public:
      IntervalTree(const std::vector<Extent>& extents) { root = buildIntervalTree(extents, 0, extents.size() - 1); }
      ~IntervalTree() { deleteIntervalTree(root); }
          // search for the physical offset of the logical offset
          // returns LBA *within partition* (not in the entire disk) in bytes
      uint64_t search(uint64_t logical_off) const { return searchIntervalTree(root, logical_off); };
    private:
      class IntervalNode {
        public:
          IntervalNode(const Extent& e) : extent(e), max(e.logical_start + e.length - 1), left(0), right(0) {}
          Extent extent;
          uint64_t max; // maximum logical_end in this subtree
          IntervalNode *left;
          IntervalNode *right;
        }; // class IntervalNode (nested)

        IntervalNode* buildIntervalTree(const std::vector<Extent>& sorted_extents, int start, int end){
          if (start > end) {
            return 0;
          }

          int mid = start + (end - start) / 2;
          IntervalNode* node = new IntervalNode(sorted_extents[mid]);
          node->left = buildIntervalTree(sorted_extents, start, mid - 1);
          node->right = buildIntervalTree(sorted_extents, mid + 1, end);

          // Update the max value
          uint64_t left_max = node->left ? node->left->max : 0;
          uint64_t right_max = node->right ? node->right->max : 0;
          node->max = std::max({node->extent.logical_start + node->extent.length - 1, left_max, right_max});

          return node;
        }; // buildIntervalTree

        void deleteIntervalTree(IntervalNode* node){
          if (!node) return;
          deleteIntervalTree(node->left);
          deleteIntervalTree(node->right);
          delete node;
        }; // deleteIntervalTree

        uint64_t searchIntervalTree (const IntervalNode* node, uint64_t logical_off) const {
          if (!node) {
            return -1;
          }
          if (node->extent.logical_start <= logical_off && logical_off < node->extent.logical_start + node->extent.length) {
            return node->extent.physical_start + (logical_off - node->extent.logical_start);
          }

          if (node->left && logical_off <= node->left->max) {
            return searchIntervalTree(node->left, logical_off);
          } else {
            return searchIntervalTree(node->right, logical_off);
          }
        }; // searchIntervalTree
        IntervalNode* root;
  };

  IntervalTree* tree;

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
