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

  void execute_io(io_context_t ctx, uint64_t maxnr, int fd,
                  const std::vector<AlignedRead> &read_reqs) {
#ifdef DEBUG
    for (auto &req : read_reqs) {
      assert(IS_ALIGNED(req.len, 512));
      // std::cout << "request:"<<req.offset<<":"<<req.len << std::endl;
      assert(IS_ALIGNED(req.offset, 512));
      assert(IS_ALIGNED(req.buf, 512));
      // assert(malloc_usable_size(req.buf) >= req.len);
    }
#endif

    // break-up requests into chunks of size maxnr each
    int64_t n_iters = ROUND_UP(read_reqs.size(), maxnr) / maxnr;
    for (int64_t iter = 0; iter < n_iters; iter++) {
      int64_t n_ops = std::min(read_reqs.size() - (iter * maxnr), maxnr);
      std::vector<iocb_t *>    cbs(n_ops, nullptr);
      std::vector<io_event_t>  evts(n_ops);
      std::vector<struct iocb> cb(n_ops);
      for (int64_t j = 0; j < n_ops; j++) {
        io_prep_pread(cb.data() + j, fd, read_reqs[j + iter * maxnr].buf,
                      read_reqs[j + iter * maxnr].len,
                      read_reqs[j + iter * maxnr].offset);
      }

      // initialize `cbs` using `cb` array
      //

      for (auto i = 0; i < n_ops; i++) {
        cbs[i] = cb.data() + i;
      }

      int64_t ret;
      int64_t num_submitted = 0;
      uint64_t submit_retry = 0;
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
                         << ", expected: " << n_ops
                         << ", retry: " << submit_retry;
          } else {
            std::stringstream err;
            err << "io_submit failed after retried " << n_retries << " times";
            throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__,
                                        __LINE__);
          }
        }
      }

      int64_t num_read = 0;
      uint64_t read_retry = 0;
      while (num_read < n_ops) {
        while ((ret = io_getevents(ctx, n_ops - num_read, n_ops - num_read,
                                   evts.data() + num_read, nullptr)) < 0) {
          if (-ret != EINTR) {
            std::stringstream err;
            err << "Unknown error occur in io_getevents, errno: " << -ret
                << ", " << strerror(-ret);
            throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__,
                                        __LINE__);
          }
        }
        num_read += ret;
        if (num_read < n_ops) {
          read_retry++;
          if (read_retry <= n_retries) {
            LOG(WARNING) << "io_getevents() failed; read: " << num_read
                         << ", expected: " << n_ops
                         << ", retry: " << read_retry;
          } else {
            std::stringstream err;
            err << "io_getevents failed after retried " << n_retries
                << " times";
            throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__,
                                        __LINE__);
          }
        }
      }
      // disabled since req.buf could be an offset into another buf
      /*
      for (auto &req : read_reqs) {
        // corruption check
        assert(malloc_usable_size(req.buf) >= req.len);
      }
      */
    }
  }


  // PASTEL: custom batch modified
  
struct io_uring_sqe_128 {
	__u8	opcode;		/* type of operation for this sqe */
	__u8	flags;		/* IOSQE_ flags */
	__u16	ioprio;		/* ioprio for the request */
	__s32	fd;		/* file descriptor to do IO on */
	union {
		__u64	off;	/* offset into file */
		__u64	addr2;
		struct {
			__u32	cmd_op;
			__u32	__pad1;
		};
	};
	union {
		__u64	addr;	/* pointer to buffer or iovecs */
		__u64	splice_off_in;
	};
	__u32	len;		/* buffer size or number of iovecs */
	union {
		__kernel_rwf_t	rw_flags;
		__u32		fsync_flags;
		__u16		poll_events;	/* compatibility */
		__u32		poll32_events;	/* word-reversed for BE */
		__u32		sync_range_flags;
		__u32		msg_flags;
		__u32		timeout_flags;
		__u32		accept_flags;
		__u32		cancel_flags;
		__u32		open_flags;
		__u32		statx_flags;
		__u32		fadvise_advice;
		__u32		splice_flags;
		__u32		rename_flags;
		__u32		unlink_flags;
		__u32		hardlink_flags;
		__u32		xattr_flags;
		__u32		msg_ring_flags;
		__u32		uring_cmd_flags;
	};
	__u64	user_data;	/* data to be passed back at completion time */
	/* pack this to avoid bogus arm OABI complaints */
	union {
		/* index into fixed buffers, if used */
		__u16	buf_index;
		/* for grouped buffer selection */
		__u16	buf_group;
	} __attribute__((packed));
	/* personality to use, if used */
	__u16	personality;
	union {
		__s32	splice_fd_in;
		__u32	file_index;
		struct {
			__u16	addr_len;
			__u16	__pad3[1];
		};
	};
	union {
		struct {
			__u64	addr3;
			__u64	__pad2[1];
		};
		/*
		 * If the ring is initialized with IORING_SETUP_SQE128, then
		 * this field is used for 80 bytes of arbitrary command data
		 */
		nvme_uring_cmd cmd;
	};
    __u8 __pad4[8];
};

// only merge pages when buffers are adjacent in memory space
// otherwise, create a normal read command
int cmd_assign_pgs(struct nvme_uring_cmd *cmd, const struct AlignedRead* reads,
int count, unsigned int logical_block_size = 512){
    if (count <= 0 || count > 8) {
        std::stringstream err;
        err << "cmd_assign_pgs: count should be in [1, 8]";
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    cmd->nsid = 1;
	static constexpr unsigned int unused_mark = 0xffffffff; // 0xffffffff is used as a flag for unused
	int can_be_batched_up_to = 0; // end inclusive
	for(int i=1; i<count; i++){
    if(reads[0].len != PAGE_SIZE) break;
		if(((char*)(reads[i].buf) == (char*)(reads[i-1].buf) + reads[i-1].len) &&
		(reads[i].len == PAGE_SIZE))
			can_be_batched_up_to++;
		else break;
	}
	
    cmd->opcode = (can_be_batched_up_to == 0) ? 2 : 144; // read/write or custom batch command
    cmd->addr = (unsigned long long)(reads[0].buf);
    cmd->data_len = (can_be_batched_up_to == 0) ? reads[0].len : PAGE_SIZE * (can_be_batched_up_to + 1);

	if(can_be_batched_up_to == 0){
    	*((unsigned long long*)(&(cmd->cdw10))) = (reads[0].offset) / logical_block_size;
		cmd->cdw12 = ((reads[0].len / logical_block_size) - 1) & 0xFFFF;
		return 1;
	}
    // cdws for custom batch command (opcode 144) is not contiguous
	// logical_block_size does not matter for custom batch command
    for(int i=0; i<2; i++){
        *((unsigned int*)(&(cmd->cdw2)) + i) = (i <= can_be_batched_up_to) ? (reads[i].offset / PAGE_SIZE) : unused_mark;
    }
    // as i starts from 2, base address is 8 bytes ahead of cdw10 (actual base address) 
    for(int i=2; i<8; i++){
        *((unsigned int*)(&(cmd->metadata_len)) + i) = (i <= can_be_batched_up_to) ? (reads[i].offset / PAGE_SIZE) : unused_mark;
    }

	return can_be_batched_up_to + 1;
}

// Get the starting offset of the partition in which the file resides, in blocks, and size of logical blocks
int GetPartitionOffset(const int fd, uint64_t* ret_off, uint64_t* ret_logical_block_size) {
    if(!ret_off) return 1;
    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::stringstream err;
        err << "Error getting file stats: " << strerror(errno);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    dev_t dev = st.st_dev;
    dev_t major = dev >> 8;
    dev_t minor = dev & 255UL;
    std::string devpath = "/sys/dev/block/" + std::to_string(major) + ":" + std::to_string(minor);
    // if devpath/partition exists, then it is a partition of a disk. Otherwise, it is a whole disk.
    std::string partition = devpath + "/partition";
    if(access(partition.c_str(), F_OK)) {
		// access returns nonzero if partition file does not exist, i.e. dev is a whole disk
        *ret_off = 0;
		std::string logical_block_size_path = devpath + "/queue/logical_block_size";
		std::ifstream logical_block_size_file(logical_block_size_path);
		if(!logical_block_size_file.is_open()){
      std::stringstream err;
      err << "Error opening file: " << logical_block_size_path << " ... " << strerror(errno);
      throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
		}
		logical_block_size_file >> *ret_logical_block_size;
        return 0;
    }
    // get the partition offset: in devpath/start
    // partition offset is in units specified in devpath/../queue/logical_block_size (usually 512 bytes)
    else{
        std::string offset_path = devpath + "/start";
        std::ifstream offset_file(offset_path);
        uint64_t logical_block_size;
        std::string logical_block_size_path = devpath + "/../queue/logical_block_size";
        if(!offset_file.is_open()){
          std::stringstream err;
          err << "Error opening file: " << strerror(errno);
          throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
        }
        offset_file >> *ret_off;

        std::ifstream logical_block_size_file(logical_block_size_path);
        if(!logical_block_size_file.is_open()){
          std::stringstream err;
          err << "Error opening file: " << strerror(errno);
          throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
        }
        logical_block_size_file >> *ret_logical_block_size;
    }
    return 0;
}

// Get the starting offset of the partition in which the file resides, in blocks, and size of logical blocks
int GetPartitionOffset(const std::string& filename, uint64_t* ret_off, uint64_t* ret_logical_block_size) {
	int fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
    std::stringstream err;
    err << "Error opening file: " << strerror(errno);
    throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
	}
	int ret = GetPartitionOffset(fd, ret_off, ret_logical_block_size);
	close(fd);
	return ret;
}

int getFileExtents(const int fd, std::vector<Extent>* extentList){
    struct fiemap *fiemap;
    struct fiemap_extent *extents;

    // first call to only get the number of extents
    fiemap = static_cast<struct fiemap *>(calloc(1, sizeof(struct fiemap)));
    fiemap->fm_start = 0;
    fiemap->fm_length = FIEMAP_MAX_OFFSET;
    fiemap->fm_flags = FIEMAP_FLAG_SYNC;
    fiemap->fm_extent_count = 0;

    if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
        std::stringstream err;
        err << "Error in FIEMAP ioctl: " << strerror(errno);
        free(fiemap);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    unsigned int num_extents = fiemap->fm_mapped_extents;

    free(fiemap);
    // second call to get the extents, additionally allocate memory for the extents
    fiemap = static_cast<struct fiemap *>(calloc(1, sizeof(struct fiemap) +
                    num_extents * sizeof(struct fiemap_extent)));
    fiemap->fm_start = 0;
    fiemap->fm_length = FIEMAP_MAX_OFFSET;
    fiemap->fm_flags = FIEMAP_FLAG_SYNC;
    fiemap->fm_extent_count = num_extents;
    lseek64(fd, 0, SEEK_SET);
    if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0){
        std::stringstream err;
        err << "Error in FIEMAP ioctl: " << strerror(errno);
        free(fiemap);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    extents = &fiemap->fm_extents[0];
    for (unsigned int i = 0; i < fiemap->fm_mapped_extents; i++) {
        extentList->push_back({
            extents[i].fe_logical,
            extents[i].fe_physical,
            extents[i].fe_length
        });
    }

  free(fiemap);
	return 0;
}

int getFileExtents(const std::string& filename, std::vector<Extent>* extentList) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::stringstream err;
        err << "Error opening file: " << strerror(errno);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
	int ret = getFileExtents(fd, extentList);
    close(fd);
    return 0;
}

// Open file descriptor of the disk containing the file.
// Opens the whole disk by default, unless open_partition is set true, in which case it opens the partition.
int open_disk_fd(int file_fd, bool open_partition = false, int oflag = O_RDONLY, bool generic = false){
    struct stat st;
    if (fstat(file_fd, &st) < 0) {
        std::stringstream err; err << "Error getting file stats: " << strerror(errno);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    dev_t dev = st.st_dev;
    dev_t major = dev >> 8;
    dev_t minor = dev & 255UL;
    std::string devpath = "/dev/block/" + std::to_string(major) + ":" + std::to_string(minor);

    // open partition if partition is true (do not check whether this is a partition)
    int disk_fd;
    if(open_partition)     disk_fd = open(devpath.c_str(), oflag);
    // open disk if partition is false (should open the whole disk)
    else{
        std::string partition = "/sys" + devpath + "/partition";
        int is_partition = !(access(partition.c_str(), F_OK)); // access returns 0 if partition exists.
        if(is_partition){
            // find parent disk name
            // DEVNAME=nvmexnx in /sys/dev/block/major:minor/../uevent
            std::string diskname_path = "/sys" + devpath + "/../uevent";
            std::ifstream diskname_file(diskname_path);
            std::string diskname;
            if(!diskname_file.is_open()){
                std::stringstream err; err << "Error getting file: " << strerror(errno);
                throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
            }
            std::string line;
            while(std::getline(diskname_file, line)){
                if(line.find("DEVNAME=") != std::string::npos){
                    diskname = line.substr(strlen("DEVNAME="));
                    break;
                }
            }
			std::string full_diskname;
			if(!generic) full_diskname = "/dev/" + diskname;
			else{
				// /dev/ng*n* instead of /dev/nvme*n*
				if (diskname.substr(0, 4) == "nvme"){
					full_diskname = "/dev/ng" + diskname.substr(4);
				}
				else {
					std::stringstream err;
          err << "Error: requested generic diskname, but the device name does not start with nvme";
          throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
				}
			}
            disk_fd = open(full_diskname.c_str(), oflag);
        }
        // the partition is also the whole disk
        else{
			if (!generic) disk_fd = open(devpath.c_str(), oflag);
			else{
				std::string diskname_path = "/sys" + devpath + "/uevent";
				std::ifstream diskname_file(diskname_path);
				std::string diskname;
				if(!diskname_file.is_open()){
					std::stringstream err; err << "Error opening file: " << strerror(errno);
          throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
				}
				std::string line;
				while(std::getline(diskname_file, line)){
					if(line.find("DEVNAME=") != std::string::npos){
						diskname = line.substr(strlen("DEVNAME="));
						break;
					}
				}
				// /dev/ng*n* instead of /dev/nvme*n*
				if (diskname.substr(0, 4) == "nvme"){
					std::string full_diskname = "/dev/ng" + diskname.substr(4);
					disk_fd = open(full_diskname.c_str(), oflag);
				}
				else {
					std::stringstream err;
          err << "Error: requested generic diskname, but the device name does not start with nvme";
          throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
				}
			}
        }
    }
    if(disk_fd < 0){
        std::stringstream err;
        err << "Error opening disk: " << strerror(errno);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    return disk_fd;
}

}  // namespace

LinuxAlignedFileReader::LinuxAlignedFileReader() {
  this->file_desc = -1;
  this->ctx_pool_ = AioContextPool::GetGlobalAioPool();
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
  int flags = O_DIRECT | O_RDONLY | O_LARGEFILE;
  this->file_desc = ::open(fname.c_str(), flags);
  // error checks
  assert(this->file_desc != -1);
  LOG_KNOWHERE_DEBUG_ << "Opened file : " << fname;
  this->disk_fd = open_disk_fd(this->file_desc, false, O_RDONLY, true);
  if (this->disk_fd < 0) {
    std::stringstream err;
    err << "Error opening disk: " << strerror(errno);
    throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
  }
  int err = GetPartitionOffset(this->file_desc, &this->partition_start, &this->logical_block_size);
  if (err != 0) {
    std::stringstream err;
    err << "Error getting partition offset: " << strerror(errno);
    throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
  }
  LOG_KNOWHERE_DEBUG_ << "Partition offset: " << this->partition_start << ", Logical block size: " << this->logical_block_size;

  // get extents of the file
  std::vector<Extent> extents;
  getFileExtents(this->file_desc, &extents);
  this->tree = new IntervalTree(extents);

  // initialize ring
  io_uring_queue_init(maxnr_cap, &this->ring, IORING_SETUP_SQE128 | IORING_SETUP_CQE32);

}

void LinuxAlignedFileReader::close() {
  //  int64_t ret;

  // check to make sure file_desc is closed
  ::fcntl(this->file_desc, F_GETFD);
  //  assert(ret != -1);

  ::close(this->file_desc);
  //  assert(ret != -1);

  ::close(this->disk_fd);
  
  io_uring_queue_exit(&this->ring);
  delete this->tree;
}

void LinuxAlignedFileReader::read(std::vector<AlignedRead> &read_reqs,
                                  io_context_t &ctx, bool async) {
  if (async == true) {
    diskann::cout << "Async currently not supported in linux." << std::endl;
  }
  assert(this->file_desc != -1);

  // not using outside function because class members are used
  // execute_io(ctx, this->ctx_pool_->max_events_per_ctx(), this->file_desc,
  //            read_reqs);
  const int queue_size = maxnr_cap;
  const int maxnr = std::min(this->ctx_pool_->max_events_per_ctx(), maxnr_cap);
  // the size of actual array in ring.sq.sqes is a power of 2
  const int actual_queue_size = [](int q){int r=1; while(r<q) r<<=1; return r;}(queue_size);
  const int actual_queue_mask = actual_queue_size - 1;
  struct io_uring_sqe_128* sqes = (struct io_uring_sqe_128*)(ring.sq.sqes);
  memset(sqes, 0, sizeof(struct io_uring_sqe_128) * actual_queue_size);

	std::vector<AlignedRead> reqs_withdiskoffsets(read_reqs); // copy of read_reqs, convert to disk offsets

	int sq_position;
	int reqs_size = read_reqs.size();
	int n_iters = (reqs_size + maxnr - 1) / maxnr;
	int reqs_done = 0;

	// for each command in read_reqs, convert LPN offset to disk offsets using tree
	for(int i = 0; i < reqs_size; i++){
		reqs_withdiskoffsets[i].offset = tree->search(read_reqs[i].offset) + partition_start * logical_block_size;
	}

	for (int iter=0; iter<n_iters; iter++){
		int sq_position = ring.sq.sqe_tail;
		int real_n_ops = std::min((int)maxnr, reqs_size - reqs_done);
		int real_command_count = 0;
		for(int i=0; i<real_n_ops;){
			sqes[sq_position & actual_queue_mask].opcode = IORING_OP_URING_CMD;
			sqes[sq_position & actual_queue_mask].cmd_op = NVME_URING_CMD_IO;
			sqes[sq_position & actual_queue_mask].fd = disk_fd;
			int pm = cmd_assign_pgs(&(sqes[sq_position & actual_queue_mask].cmd),  // command to assign
			reqs_withdiskoffsets.data() + reqs_done + i, std::min((int)(pages_per_cmd), real_n_ops - i), logical_block_size);
			// return value = how many reads there are in the command
			if (pm == -1) {
        std::stringstream err;
        err << "cmd_assign_pgs: failed";
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
				// fprintf(stderr, "cmd_assign_pgs: failed\n");
				// return 1;
			}
			i += pm;
			sq_position++;
			real_command_count++;
		}

		*(ring.sq.ktail) += real_command_count;
		ring.sq.sqe_tail += real_command_count;

		int ret = io_uring_submit(&ring);
		if(ret < 0){
      std::stringstream err;
      err << "io_uring_submit: " << strerror(-ret);
      throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
			// fprintf(stderr, "io_uring_submit: %s\n", strerror(-ret));
			// return 1;
		}
		struct io_uring_cqe *cqe;
		ret = 0;
		for(int i=0; i<real_command_count; i++){
			ret=io_uring_wait_cqe(&ring, &cqe);
			if (ret < 0) {
        std::stringstream err;
        err << "io_uring_wait_cqe: " << strerror(-ret);
        throw diskann::ANNException(err.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
				// fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
				// return 1;
			}
			io_uring_cqe_seen(&ring, cqe);
		}
		reqs_done += real_n_ops;
	}
	return;

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