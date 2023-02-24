// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// REVIEW: DEBUG: TODO: FIXME: NOTE: SOLVE: PROGRESS: JH:
#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"


namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  /*----------------------------------*/
  std::vector<Output> outputs;
  std::vector<Output> outputs_hot;/*zewei_comp*/
  /*----------------------------------*/

  // State kept for output being generated
  WritableFile* outfile;
  /*----------------------------------*/
  TableBuilder* builder;
  TableBuilder* builder_hot;/*zewei_comp*/
  /*----------------------------------*/

  uint64_t total_bytes;
  /*---------------------------------------------*/
  Output* current_output() { return &outputs[outputs.size()-1]; }
  Output* current_output_hot() { return &outputs_hot[outputs_hot.size()-1]; } /*zewei_comp*/
  /*---------------------------------------------*/

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(nullptr),
        builder(nullptr),
	builder_hot(nullptr), /*zewei_comp*/
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
    // SOLVE: JH
  if (result.sst_type == kPmemSST) {
    switch (result.ds_type) {
      // DS_Option1: Skiplist
      case kSkiplist:
        result.pmem_skiplist = new PmemSkiplist*[NUM_OF_SKIPLIST_MANAGER];
        result.pmem_skiplist[0] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_0);
        result.pmem_skiplist[1] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_1);
        result.pmem_skiplist[2] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_2);
        result.pmem_skiplist[3] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_3);
        result.pmem_skiplist[4] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_4);
        result.pmem_skiplist[5] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_5);
        result.pmem_skiplist[6] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_6);
        result.pmem_skiplist[7] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_7);
        result.pmem_skiplist[8] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_8);
        result.pmem_skiplist[9] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_9);

        // result.pmem_skiplist[10] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_10);
        // result.pmem_skiplist[11] = new PmemSkiplist(SKIPLIST_MANAGER_PATH_11);
        
        // Initialize
        for (int i=0; i<NUM_OF_SKIPLIST_MANAGER; i++) {
          result.pmem_skiplist[i]->ClearAll();
        }

        // NOTE: FIXME: [190313] use this as cache iterator.. 
        // actual skiplist_cache has ordering problem on compaction
        // Smallest and largest key are invalid..
        result.pmem_internal_iterator = new PmemIterator*[NUM_OF_SKIPLIST_MANAGER];
        result.pmem_internal_iterator[0] = new PmemIterator(0, result.pmem_skiplist[0]);
        result.pmem_internal_iterator[1] = new PmemIterator(1, result.pmem_skiplist[1]);
        result.pmem_internal_iterator[2] = new PmemIterator(2, result.pmem_skiplist[2]);
        result.pmem_internal_iterator[3] = new PmemIterator(3, result.pmem_skiplist[3]);
        result.pmem_internal_iterator[4] = new PmemIterator(4, result.pmem_skiplist[4]);
        result.pmem_internal_iterator[5] = new PmemIterator(5, result.pmem_skiplist[5]);
        result.pmem_internal_iterator[6] = new PmemIterator(6, result.pmem_skiplist[6]);
        result.pmem_internal_iterator[7] = new PmemIterator(7, result.pmem_skiplist[7]);
        result.pmem_internal_iterator[8] = new PmemIterator(8, result.pmem_skiplist[8]);
        result.pmem_internal_iterator[9] = new PmemIterator(9, result.pmem_skiplist[9]);

        // result.pmem_internal_iterator[10] = new PmemIterator(10, result.pmem_skiplist[10]);
        // result.pmem_internal_iterator[11] = new PmemIterator(11, result.pmem_skiplist[11]);
        break;
      // DS_Option2: Hashmap
      case kHashmap:
        result.pmem_hashmap = new PmemHashmap*[NUM_OF_HASHMAP];
        result.pmem_hashmap[0] = new PmemHashmap(HASHMAP_PATH_0);
        result.pmem_hashmap[1] = new PmemHashmap(HASHMAP_PATH_1);
        result.pmem_hashmap[2] = new PmemHashmap(HASHMAP_PATH_2);
        result.pmem_hashmap[3] = new PmemHashmap(HASHMAP_PATH_3);
        result.pmem_hashmap[4] = new PmemHashmap(HASHMAP_PATH_4);
        result.pmem_hashmap[5] = new PmemHashmap(HASHMAP_PATH_5);
        result.pmem_hashmap[6] = new PmemHashmap(HASHMAP_PATH_6);
        result.pmem_hashmap[7] = new PmemHashmap(HASHMAP_PATH_7);
        result.pmem_hashmap[8] = new PmemHashmap(HASHMAP_PATH_8);
        result.pmem_hashmap[9] = new PmemHashmap(HASHMAP_PATH_9);

        // Initialize
        for (int i=0; i<NUM_OF_HASHMAP; i++) {
          result.pmem_hashmap[i]->ClearAll();
        }

        // NOTE: FIXME: [190313] use this as cache iterator.. 
        // actual skiplist_cache has ordering problem on compaction
        // Smallest and largest key are invalid..
        result.pmem_internal_iterator = new PmemIterator*[NUM_OF_HASHMAP];
        result.pmem_internal_iterator[0] = new PmemIterator(0, result.pmem_hashmap[0]);
        result.pmem_internal_iterator[1] = new PmemIterator(1, result.pmem_hashmap[1]);
        result.pmem_internal_iterator[2] = new PmemIterator(2, result.pmem_hashmap[2]);
        result.pmem_internal_iterator[3] = new PmemIterator(3, result.pmem_hashmap[3]);
        result.pmem_internal_iterator[4] = new PmemIterator(4, result.pmem_hashmap[4]);
        result.pmem_internal_iterator[5] = new PmemIterator(5, result.pmem_hashmap[5]);
        result.pmem_internal_iterator[6] = new PmemIterator(6, result.pmem_hashmap[6]);
        result.pmem_internal_iterator[7] = new PmemIterator(7, result.pmem_hashmap[7]);
        result.pmem_internal_iterator[8] = new PmemIterator(8, result.pmem_hashmap[8]);
        result.pmem_internal_iterator[9] = new PmemIterator(9, result.pmem_hashmap[9]);
        break;
    }
  }
  if (result.use_pmem_buffer) {
    // PROGRESS:
    result.pmem_buffer = new PmemBuffer*[NUM_OF_BUFFER]; 
    result.pmem_buffer[0] = new PmemBuffer(BUFFER_PATH_0);
    result.pmem_buffer[1] = new PmemBuffer(BUFFER_PATH_1);
    result.pmem_buffer[2] = new PmemBuffer(BUFFER_PATH_2);
    result.pmem_buffer[3] = new PmemBuffer(BUFFER_PATH_3);
    result.pmem_buffer[4] = new PmemBuffer(BUFFER_PATH_4);
    result.pmem_buffer[5] = new PmemBuffer(BUFFER_PATH_5);
    result.pmem_buffer[6] = new PmemBuffer(BUFFER_PATH_6);
    result.pmem_buffer[7] = new PmemBuffer(BUFFER_PATH_7);
    result.pmem_buffer[8] = new PmemBuffer(BUFFER_PATH_8);
    result.pmem_buffer[9] = new PmemBuffer(BUFFER_PATH_9);

    // result.pmem_buffer[10] = new PmemBuffer(BUFFER_PATH_10);
    // result.pmem_buffer[11] = new PmemBuffer(BUFFER_PATH_11);
    // result.pmem_buffer[12] = new PmemBuffer(BUFFER_PATH_12);
    // result.pmem_buffer[13] = new PmemBuffer(BUFFER_PATH_13);
    // result.pmem_buffer[14] = new PmemBuffer(BUFFER_PATH_14);

    // Initialize
    for (int i=0; i<NUM_OF_BUFFER; i++) {
      result.pmem_buffer[i]->ClearAll();
    }
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(nullptr),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)),
      // JH
      total_delayed_micros(0),
      tiering_stats_{},
      preserve_flag(false)
      {
  has_imm_.Release_Store(nullptr);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-null value is ok
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  // JH
  Log(options_.info_log, "[Finish] total delayed micros: %lld\n", 
                          total_delayed_micros);
  printf("[Finish] total delayed micros: %lld\n", total_delayed_micros);
  
  // if (options_.sst_type == kPmemSST && options_.ds_type == kSkiplist) {
  //   printf("[DEBUG] free_list size\n");
  //   for (int i=0; i<NUM_OF_SKIPLIST_MANAGER; i++) {
  //     size_t freeListSize = options_.pmem_skiplist[i]->GetFreeListSize();
  //     size_t allocatedMapSize = options_.pmem_skiplist[i]->GetAllocatedMapSize();
  //     printf("%d] free_list:'%d', allocated_map:'%d'\n", i, freeListSize, allocatedMapSize);
  //   }
  // }
  
  // Tiering statistics
  printf("File Set size %d\n", tiering_stats_.GetFileSetSize());
  printf("Skiplist Set size %d\n", tiering_stats_.GetSkiplistSetSize());

  // Delete persistent object
  if (options_.sst_type == kPmemSST) {
    switch (options_.ds_type) {
      // DS_Option1: Skiplist
      case kSkiplist:
        for (int i=0; i<NUM_OF_SKIPLIST_MANAGER; i++) {
          delete options_.pmem_skiplist[i];
          // delete options_.pmem_internal_iterator[i]; // DEBUG:
        }
        delete[] options_.pmem_skiplist;
        // delete[] options_.pmem_internal_iterator;

        break;
      // DS_Option2: Hashmap
      case kHashmap:
        for (int i=0; i<NUM_OF_HASHMAP; i++) {
          delete options_.pmem_hashmap[i];
          delete options_.pmem_internal_iterator[i];
        }
        delete[] options_.pmem_hashmap;
        delete[] options_.pmem_internal_iterator;

        break;
    }
  }
  if (options_.use_pmem_buffer) {
    for (int i=0; i<NUM_OF_BUFFER; i++) {
      delete options_.pmem_buffer[i];
    }
    delete[] options_.pmem_buffer;
  }

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            static_cast<int>(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
        // printf("Here\n");
      }
    }
  }
  

}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    /*
     * SOLVE: Write file based on pmem
     */
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta, &tiering_stats_);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
//	printf("flush to level:%d \n", level);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      // printf("11]\n");
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr &&
             manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    // printf("MaybeScheduleCompaction()\n");
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
      // printf("22]\n");
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  Version* current = versions_->current();/*zewei coldfind*/
  if (is_manual) {
    /*----------------*/
    // zewei coldfind
    current->cold_input.clear();
    current->cold_output.clear();
    /*----------------*/
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    // c = versions_->PickCompaction();
    c = versions_->PickCompaction(&tiering_stats_);
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    /*----------------*/
    // zewei coldfind 
    current->cold_input.clear();
    current->cold_output.clear();
    /*----------------*/    
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    /*----------------*/
    // zewei coldfind 
    current->cold_input.clear();
    current->cold_output.clear();
    /*----------------*/
    CompactionState* compact = new CompactionState(c);
    /* PROGRESS: Compaction based on pmem */
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();

    /* SOLVE: Delete files based on pmem */
    if(options_.sst_type == kFileDescriptorSST || 
       options_.tiering_option != kNoTiering) {
      DeleteObsoleteFiles();
    } else if (options_.sst_type == kPmemSST) {
      // Done
    }
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}
/*----------------------------------------------------------------*/
void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }

  /*---------------------*/
  // zewei_comp
  if (compact->builder_hot != nullptr) {
      compact->builder_hot->Abandon();
      delete compact->builder_hot;
  }
  for (size_t i = 0; i < compact->outputs_hot.size(); i++) {
      const CompactionState::Output& out_hot = compact->outputs_hot[i];
      pending_outputs_.erase(out_hot.number);
  }
  /*---------------------*/

  delete compact;
}
/*------------------------------------------------------------------*/
/* SOLVE: Compaction based on pmem */
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact, 
                                uint64_t file_number, bool is_file_creation, CreatOption creat_option /*zewei_comp*/) {
  assert(compact != nullptr);
  //std::cout << "---call:open compaction outputfile---" << std::endl; // print hotcomp
  /*assert(compact->builder == nullptr);*/
  // NOTE: Get file_number in advance.
  // uint64_t file_number;
  {
    mutex_.Lock();
    // file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    /*-----------------------------*/
    // zewei_comp
    if (creat_option == kWarm) 
        compact->outputs.push_back(out);
    else if (creat_option == kHot)
        compact->outputs_hot.push_back(out);
    /*-----------------------------*/
    mutex_.Unlock();
  }
  // Make the output file
  //std::string fname = TableFileName(dbname_, file_number);
  Status s;
  if (options_.sst_type == kFileDescriptorSST || is_file_creation) {
    std::string fname = TableFileName(dbname_, file_number);
    s = env_->NewWritableFile(fname, &compact->outfile);
    if (s.ok()) {
      //std::cout << "[open] SST: " << file_number<< std::endl; // print hotcomp
      compact->builder = new TableBuilder(options_, compact->outfile);
    }
  } else if (options_.sst_type == kPmemSST) {
      /*--------------------------*/
      if (creat_option == kWarm){
	//std::cout << "[open] warm: " << file_number << std::endl; // print hotcomp
         compact->builder = new TableBuilder(options_, nullptr);
      }
      // zewei_comp
      else if (creat_option == kHot){
	 //std::cout << "[open] hot :" << file_number << std::endl; // print hotcomp
	 compact->builder_hot = new TableBuilder(options_, nullptr);
      }
      /*--------------------------*/
      //compact->builder = new TableBuilder(options_, nullptr);
  }
  //std::cout << "---finish:open compaction outputfile---" << std::endl; // print hotcomp
  return s;
}

/*--------------------------------------------------------------------------*/
/* DEBUG: Compaction based on pmem */
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                      Iterator* input, bool is_file_creation, CreatOption creat_option /*zewei_comp*/ ) {
 

  //std::cout << "---call:finish compaction outputfile---" << std::endl; // print hotcomp
  assert(compact != nullptr);
  //std::cout << "1" << std::endl; // print hotcomp
  if (is_file_creation) {
    assert(compact->outfile != nullptr);
  }
  //assert(compact->builder != nullptr);

  //std::cout << "2" << std::endl; // print hotcomp
  SSTMakerType sst_type = options_.sst_type;
/*---------------------------------------------*/
  // zewei_comp
  //std::cout << "3" << std::endl; // print hotcomp
  uint64_t output_number;
  if (creat_option==kWarm)
      output_number = compact->current_output()->number;
  else if (creat_option==kHot)
      output_number = compact->current_output_hot()->number;
/*---------------------------------------------*/
//  const uint64_t output_number = compact->current_output()->number;
  //std::cout << "4" << std::endl; // print hotcomp
  assert(output_number != 0);
  //DEBUG:
  // printf("[FinishCompactionOutputFile]\n");
  // Check for iterator errors
  Status s = input->status();
  /*---------*/
  // zewei_hotcomp
  //const uint64_t current_entries = compact->builder->NumEntries();
  uint64_t current_entries = 0;
  uint64_t current_entries_hot = 0;
  if (creat_option == kWarm) current_entries = compact->builder->NumEntries();
  else if (creat_option == kHot) current_entries_hot = compact->builder_hot->NumEntries();
  /*---------*/
  //std::cout << "5" << std::endl; // print hotcomp
  if (sst_type == kFileDescriptorSST || is_file_creation) {
    if (s.ok()) {
      //std::cout << "[finish] SST" << std::endl; // print hotcomp
      s = compact->builder->Finish();
      tiering_stats_.InsertIntoFileSet(output_number);
    } else {
      compact->builder->Abandon();
    }
  } else if (sst_type == kPmemSST) {
    DelayPmemWriteNtimes(1);
    /*---------------*/
    // zewei_comp
    if (creat_option==kWarm){
         //std::cout << "insert into skiplist [warm]:" << output_number << ", entrries :" << compact->builder->NumEntries() << std::endl;
	 s = compact->builder->FinishPmem();
    	 tiering_stats_.InsertIntoSkiplistSet(output_number);
    }
    else if (creat_option==kHot && current_entries_hot>0){
         //std::cout << "insert into skiplist [hot]:" << output_number << ", entrries :" << compact->builder_hot->NumEntries() << std::endl;
	 s = compact->builder_hot->FinishPmem();
    	 tiering_stats_.InsertIntoSkiplistSet(output_number);
    }
    /*---------------*/ 
    
    if (options_.tiering_option == kColdDataTiering ||
        options_.tiering_option == kLRUTiering) {
      tiering_stats_.PushToNumberListInPmem(compact->compaction->level()+1, output_number);
    }
  }
  /*--------------------------------*/
  //std::cout << "6" << std::endl; // print hotcomp

  uint64_t current_bytes;
  if(creat_option==kWarm){
    //std::cout << "[finish & reserve] warm:" << output_number << ", entrries :" << compact->builder->NumEntries() << std::endl;

    current_bytes = compact->builder->FileSize();
    compact->current_output()->file_size = current_bytes;
    // printf("[DEBUG][num_entries %d][filesize %d]\n", current_entries, current_bytes);
    compact->total_bytes += current_bytes;
    delete compact->builder;
    compact->builder = nullptr;
  }
  /*-----------------------*/
  // zewei_comp
  else if (creat_option == kHot) {
      //std::cout << "[finish] hot:" << output_number << ", entrries :" << compact->builder_hot->NumEntries() << std::endl;

      if (current_entries_hot > 0) {
        //std::cout << "[finish-hot] reserve: " << output_number << std::endl;
        current_bytes = compact->builder_hot->FileSize();
      	compact->current_output_hot()->file_size = current_bytes;
      	compact->total_bytes += current_bytes;
      }
      else{
	//std::cout << "[finish-hot] delete: " << output_number << std::endl;
     	compact->outputs_hot.pop_back();
      }
      
      delete compact->builder_hot;
      compact->builder_hot = nullptr;
  }
  /*--------------------------------*/
  //std::cout << "7" << std::endl; // print hotcomp

  // Output file
  if (sst_type == kFileDescriptorSST || is_file_creation) {
    // Finish and check for file errors
    if (s.ok()) {
      s = compact->outfile->Sync();
    }
    if (s.ok()) {
      s = compact->outfile->Close();
    }
    delete compact->outfile;
    compact->outfile = nullptr;
  } else if (sst_type == kPmemSST) {
    // Done
    // No outfile
    // delete compact->outfile;
    // compact->outfile = nullptr;
  }
  //std::cout << "8" << std::endl; // print hotcomp

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator *iter;
    // DEBUG:
    if (sst_type == kFileDescriptorSST || is_file_creation) {
    printf("OMGOMG~~~~");    
    // if (sst_type == kFileDescriptorSST ) {
      iter = table_cache_->NewIterator(ReadOptions(),
                                       output_number,
                                       current_bytes);
      s = iter->status();
      delete iter;
    // } else if (sst_type == kPmemSST ) {
    } else if (sst_type == kPmemSST && 
                options_.ds_type == kSkiplist && 
                options_.skiplist_cache) {
      iter = table_cache_->NewIteratorFromPmem(ReadOptions(),
                                                output_number,
                                                current_bytes);
      // FIXME:
      s = iter->status();
      iter->RunCleanupFunc();
    }
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  //std::cout << "---finish:finish compaction outputfile---" << std::endl; // print hotcomp
  // printf("[DEBUG][FinishCompaction1 End] num:%d\n",output_number);
  return s;
}
/*------------------------------------------------------------------------------*/

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));
 
  /*--------------------------------------*/
  // zewei coldfind
  Version* current = versions_->current();

  if (compact->compaction->level() == 1) {
      for (int i = 0; i < compact->compaction->num_input_files(0); i++) {
          //std::cout << "input skiplist number:" << compact->compaction->input(0, i)->number << std::endl;
          current->cold_input.push_back(compact->compaction->input(0, i));
      }
      //std::cout << "output warm :" << compact->outputs.size() << std::endl;
      for (int i = 0; i < compact->outputs.size(); i++) {
          const CompactionState::Output& out = compact->outputs[i];
          FileMetaData* f = new FileMetaData();
          //std::cout << "f ptr:" << f << std::endl;
          //std::cout << "output skiplist number:" << out.number<<std::endl;
          f->number = out.number;
          f->file_size = out.file_size;
          f->smallest = out.smallest;
          f->largest = out.largest;
          current->cold_output.push_back(f);
      }
      //std::cout << "cold_input size:" << current->cold_input.size() << std::endl;
      //std::cout << "cold_output size:" << current->cold_output.size() << std::endl;

  }
  /*--------------------------------------*/ 
  
  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile( level + 1, out.number, out.file_size, out.smallest, out.largest);
  }
  /*---------*/
  // zewei_comp
  for (size_t i = 0; i < compact->outputs_hot.size(); i++) {
      const CompactionState::Output& out = compact->outputs_hot[i];
      compact->compaction->edit()->AddFile(0, out.number, out.file_size, out.smallest, out.largest);
  }
  /*---------*/
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}
/*-------------------------------------------------------------------------------*/
//================================================================================
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  //std::cout << "---start compaction---" << std::endl; //print hotcomp
						    //
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  
  //std::cout << "step1:" << std::endl; //print hotcomp
  //=================================================
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }


  //std::cout << "step2:" << std::endl; //print hotcomp
  //=================================================
  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();
  // SOLVE: Need to analyze here
  Iterator* input = versions_->MakeInputIterator(compact->compaction, &tiering_stats_);
  // printf("SeekToFirst1\n");

  //std::cout << "step3:" << std::endl; //print hotcomp
  //=================================================
  input->SeekToFirst();
  // printf("SeekToFirst2\n");
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  SSTMakerType sst_type = options_.sst_type;
  // int i=0;

  /* Tiering trigger */
  bool write_pmem_buffer = false;  // flag that store contents into pmem
  bool maintain_flag = false;
  bool need_file_creation = false; // flag that store contents as SST file
  bool leveled_trigger = false;    // Opt1
  bool lru_trigger = false;        // Opt3
  uint64_t lru_flushed_bytes_written = 0; // Opt3 for stats
  // std::vector<uint64_t> pending_deleted_number_in_pmem; // for synchronization
 
  /*--------------------------*/
  // zewei_hotcomp
  //bool hotcomp = compact->compaction->level() == 0;
  
  int comp_level = compact->compaction->level();
  bool hotcomp =  (comp_level==0 || comp_level == 1);
  /*--------------------------*/

  if (options_.ds_type == kSkiplist) {
    switch(options_.tiering_option) {
      // Opt1
      case kLeveledTiering:
      { 
        int output_file_level = compact->compaction->level()+1;
        if (output_file_level > PMEM_SKIPLIST_LEVEL_THRESHOLD) {
          leveled_trigger = true;
        }
        break;
      }
      // Opt2
      case kColdDataTiering:
      // Opt3
      case kLRUTiering:
        // If compaction candidate include any SST files, create output as SST
        // It prevents from skip list + SST => skip list
        if (compact->compaction->inputs_in_fileset_->size() > 0) {
          lru_trigger = true;
        }
        break;
      // Opt4
      case kNoTiering:
        // Done
        break;
    }
  }

  // uint32_t cry=0;
  // printf("Start iteration\n");
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    
    //std::cout << "step3.1:" << std::endl; //print hotcomp
    //===================================
    // printf("key:'%s'\n", input->key());
    // Check skiplist's free_list is full
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != nullptr) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }


    //std::cout << "step3.2:" << std::endl; //print hotcomp
    //===================================
    Slice key = input->key();
    /*-------------------------*/
    // zewei_hotcomp
    if (compact->compaction->ShouldStopBefore(key)) {
	   // std::cout<<"should stop before"<<std::endl; // print hotcomp
        if (compact->builder != nullptr) {
            //std::cout << "should stop warm" << std::endl; // print hotcomp
            if (write_pmem_buffer) {
                uint64_t file_number = compact->current_output()->number;
                PmemBuffer* pmem_buffer =
                    options_.pmem_buffer[file_number % NUM_OF_BUFFER];
                compact->builder->FlushBufferToPmemBuffer(pmem_buffer, file_number);
                write_pmem_buffer = false;
            }
	    //std::cout << "finish 1:" << std::endl; //print hotcomp
            status = FinishCompactionOutputFile(compact, input, need_file_creation, kWarm); // need file creation
            /*------------------------------------------------*/
            // JH
            // reset tiering-trigger flag
            need_file_creation = false;
            maintain_flag = false;
            /*------------------------------------------------*/

            if (!status.ok()) {
                break;
            }
        }

        if (compact->builder_hot != nullptr) {
	    //std::cout << "should stop hot" << std::endl; // print hotcomp
	    //std::cout << "finish 2:" << std::endl; //print hotcomp
            status = FinishCompactionOutputFile(compact, input, need_file_creation, kHot);
        }

        if (!status.ok()) {
            break;
        }
    }
    /*-------------------------*/
    //if (compact->compaction->ShouldStopBefore(key) &&
    //    compact->builder != nullptr) {
    //  if (write_pmem_buffer) {
    //    uint64_t file_number = compact->current_output()->number;
    //    PmemBuffer* pmem_buffer = 
    //            options_.pmem_buffer[file_number % NUM_OF_BUFFER];
    //    compact->builder->FlushBufferToPmemBuffer(pmem_buffer, file_number);
    //    write_pmem_buffer = false;
    //  }
    //  status = FinishCompactionOutputFile(compact, input, need_file_creation, kWarm /*zewei_comp*/);
    //  // reset tiering-trigger flag
    //  need_file_creation = false;
    //  maintain_flag = false;
    //  if (!status.ok()) {
    //    break;
    //  }
    //}

    // NOTE: Check whether current key is valid. If not, drop = true.
    // Handle key/value, add to state, etc.
    
    //std::cout << "step3.3:" << std::endl; //print hotcomp
    //===================================
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
	// For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif


    //std::cout << "step3.4:" << std::endl; //print hotcomp
    //===================================
    if (!drop) {
      // Open output file if necessary
      //std::cout << "debug 1" << std::endl; //print hotcomp
      if (compact->builder == nullptr && !maintain_flag ) {
        uint64_t file_number = versions_->NewFileNumber();
	//std::cout << "debug 1-1" << std::endl; //print hotcomp

        /* Check tiering conditions */
        switch (options_.ds_type) {
          case kSkiplist:
            PmemSkiplist* pmem_skiplist = 
                      options_.pmem_skiplist[file_number % NUM_OF_SKIPLIST_MANAGER];
            bool is_freelist_empty = pmem_skiplist->IsFreeListEmptyWarning();
            // PROGRESS: Flush [Opt2, Opt3]
            switch (options_.tiering_option) {
              case kLeveledTiering:
                need_file_creation = (is_freelist_empty || leveled_trigger) ? 
                                      true : 
                                      false;
                break;
              case kColdDataTiering:
              // 1) Get candidate
              // 2) Flush pmem_skiplist to SST
              // 3) collect statistics
                need_file_creation = false;
                break;
              case kLRUTiering:
              {
                if (lru_trigger) {
                  // compulsory creation of SST
                  need_file_creation = true;
                }
                else if (is_freelist_empty) {
                  /* 1) Get candidate number */
                  level_number evicted_level_number;
                  for (int i=0 ; ; i++) {
                    evicted_level_number =
                        tiering_stats_.GetElementFromNumberListInPmem(file_number, i);
                        // printf("evicted_number %d\n", evicted_number);
                    bool current_in_use = false;
                    for (int layer=0; layer<2; layer++) {
                      for (int i=0; i<compact->compaction->num_input_files(layer); i++) {
                        uint64_t number = compact->compaction->input(layer, i)->number;
                        if (evicted_level_number.number == number) {
                          current_in_use = true;
                          break;
                        }
                      }
                      if(current_in_use) break;
                    }
                    if(!current_in_use) break;
                  }
                  tiering_stats_.RemoveFromNumberListInPmem(evicted_level_number.number);

                  /* 
                   * 2) Flush pmem_skiplist to SST 
                   * Copy from builder.cc
                   */
                  FileMetaData meta;
                  meta.number = evicted_level_number.number;
                  meta.file_size = 0;
                  std::string fname = TableFileName(dbname_, meta.number);
                  WritableFile* file;
                  Status s = env_->NewWritableFile(fname, &file);
                  if (!s.ok()) {
                    return s;
                  }
                  TableBuilder* builder = new TableBuilder(options_, file);
                  // printf("Compaction meta %d\n", meta.number);
                  PmemIterator* pmem_iterator = new PmemIterator(meta.number, 
                    options_.pmem_skiplist[meta.number % NUM_OF_SKIPLIST_MANAGER]);
                  pmem_iterator->SeekToFirst();
                  meta.smallest.DecodeFrom(pmem_iterator->key());
                  for ( ; pmem_iterator->Valid() ; pmem_iterator->Next()) {
                    Slice key = pmem_iterator->key();
                    meta.largest.DecodeFrom(key);
                    Slice value = pmem_iterator->value();
                    builder->Add(key, value);
                  }
                  delete pmem_iterator;

                  s = builder->Finish();
                  if (s.ok()) {
                    meta.file_size = builder->FileSize();
                    assert(meta.file_size > 0);
                    lru_flushed_bytes_written += meta.file_size; // stats
                  }
                  delete builder;

                  if (s.ok()) {
                    s = file->Sync();
                  }
                  if (s.ok()) {
                    s = file->Close();
                  }
                  delete file;
                  file = nullptr;

                  if (s.ok()) {
                    // Verify that the table is usable
                    Iterator* it = table_cache_->NewIterator(ReadOptions(),
                                                    meta.number,
                                                    meta.file_size);
                    s = it->status();
                    delete it;
                  }
                  /* 3) Stats */
                  // NOTE: pending delete file from pmem_skiplist and tiering_stats
                  // pending_deleted_number_in_pmem.push_back(evicted_level_number.number);
                  mutex_.Lock();
                  pmem_skiplist->DeleteFileWithCheckRef(evicted_level_number.number);
                  tiering_stats_.DeleteFromSkiplistSet(evicted_level_number.number);
                  tiering_stats_.InsertIntoFileSet(evicted_level_number.number);
                  mutex_.Unlock();
                  // printf("[DEBUG] End LRU tiering on %d %d\n", evicted_level_number.number, file_number);
                  need_file_creation = false;
                  need_file_creation = pmem_skiplist->IsFreeListEmpty() ? true : false;
                  if (need_file_creation) {
                    printf("[WARNING][Compaction] already use LRU-tiering option. but temporarily need_file_creation\n");
                    printf("file_number %d\n", file_number);
                  }
                } else {
                  need_file_creation = false;
                }
                break;
              }
              case kNoTiering:
                need_file_creation = is_freelist_empty ? true : false;
                if (need_file_creation) {
                  printf("[WARNING][Compaction] already use no-tiering option. but temporarily need_file_creation\n");
                  abort(); // optionally
                }
                break;
            } 
          break;
        }
	//std::cout << "debug 2" << std::endl; //print hotcomp
        maintain_flag = true;
        status = OpenCompactionOutputFile(compact, file_number, need_file_creation, kWarm /*zewei_comp*/);
        //std::cout << "debug 3" << std::endl; //print hotcomp
	if (!status.ok()) {
          break;   
	}
      }

      /*--------------------------------*/
      // zewei_hotcomp
      if (hotcomp && compact->builder_hot == nullptr && !need_file_creation) {
          uint64_t file_number_hot = versions_->NewFileNumber();
          //std::cout << "[start] open hot file :"<< file_number_hot << std::endl;
	  status = OpenCompactionOutputFile(compact, file_number_hot, need_file_creation, kHot);
          //std::cout << "[done] open hot file :" << file_number_hot << std::endl;
      }
      if (!status.ok()) {
          std::cout << "[hotcomp] Open output file wrong!" << std::endl;
          break;
      }
      /*-------------------------------*/     
      //std::cout << "debug 4" << std::endl; //print hotcomp
 


      /*
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      */

      Slice value = input->value();
      if (sst_type == kFileDescriptorSST || (need_file_creation && maintain_flag)) {
        compact->builder->Add(key, value);

        //-------
        if (compact->builder->NumEntries() == 0) {
            compact->current_output()->smallest.DecodeFrom(key);
        }
        compact->current_output()->largest.DecodeFrom(key);
        //------- 
	 
        // Close output file if it is big enough
        if (compact->builder->FileSize() >=
            compact->compaction->MaxOutputFileSize()) {
	  //std::cout << "finish 3:" << std::endl; //print hotcomp
          status = FinishCompactionOutputFile(compact, input, need_file_creation, kWarm /*zewei_comp*/);
          need_file_creation = false;
          maintain_flag = false;

          if (!status.ok()) {
            break;
          }
        }
      }
          /*---------------------------------------------------------*/
          // zewei_hotcomp
          else if (sst_type == kPmemSST){

		//++cry;
                uint16_t ref_times = input->refTimes();
		//if(ref_times!=0 && hotcomp)std::cout<<"reftimes: "<<ref_times<<std::endl;
                // warm
                if (!hotcomp || (hotcomp &&  ref_times < HOT_THRESHOLD)/* cry%2==1)*/) { 
       
	
       		    //-------
           	    if (compact->builder->NumEntries() == 0) { 
              	    compact->current_output()->smallest.DecodeFrom(key);
              	    }
           	    compact->current_output()->largest.DecodeFrom(key);
           	    //-------
       
		    uint16_t file_number_warm = compact->current_output()->number;
                    PmemSkiplist* pmem_skiplist_warm;
                    pmem_skiplist_warm = options_.pmem_skiplist[file_number_warm % NUM_OF_SKIPLIST_MANAGER];
                    PmemBuffer* pmem_buffer_warm = options_.pmem_buffer[file_number_warm % NUM_OF_BUFFER];
                    // SST -> skip list
                    if (input->buffer_ptr() == nullptr) {
                        compact->builder->AddToBufferAndSkiplist(pmem_buffer_warm, pmem_skiplist_warm, file_number_warm, key, value, 0);
                        if (!write_pmem_buffer) write_pmem_buffer = true;
                    }
                    // skip list -> skip list
                    else {
                        compact->builder->AddToSkiplistByPtr(pmem_skiplist_warm, file_number_warm, key, value, input->buffer_ptr(), 0 );
                    }


                    //----------
                    // warm太滿
                    if (compact->builder->NumEntries() >= compact->compaction->MaxOutputEntriesNum() - 1) {
                        if (write_pmem_buffer) {
                            //PmemBuffer* pmem_buffer_warm = options_.pmem_buffer[file_number_warm % NUM_OF_BUFFER];
                            compact->builder->FlushBufferToPmemBuffer(pmem_buffer_warm, file_number_warm); //把右邊flush到左邊，透過builder的rep->buffer
                            write_pmem_buffer = false;
                        }
                        //----//
                        //std::cout << "finish 4:" << std::endl; //print hotcomp
		       	status = FinishCompactionOutputFile(compact, input, need_file_creation, kWarm /*zewei_comp*/); //壓縮完成，結束寫入對象
                        need_file_creation = false; //在這裡本來就為false，所以沒差
                        maintain_flag = false;
                        //----//
                        if (!status.ok()) {
                            break;
                        }
                    }

                }
                // hot
                else if (hotcomp && ref_times >= HOT_THRESHOLD/*cry%2==0*/) {

    		    /*----------------------------------------------------------------------------*/
                    // zewei_hotcomp fixed
                    if (compact->builder_hot->NumEntries() == 0) {
                        compact->current_output_hot()->smallest.DecodeFrom(key);
                    }
                    compact->current_output_hot()->largest.DecodeFrom(key);
                    /*----------------------------------------------------------------------------*/
      		    uint16_t file_number_hot = compact->current_output_hot()->number;
                    PmemSkiplist* pmem_skiplist_hot;
                    pmem_skiplist_hot = options_.pmem_skiplist[file_number_hot % NUM_OF_SKIPLIST_MANAGER];
                    PmemBuffer* pmem_buffer_hot = options_.pmem_buffer[file_number_hot % NUM_OF_BUFFER];
                    // SST -> skip list
                    if (input->buffer_ptr() == nullptr) {
                        // ref_times不給過，所以照理來說不會走這，下面的if write_pmem_buffer不影響、也不會走，可刪掉
                        compact->builder_hot->AddToBufferAndSkiplist(pmem_buffer_hot, pmem_skiplist_hot, file_number_hot, key, value, 0);
                        if (!write_pmem_buffer) write_pmem_buffer = true;
                    }
                    // skip list -> skip list
                    else {
                        compact->builder_hot->AddToSkiplistByPtr(pmem_skiplist_hot, file_number_hot, key, value, input->buffer_ptr(), 0 );
                    }


                    //----------
                    // hot太滿
                    if (compact->builder_hot->NumEntries() >= compact->compaction->MaxOutputEntriesNum() - 1) {
                        if (write_pmem_buffer) {
                            //PmemBuffer* pmem_buffer = options_.pmem_buffer[file_number_hot % NUM_OF_BUFFER];
                            compact->builder_hot->FlushBufferToPmemBuffer(pmem_buffer_hot, file_number_hot); //把右邊flush到左邊，透過builder的rep->buffer
                            write_pmem_buffer = false;
                        }
                        //----//
			//std::cout << "finish 5:" << std::endl; //print hotcomp
                        status = FinishCompactionOutputFile(compact, input, need_file_creation, kHot /*zewei_comp*/); //壓縮完成，結束寫入對象

			need_file_creation = false; //在這裡本來就為false，所以沒差
                        //maintain_flag = false; 優秀的hot設計不需要此flag
                        //----//
                        if (!status.ok()) {
                            break;
			}

		    }
                }

          }


//      else if (sst_type == kPmemSST) {
//        uint64_t file_number = compact->current_output()->number;
//        PmemSkiplist* pmem_skiplist;
//        PmemHashmap* pmem_hashmap;
//        switch (options_.ds_type) {
//          case kSkiplist:
//            pmem_skiplist = options_.pmem_skiplist[file_number % NUM_OF_SKIPLIST_MANAGER];
//            if (options_.use_pmem_buffer) {
//              PmemBuffer* pmem_buffer =
//                    options_.pmem_buffer[file_number % NUM_OF_BUFFER];
//              if(input->buffer_ptr() == nullptr) { // SST -> skip list
//                      /*----tmp out----*/
//                      // uint16_t tmp = input->refTimes();
//                      //if (tmp != 0)
//                      //    std::cout << " pmem key: "<< tmp << std::endl;
//                      /*---------------*/ 
//		      compact->builder->AddToBufferAndSkiplist(pmem_buffer, pmem_skiplist,
//                                                    file_number, key, value, 0 /*zewei*/);
//                if(!write_pmem_buffer) write_pmem_buffer = true;
//              } else { // skip list -> skip list
//		       //
//                    /*----tmp out----*/
//		    // uint16_t tmp =input->refTimes();
//                    // if (tmp!=0)
//                    //     std::cout << " pmem key ref: "<<tmp << std::endl;
//                    /*---------------*/
//
//                compact->builder->AddToSkiplistByPtr (pmem_skiplist,
//                              file_number, key, value, input->buffer_ptr(), 0 /*zewei*/);
//              }
//            } else {
//              // Deprecated in this version
//              /*
//              compact->builder->AddToPmemByPtr(pmem_skiplist, 
//                            file_number, key, value,
//                            input->key_ptr(), input->value_ptr());
//              */
//            }
//            break;
//          case kHashmap:
//            pmem_hashmap = options_.pmem_hashmap[file_number % NUM_OF_SKIPLIST_MANAGER];
//            if (options_.use_pmem_buffer) {
//              compact->builder->AddToHashmapByPtr (pmem_hashmap,
//                            file_number, key, value,
//                            input->key_ptr(), input->buffer_ptr());
//            } else {
//              // TODO:
//              // Deprecated in this version
//              /*
//              compact->builder->AddToPmemByPtr(pmem_skiplist, 
//                            file_number, key, value,
//                            input->key_ptr(), input->value_ptr());
//              */
//            }
//            break;
//        }
//        // Close output file if it is big enough
//        if (compact->builder->NumEntries() >=
//            compact->compaction->MaxOutputEntriesNum() -1 ) {
//          if (write_pmem_buffer) {
//            PmemBuffer* pmem_buffer = 
//                    options_.pmem_buffer[file_number % NUM_OF_BUFFER];
//            compact->builder->FlushBufferToPmemBuffer(pmem_buffer, file_number);
//            write_pmem_buffer = false;
//          }
//          status = FinishCompactionOutputFile(compact, input, need_file_creation, kWarm /*zewei_comp*/);
//          need_file_creation = false;
//          maintain_flag = false;
//          if (!status.ok()) {
//            break;
//          }
//        }
//      }
//
    
    
      
    }
   // printf("Next\n");
    input->Next();
   // printf("Next End\n");
  }
  // printf("End iteration\n");
 


  //std::cout << "step4:" << std::endl; //print hotcomp
  //=================================================
  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    if (write_pmem_buffer) {
      uint64_t file_number = compact->current_output()->number;
      PmemBuffer* pmem_buffer = 
              options_.pmem_buffer[file_number % NUM_OF_BUFFER];
      compact->builder->FlushBufferToPmemBuffer(pmem_buffer, file_number);
      write_pmem_buffer = false;
    }
    //std::cout << "finish 6:" << std::endl; //print hotcomp
    status = FinishCompactionOutputFile(compact, input, need_file_creation, kWarm /*zewei_comp*/);
    need_file_creation = false;
    maintain_flag = false;
  }

  /*--------------------*/
  // zewei_hotcomp
  if (hotcomp && status.ok() && compact->builder_hot != nullptr) {
      
      //std::cout << "finish 7:" << std::endl; //print hotcomp
      status = FinishCompactionOutputFile(compact, input, need_file_creation, kHot /*zewei_comp*/);
      need_file_creation = false; 
      maintain_flag = false;      
  }
  /*--------------------*/



  if (status.ok()) {
    status = input->status();
  }
  if (sst_type == kFileDescriptorSST) {
    delete input;
    input = nullptr;
  } else if (sst_type == kPmemSST) {
    if (options_.skiplist_cache) {
      // TODO:
      // occur memory leak about delete input
      input->RunCleanupFunc();
      input = nullptr;
    }
    else {
      delete input;
      input = nullptr;
    }
  }


  //std::cout << "step5:" << std::endl; //print hotcomp
  //=================================================
  // Make compaction-stats
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  // customized by JH for measuring WAF
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    uint64_t number = compact->outputs[i].number;
    if (tiering_stats_.IsInFileSet(number)) {
      stats.bytes_written += compact->outputs[i].file_size;
    } else if (tiering_stats_.IsInSkiplistSet(number)) {
      uint64_t estimated_written = (compact->outputs[i].file_size / 120) * 8; // pointer = 8bytes
      stats.bytes_written += estimated_written;
    } else {
      printf("[WARN][BGCompaction][Stats] % is not in both fileset and skiplistset\n", number);
      stats.bytes_written += compact->outputs[i].file_size;
    }
  }

  /*----------------------------------------------*/
  // zewei_hotcomp
  CompactionStats stats_hot;

  if (hotcomp && compact->builder_hot!=nullptr) {
      for (size_t i = 0; i < compact->outputs_hot.size(); i++) {
          uint64_t number = compact->outputs_hot[i].number;
          // SST
          if (tiering_stats_.IsInFileSet(number)) {
              stats_hot.bytes_written += compact->outputs_hot[i].file_size;
          }
          // pmem skiplist
          else if (tiering_stats_.IsInSkiplistSet(number)) {
              uint64_t estimated_written = (compact->outputs_hot[i].file_size / 120) * 8; // pointer = 8bytes
              stats_hot.bytes_written += estimated_written;
          }
          // wrong
          else {
              printf("hotcomp wrong estimation, um...maybe not wrong:D :\n", number);
              stats.bytes_written += compact->outputs_hot[i].file_size;
          }
      }
  }
  /*----------------------------------------------*/



  // LRU stats
  stats.bytes_written += lru_flushed_bytes_written;

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);
  if(hotcomp)stats_[compact->compaction->level()].Add(stats_hot);/*zewei_hotcomp*/


  // Actual insertion into current Version
  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));

  /* 
   * pmem table_cache eviction
   * PmemSkiplist deletefile
   * Delete FileSet or SkiplistSet
   */
  // L(i) & L(i+1)
  // printf("clear info \n");
  for (int layer=0; layer<2; layer++) {
    std::vector<FileMetaData*>::iterator iter;
    /* in file set */
    for (iter = compact->compaction->inputs_in_fileset_[layer].begin(); 
          iter != compact->compaction->inputs_in_fileset_[layer].end(); 
          iter++ ) {
      FileMetaData* tmp = *iter;
      uint64_t file_number = tmp->number;
      tiering_stats_.DeleteFromFileSet(file_number);
    }
    /* in skip list set */
    for (iter = compact->compaction->inputs_in_skiplistset_[layer].begin(); 
          iter != compact->compaction->inputs_in_skiplistset_[layer].end(); 
          iter++ ) {
      FileMetaData* tmp = *iter;
      uint64_t file_number = tmp->number;
      tiering_stats_.DeleteFromSkiplistSet(file_number);

      if (options_.sst_type == kPmemSST && 
            options_.ds_type == kSkiplist) {
        //std::cout << "Delete skip list number:" << file_number << std::endl;

        PmemSkiplist* pmem_skiplist = 
                options_.pmem_skiplist[file_number % NUM_OF_SKIPLIST_MANAGER];
        pmem_skiplist->DeleteFile(file_number);

        // PROGRESS: Cold_data, LRU => evict from tiering_stats
        if (options_.tiering_option == kColdDataTiering ||
            options_.tiering_option == kLRUTiering) {
          tiering_stats_.RemoveFromNumberListInPmem(file_number);
        }
        if (options_.skiplist_cache) {
          table_cache_->Evict(file_number);
        }
      }
    }
  }
  //std::cout << "---end compaction---" << std::endl;// print hotcomp
  // printf("End background compaction\n");
  return status;
}

//=================================================================================
namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) { }
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list, &tiering_stats_, fileSet, skiplistSet,preserve_flag);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();
  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      /* SOLVE: Get based on pmem */
      // s = current->Get(options, lkey, value, &stats);
      s = current->Get(options_, options, lkey, value, &stats, &tiering_stats_);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  // DEBUG: Stop scheduling additional compaction
  // if (have_stat_update && current->UpdateStats(stats)) {
  //   MaybeScheduleCompaction();
  // }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != nullptr
       ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    // printf("RecordReadSample ??\n");
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }
  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  uint64_t current_micros = env_->NowMicros();
  uint64_t delayed_micros = 0;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
      delayed_micros += env_->NowMicros() - current_micros;
      current_micros = env_->NowMicros();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
      delayed_micros += env_->NowMicros() - current_micros;
      current_micros = env_->NowMicros();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
      delayed_micros += env_->NowMicros() - current_micros;
      current_micros = env_->NowMicros();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      // printf("[DEBUG %d] log_num\n", new_log_number);
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      // printf("33]\n");
      MaybeScheduleCompaction();
    }
  }
  if (delayed_micros != 0) {
    total_delayed_micros += delayed_micros;
    // Log(options_.info_log, "[MakeRoomForWrite] delayed_micros: %lld\n", delayed_micros);
    // Log(options_.info_log, "[MakeRoomForWrite] total_delayed_micros: %lld\n", total_delayed_micros);
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = nullptr;
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
      // printf("44]\n");
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
