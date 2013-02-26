// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)

#include "txn/txn_processor.h"
#include <stdio.h>

#include <set>
#include <algorithm>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 100
#define QUEUE_COUNT 10

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT, QUEUE_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);

  // Start 'RunScheduler()' running as a new task in its own thread.
  tp_.RunTask(
        new Method<TxnProcessor, void>(this, &TxnProcessor::RunScheduler));
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler();
    case LOCKING:                RunLockingScheduler();
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler();
    case OCC:                    RunOCCScheduler();
    case P_OCC:                  RunOCCParallelScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      int blocked = 0;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it))
          blocked++;
      }

      // Request write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        if (!lm_->WriteLock(txn, *it))
          blocked++;
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed.
      if (blocked == 0)
        ready_txns_.push_back(txn);
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }
  }
}

void TxnProcessor::RunOCCScheduler() {

  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      txn->occ_start_time_ = GetTime();
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }

    // Verify all completed transactions
    bool verified = true;
    while (completed_txns_.Pop(&txn)) {

      // check for overlap in readset
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {

        // if last modified > my start then invalid
        if (storage_.Timestamp(*it) > txn->occ_start_time_) {
          verified = false;
          break;
        }

      }

      // check for overlap in writeset
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {

        // if last modified > my start then invalid
        if (storage_.Timestamp(*it) > txn->occ_start_time_) {
          verified = false;
          break;
        }

      }

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        if (verified) {

          // Everything is hunky dory
          ApplyWrites(txn);

        } else {

          // Try transaction again
          NewTxnRequest(txn);
          continue;

        }
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

// number of completed transactions to validate per thread
#define N 200
#define M 200

void TxnProcessor::RunOCCParallelScheduler() {
  Txn* txn;
  while (tp_.Active()) {

    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      txn->occ_start_time_ = GetTime();
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }

    // Set the verified state of completed transactions
    int i = 0;
    while (completed_txns_.Pop(&txn) && i++ < N) {
      set<Txn*> active_set_copy = active_set_;
      active_set_.insert(txn);
      tp_.RunTask(new Method<TxnProcessor, void, Txn*, set<Txn*>>(
            this,
            &TxnProcessor::ValidateTxn,
            txn,
            active_set_copy));
    }

    // Restart or commit transactions
    std::pair<Txn*, bool> p;
    int j = 0;
    while (validated_txns_.Pop(&p) && j++ < M) {
      active_set_.erase(p.first);
      if (!p.second) {
        p.first->status_ = INCOMPLETE;
        NewTxnRequest(p.first);
        continue;
      }

      // Return result to client.
      txn_results_.Push(p.first);
    }
  }

}

void TxnProcessor::ValidateTxn(Txn *txn, set<Txn*> active_set_copy) {
  
  assert(active_set_copy.count(txn) == 0);

  // ensure that status is COMPLETED_C
  if (txn->Status() == COMPLETED_A) {
    txn->status_ = ABORTED;
    validated_txns_.Push(std::make_pair(txn, true));
    return;
  } else if (txn->Status() != COMPLETED_C) {
    DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
  }

  bool verified = true;

  // check for overlap in readset
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {

    // if last modified > my start then invalid
    if (storage_.Timestamp(*it) > txn->occ_start_time_) {
      verified = false;
      break;
    }

  }

  // check for overlap in writeset
  // why do we need this?
  // this isn't necessary by abadi's pseudocode
  /*
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {

    // if last modified > my start then invalid
    if (storage_.Timestamp(*it) > txn->occ_start_time_) {
      verified = false;
      break;
    }

  }
  */

  // check if the writeset intersects with the read or write sets
  // of any concurrently validating txns
  for (set<Txn*>::iterator it = active_set_copy.begin();
       it != active_set_copy.end(); ++it) {
    set<Key> union_set;
    std::set_union(
      (*it)->writeset_.begin(), (*it)->writeset_.end(),
      (*it)->readset_.begin(), (*it)->readset_.end(),
      std::inserter(union_set, union_set.begin()));
    for(set<Key>::iterator it2 = txn->writeset_.begin();
        it2 != txn->writeset_.end(); ++it2)
    {
      verified = verified && !union_set.count(*it2);
      if(!verified) break;
    }
  }

  if (verified) ApplyWrites(txn);
  validated_txns_.Push(std::make_pair(txn, verified));
}


void TxnProcessor::ExecuteTxn(Txn* txn) {
  
  // wipe reads_ and writes_
  txn->reads_.clear();
  txn->writes_.clear();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_.Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_.Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);

}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_.Write(it->first, it->second);
  }

  // Set status to committed.
  txn->status_ = COMMITTED;
}
