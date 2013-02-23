// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"
#include <assert.h>

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}


bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  
  // Make a new LockRequest
  LockRequest l(EXCLUSIVE, txn);

  // Initialize the locktable for that key
  if (lock_table_.count(key))
    lock_table_[key]->push_back(l);
  else {
    deque<LockRequest> *my_queue = new deque<LockRequest>(1, l);
    lock_table_[key] = my_queue;
  }

  // Check if the transaction acquired the lock
  if (lock_table_[key]->size() == 1)
    return true;
  else {
    if (txn_waits_.count(txn))
      txn_waits_[txn] = 1;
    else txn_waits_[txn]++;
    return false;
  }
}


bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}


void LockManagerA::Release(Txn* txn, const Key& key) {

  // Whether the removed trasaction had a lock
  bool hadLock;

  // The transaction requests for the key
  deque<LockRequest> *requests = lock_table_[key];

  // Remove the txn from requests
  deque<LockRequest>::iterator i;
  for (i=requests->begin(); i != requests->end(); i++) {
    if (i->txn_ == txn) {
      hadLock = (requests->front().txn_ == txn);
      requests->erase(i);
      break;
    }
  }

  // Start the next txn if it acquired the lock
  if (requests->size() >= 1 && hadLock) {
    Txn *to_start = requests->front().txn_;
    if (--txn_waits_[to_start] == 0) ready_txns_->push_back(to_start);
  }

}


LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {

  // reinitialize the owners vector
  deque<LockRequest>::iterator i;
  owners->clear();

  // fill the owners vector
  if (lock_table_[key]->size() != 0)
    owners->push_back(lock_table_[key]->begin()->txn_);

  // report status to user
  return owners->empty() ? UNLOCKED : EXCLUSIVE;

}


LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
  assert(ready_txns_->size() == 0);
}


bool LockManagerB::WriteLock(Txn* txn, const Key& key) {

  // Initialize the deque if it doesn't exist
  LockRequest l(EXCLUSIVE, txn);
  if (lock_table_.count(key))
    lock_table_[key]->push_back(l);
  else {
    deque<LockRequest> *my_queue = new deque<LockRequest>(1, l);
    lock_table_[key] = my_queue;
  }

  if (lock_table_[key]->front().txn_ == txn)
    // since it's the only transaction that wants the lock, say ok to start
    return true;
  else {
    // initialize or increment the number of locks to wait for
    if (txn_waits_.count(txn))
      txn_waits_[txn] = 1;
    else txn_waits_[txn]++;
    return false;
  }
}


bool LockManagerB::ReadLock(Txn* txn, const Key& key) {

  // Make a new LockRequest
  LockRequest l(SHARED, txn);

  // Initialize lock_table_
  if (lock_table_.count(key)) {
    lock_table_[key]->push_back(l);
  } else {
    deque<LockRequest> *my_queue = new deque<LockRequest>(1, l);
    lock_table_[key] = my_queue;
  }

  // Increment position in txn_waits_ if lock is not acquired
  deque<LockRequest> *requests = lock_table_[key];
  deque<LockRequest>::iterator i;
  for (i=requests->begin(); i != requests->end(); i++) {
    if (i->mode_ == EXCLUSIVE) {
      if (txn_waits_.count(txn))
        txn_waits_[txn] = 1;
      else txn_waits_[txn]++;
      return false;
    }
  }
  return true;
}


void LockManagerB::Release(Txn* txn, const Key& key) {

  // Lock requests for the key
  deque<LockRequest> *requests = lock_table_[key];

  // Remove the txn from the requests list
  deque<LockRequest>::iterator i;
  for (i=requests->begin(); i != requests->end(); i++) {

    // Transaction was found
    if (i->txn_ == txn) {
      if ((i+1) != requests->end()) {
        bool atStart = i == requests->begin();

        // _SE -> start E
        // _EE -> start E
        if (atStart && ((i+1)->mode_ == EXCLUSIVE)) {
          if (--txn_waits_[(i+1)->txn_] == 0) ready_txns_->push_back((i+1)->txn_);
        }

        // _ES -> start longest substring of sses
        // SES -> start longest substring of sses
        if ((i->mode_ == EXCLUSIVE) && (
          (atStart && (i+1)->mode_ == SHARED) ||
          ((!atStart && (i-1)->mode_ == SHARED) && ((i+1)->mode_ == SHARED)))) {
              deque<LockRequest>::iterator j;
            for (j=i+1; j != requests->end() && j->mode_ == SHARED; j++)
              if (--txn_waits_[j->txn_] == 0) ready_txns_->push_back(j->txn_);
        }
      }
      requests->erase(i);
      break;
    }

  }

}


LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {

  // Initialize owners vector
  owners->clear();

  // Nothing to do
  if (lock_table_[key]->size() == 0) return UNLOCKED;

  // Fill the vector with the transaction with an exclusive lock
  if (lock_table_[key]->begin()->mode_ == EXCLUSIVE) {
    owners->push_back(lock_table_[key]->begin()->txn_);
    return EXCLUSIVE;
  }

  // Fill the vector with transactions with shared locks
  deque<LockRequest>::iterator i;
  deque<LockRequest> *requests = lock_table_[key];
  for (i=requests->begin(); i != requests->end() && i->mode_ == SHARED; i++) {
    owners->push_back(i->txn_);
  }
  return SHARED;
  
}
