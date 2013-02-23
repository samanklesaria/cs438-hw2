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
  LockRequest l(EXCLUSIVE, txn);
  if (lock_table_.count(key))
    lock_table_[key]->push_back(l);
  else {
    deque<LockRequest> *my_queue = new deque<LockRequest>(1, l);
    lock_table_[key] = my_queue;
  }
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
  bool hadLock;
  deque<LockRequest> *requests = lock_table_[key];
  deque<LockRequest>::iterator i;
  for (i=requests->begin(); i != requests->end(); i++) {
    if (i->txn_ == txn) {
      hadLock = (requests->front().txn_ == txn);
      requests->erase(i);
      break;
    }
  }
  if (requests->size() >= 1 && hadLock) {
    Txn *to_start = requests->front().txn_;
    if (--txn_waits_[to_start] == 0) ready_txns_->push_back(to_start);
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest>::iterator i;
  owners->clear();
  if (lock_table_[key]->size() != 0)
    owners->push_back(lock_table_[key]->begin()->txn_);
  // for (i=lock_table_[key]->begin(); i != lock_table_[key]->end(); i++)
  //  owners->push_back(i->txn_);
  return owners->empty() ? UNLOCKED : EXCLUSIVE;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
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
  deque<LockRequest> *requests = lock_table_[key];

  LockRequest l(SHARED, txn);
  if (lock_table_.count(key))
    lock_table_[key]->push_back(l);
  else {
    deque<LockRequest> *my_queue = new deque<LockRequest>(1, l);
    lock_table_[key] = my_queue;
  }
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

  bool hadLock;
  LockMode hadMode;
  deque<LockRequest> *requests = lock_table_[key];
  deque<LockRequest>::iterator i;
  for (i=requests->begin(); i != requests->end(); i++) {
    if (i->txn_ == txn) {
      hadLock = (requests->front().txn_ == txn);
      hadMode = requests->front().mode_;
      requests->erase(i);
      break;
    }
  }

  // if we took away a shared lock and the next is exclusive, start it
  // if we took away an exclusive lock, start all the shared locks after it
  if (requests->size() >= 1 && hadLock) {
    if (hadMode == SHARED && requests->front().mode_ == EXCLUSIVE) {
      Txn *to_start = requests->front().txn_;
      if (--txn_waits_[to_start] == 0) ready_txns_->push_back(to_start);
    } else if (hadMode == EXCLUSIVE) {
      for (i=requests->begin() + 1; i != requests->end() && i->mode_ == SHARED; i++)
        if (--txn_waits_[i->txn_] == 0) ready_txns_->push_back(i->txn_);
    }
  }

}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  owners->clear();
  if (lock_table_[key]->size() == 0) return UNLOCKED;
  if (lock_table_[key]->begin()->mode_ == EXCLUSIVE) {
    owners->push_back(lock_table_[key]->begin()->txn_);
    return EXCLUSIVE;
  }
  deque<LockRequest>::iterator i;
  deque<LockRequest> *requests = lock_table_[key];
  for (i=requests->begin(); i != requests->end() && i->mode_ == SHARED; i++) {
    owners->push_back(i->txn_);
  }
  return SHARED;
}
