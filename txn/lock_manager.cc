// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  LockRequest l(EXCLUSIVE, txn);
  lock_table_[key]->push_back(l);
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
  deque<LockRequest> *requests = lock_table_[key];
  for (i=requests->begin(); i != requests->end(); i++) {
    if (i->txn_ == txn) {
      requests.erase(i);
      break;
    }
  }
  if (requests->size() == 1) {
    Txn *to_start = requests->front().txn_;
    --txn_waits_[to_start] || ready_txns->push_back(to_start);
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest>::iterator i;
  owners->clear();
  for (i=lock_table_[key]->begin(); i != lock_table_[key]->end(); i++)
    owners->push_back(*i);
  return owners->empty() ? UNLOCKED : EXCLUSIVE;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

