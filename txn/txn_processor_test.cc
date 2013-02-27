// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "txn/txn_processor.h"
#include "txn/txn.h"

#include <vector>
#include <string>

#include "txn/txn_types.h"
#include "utils/testing.h"


class BankTxn : public Txn {
 public:
  BankTxn(double time = 0) : time_(time) {
    readset_ = {1};
    writeset_ = {1};
  }

  BankTxn* clone() const {             // Virtual constructor (copying)
    BankTxn* clone = new BankTxn(time_);
    return clone;
  }

  void Run() {
    Value result;
    Read(1, &result);
    Write(1, result + 1);

    // Wait a random amount of time (averaging time_) before committing.
    Sleep(0.9 * time_ + RandomDouble(time_ * 0.2));
    COMMIT;
  }

 private:
  double time_;
};

class Shopping : public Txn {
 public:
  Shopping(Key account, double time = 0) : time_(time) {
    account_ = account;
    readset_ = {1};
    writeset_ = {1, account};
  }

  Shopping* clone() const {             // Virtual constructor (copying)
    Shopping* clone = new Shopping(account_, time_);
    return clone;
  }

  void Run() {
    Value result;
    Read(1, &result);

    if (result) {
      Write(1, result - 1);
      Read(account_, &result);
      Write(account_, result + 1);
    }

    // Wait a random amount of time (averaging time_) before committing.
    Sleep(0.9 * time_ + RandomDouble(time_ * 0.2));
    COMMIT;
  }

 private:
  Key account_;
  double time_;
};

TEST(NoopTest) {
  TxnProcessor p(P_OCC);

  Txn* t = new Noop();
  EXPECT_EQ(INCOMPLETE, t->Status());

  p.NewTxnRequest(t);
  p.GetTxnResult();

  EXPECT_EQ(COMMITTED, t->Status());
  delete t;

  END;
}

TEST(PutTest) {
  TxnProcessor p(P_OCC);
  Txn* t;

  map<long unsigned int, long unsigned int> m = {{1,2}, {3,4}, {5,6}, {7,8}};

  p.NewTxnRequest(new Put(m));
  delete p.GetTxnResult();

  map<long unsigned int, long unsigned int> nokey = {{2,2}};
  p.NewTxnRequest(new Expect(nokey));  // Should abort (no key '2' exists)
  t = p.GetTxnResult();
  EXPECT_EQ(ABORTED, t->Status());
  delete t;

  map<long unsigned int, long unsigned int> wrongval = {{1,1}};
  p.NewTxnRequest(new Expect(wrongval));  // Should abort (wrong value for key)
  t = p.GetTxnResult();
  EXPECT_EQ(ABORTED, t->Status());
  delete t;

  map<long unsigned int, long unsigned int> ok = {{1,2}};
  p.NewTxnRequest(new Expect(ok));  // Should commit
  t = p.GetTxnResult();
  EXPECT_EQ(COMMITTED, t->Status());
  delete t;

  END;
}

TEST(BasicBank) {

  TxnProcessor p(P_OCC);
  Txn* t;

  map<long unsigned int, long unsigned int> m = {{1,0}};

  p.NewTxnRequest(new Put(m));
  delete p.GetTxnResult();

  p.NewTxnRequest(new BankTxn(0.0001));
  p.NewTxnRequest(new BankTxn(0.001));
  p.NewTxnRequest(new BankTxn(0.01));
  p.NewTxnRequest(new BankTxn(0.1));
  p.NewTxnRequest(new BankTxn(0));

  delete p.GetTxnResult();
  delete p.GetTxnResult();
  delete p.GetTxnResult();
  delete p.GetTxnResult();
  delete p.GetTxnResult();

  Sleep(5);

  map<long unsigned int, long unsigned int> ok = {{1,5}};
  p.NewTxnRequest(new Expect(ok));  // Should commit
  t = p.GetTxnResult();
  EXPECT_EQ(COMMITTED, t->Status());
  delete t;

  END;

}

TEST(ShoppingTest) {

  TxnProcessor p(P_OCC);
  Txn* t;

  map<long unsigned int, long unsigned int> m = {{1,3}, {2,0}, {3,0}, {4,0}, {5,0}, {6,0}, {7,0}, {8,0},};

  p.NewTxnRequest(new Put(m));
  delete p.GetTxnResult();

  p.NewTxnRequest(new Shopping(2,0.0001));
  p.NewTxnRequest(new Shopping(3, 0.001));
  p.NewTxnRequest(new Shopping(4, 0.01));
  p.NewTxnRequest(new Shopping(5, 0.1));
  p.NewTxnRequest(new Shopping(6, 0));

  delete p.GetTxnResult();
  delete p.GetTxnResult();
  delete p.GetTxnResult();
  delete p.GetTxnResult();
  delete p.GetTxnResult();

  Sleep(5);

  map<long unsigned int, long unsigned int> ok = {{1,0}};
  p.NewTxnRequest(new Expect(ok));  // Should commit
  t = p.GetTxnResult();
  EXPECT_EQ(COMMITTED, t->Status());
  delete t;

  END;

}


// Returns a human-readable string naming of the providing mode.
string ModeToString(CCMode mode) {
  switch (mode) {
    case SERIAL:                 return " Serial   ";
    case LOCKING_EXCLUSIVE_ONLY: return " Locking A";
    case LOCKING:                return " Locking B";
    case OCC:                    return " OCC      ";
    case P_OCC:                  return " OCC-P    ";
    default:                     return "INVALID MODE";
  }
}

class LoadGen {
 public:
  virtual ~LoadGen() {}
  virtual Txn* NewTxn() = 0;
};

class RMWLoadGen : public LoadGen {
 public:
  RMWLoadGen(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    return new RMW(dbsize_, rsetsize_, wsetsize_, wait_time_);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

class RMWLoadGen2 : public LoadGen {
 public:
  RMWLoadGen2(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    // 10% of transactions are READ only transactions and run for the full
    // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // (65%+) updates.
    if (rand() % 100 < 10)
      return new RMW(dbsize_, rsetsize_, 0, wait_time_);
    else
      return new RMW(dbsize_, 0, wsetsize_, 0);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

void Benchmark(const vector<LoadGen*>& lg) {
  // Number of transaction requests that can be active at any given time.
  int active_txns = 100;
  deque<Txn*> doneTxns;

  // Set initial db state.
  map<Key, Value> db_init;
  for (int i = 0; i < 10000; i++)
    db_init[i] = 0;

  // For each MODE...
  for (CCMode mode = SERIAL;
      mode <= P_OCC;
      mode = static_cast<CCMode>(mode+1)) {
  // CCMode mode = P_OCC;
  // if (1) {
    // Print out mode name.
    cout << ModeToString(mode) << flush;

    // For each experiment...
    for (uint32 exp = 0; exp < lg.size(); exp++) {
      int txn_count = 0;

      // Create TxnProcessor in next mode.
      TxnProcessor* p = new TxnProcessor(mode);

      // Initialize data with initial db state.
      Put init_txn(db_init);
      p->NewTxnRequest(&init_txn);
      p->GetTxnResult();

      // Record start time.
      double start = GetTime();

      // Start specified number of txns running.
      for (int i = 0; i < active_txns; i++)
        p->NewTxnRequest(lg[exp]->NewTxn());

      // Keep 100 active txns at all times for the first full second.
      while (GetTime() < start + 1) {
        doneTxns.push_back(p->GetTxnResult());
        txn_count++;
        p->NewTxnRequest(lg[exp]->NewTxn());
      }

      // Wait for all of them to finish.
      for (int i = 0; i < active_txns; i++) {
        doneTxns.push_back(p->GetTxnResult());
        txn_count++;
      }

      // Record end time.
      double end = GetTime();

      // Print throughput
      cout << "\t" << (txn_count / (end-start)) << "\t" << flush;

      // Delete TxnProcessor and completed transactions.
      doneTxns.clear();
      delete p;
    }

    cout << endl;
  }
}

int main(int argc, char** argv) {

  vector<LoadGen *> lg;

  NoopTest();
  PutTest();
  BasicBank();
  ShoppingTest();

  cout << "\t\t\t    Average Transaction Duration" << endl;
  cout << "\t\t0.1ms\t\t1ms\t\t10ms\t\t100ms";
  cout << endl;

  cout << "Read only" << endl;
  lg.push_back(new RMWLoadGen(10000, 10, 0, 0.0001));
  lg.push_back(new RMWLoadGen(10000, 10, 0, 0.001));
  lg.push_back(new RMWLoadGen(10000, 10, 0, 0.01));
  lg.push_back(new RMWLoadGen(10000, 10, 0, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "1% contention" << endl;
  lg.push_back(new RMWLoadGen(10000, 10, 10, 0.0001));
  lg.push_back(new RMWLoadGen(10000, 10, 10, 0.001));
  lg.push_back(new RMWLoadGen(10000, 10, 10, 0.01));
  lg.push_back(new RMWLoadGen(10000, 10, 10, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "10% contention" << endl;
  lg.push_back(new RMWLoadGen(1000, 10, 10, 0.0001));
  lg.push_back(new RMWLoadGen(1000, 10, 10, 0.001));
  lg.push_back(new RMWLoadGen(1000, 10, 10, 0.01));
  lg.push_back(new RMWLoadGen(1000, 10, 10, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "65% contention" << endl;
  lg.push_back(new RMWLoadGen(100, 10, 10, 0.0001));
  lg.push_back(new RMWLoadGen(100, 10, 10, 0.001));
  lg.push_back(new RMWLoadGen(100, 10, 10, 0.01));
  lg.push_back(new RMWLoadGen(100, 10, 10, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "100% contention" << endl;
  lg.push_back(new RMWLoadGen(10, 0, 10, 0.0001));
  lg.push_back(new RMWLoadGen(10, 0, 10, 0.001));
  lg.push_back(new RMWLoadGen(10, 0, 10, 0.01));
  lg.push_back(new RMWLoadGen(10, 0, 10, 0.1));


  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "High contention mixed read/write" << endl;
  lg.push_back(new RMWLoadGen2(100, 20, 10, 0.0001));
  lg.push_back(new RMWLoadGen2(100, 20, 10, 0.001));
  lg.push_back(new RMWLoadGen2(100, 20, 10, 0.01));
  lg.push_back(new RMWLoadGen2(100, 20, 10, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();
}

