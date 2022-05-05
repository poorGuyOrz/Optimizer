#pragma once
#include "../header/stdafx.h"

class CostModel;

// Cost Model - class CostModel

class CostModel {
 private:
  double CPU_READ;
  double TOUCH_COPY;
  double CPU_PRED;
  double CPU_APPLY;
  double CPU_COMP_MOVE;
  double HASH_COST;
  double HASH_PROBE;
  double INDEX_PROBE;
  double BF;
  double INDEX_BF;
  double IO;
  double BIT_BF;

 public:
  CostModel(string filename) {
    ifstream ifs(filename);
    if (!ifs.is_open()) OUTPUT_ERROR("can not open CostModel file");

    json j;
    ifs >> j;

    CPU_READ = j["CPU_READ"].get<double>();
    TOUCH_COPY = j["TOUCH_COPY"].get<double>();
    CPU_PRED = j["CPU_PRED"].get<double>();
    CPU_APPLY = j["CPU_APPLY"].get<double>();
    CPU_COMP_MOVE = j["CPU_COMP_MOVE"].get<double>();
    HASH_COST = j["HASH_COST"].get<double>();
    HASH_PROBE = j["HASH_PROBE"].get<double>();
    INDEX_PROBE = j["INDEX_PROBE"].get<double>();
    BF = j["BF"].get<double>();
    INDEX_BF = j["INDEX_BF"].get<double>();
    IO = j["IO"].get<double>();
    BIT_BF = j["BIT_BF"].get<double>();

    ifs.close();
  };  // read information into cost model from some default file

  json toJson() {
    json j;

    j["CPU_READ"] = CPU_READ;
    j["TOUCH_COPY"] = TOUCH_COPY;
    j["CPU_PRED"] = CPU_PRED;
    j["CPU_APPLY"] = CPU_APPLY;
    j["CPU_COMP_MOVE"] = CPU_COMP_MOVE;
    j["HASH_COST"] = HASH_COST;
    j["HASH_PROBE"] = HASH_PROBE;
    j["INDEX_PROBE"] = INDEX_PROBE;
    j["BF"] = BF;
    j["INDEX_BF"] = INDEX_BF;
    j["IO"] = IO;
    j["BIT_BF"] = BIT_BF;

    return j;
  }

  string Dump() { return toJson().dump(); }

  // cpu cost of reading one block from the disk
  inline double cpu_read() { return CPU_READ; }

  // cpu cost of copying one tuple to the next operator
  inline double touch_copy() { return TOUCH_COPY; }

  // cpu cost of evaluating one predicate
  inline double cpu_pred() { return CPU_PRED; }

  // cpu cost of applying function on one attribute
  inline double cpu_apply() { return CPU_APPLY; }

  // cpu cost of comparing and moving tuples
  inline double cpu_comp_move() { return CPU_COMP_MOVE; }

  // cpu cost of building hash table
  inline double hash_cost() { return HASH_COST; }

  // cpu cost of finding hash bucket
  inline double hash_probe() { return HASH_PROBE; }

  // cpu cost of finding index
  inline double index_probe() { return INDEX_PROBE; }

  // block factor of table file
  inline double bf() { return BF; }

  // block factor of index file
  inline double index_bf() { return INDEX_BF; }

  // i/o cost of reading one block
  inline double io() { return IO; }

  // block factor of bit index
  inline double bit_bf() { return BIT_BF; }
};
