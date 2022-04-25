/*
CM.cpp -  implementation of COST MODEL parser
*/
#include "../header/cm.h"

#include "../header/stdafx.h"

// read catalog text file and store the information into CAT
CM::CM(string filename) {
  ifstream ifs(filename);
  if (!ifs.is_open()) OUTPUT_ERROR("can not open CM file");

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
}
