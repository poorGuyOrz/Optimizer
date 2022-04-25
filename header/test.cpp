#include "cstring.h"

int main(int argc, char const *argv[]) {
  CString str;
  str.Format("xx %d, %f, %s", 111, 111.22, "asdad");

  str = "ccc";
  CString xx ("sdasd");

  str += xx;
  auto xxxx = str[2];

  CString a ("aa");
  CString b ("aa");
  auto ab = a == b;
  
  return 0;
}
