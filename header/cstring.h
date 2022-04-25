#pragma once
#include <algorithm>
#include <cstdarg>
#include <string>
using namespace std;

class CString {
 public:
  string str_;
  CString() : str_(){};
  CString(const CString &A) : str_(A.str_){};
  CString(const char *S) : str_(S){};
  CString(const string &A) : str_(A){};
  void Format(const char *format, ...) {
    va_list args;
    int n;
    va_start(args, format);
    char sprint_buf[1024];
    n = vsprintf(sprint_buf, format, args);
    va_end(args);
    this->str_ = string(sprint_buf);
  };

  char *GetBuffer(int x) { return str_.substr(x).data(); }
  int GetLength() { return str_.length(); }
  void ReleaseBuffer() { str_.resize(0); }

  int Find(const char x) { return str_.find(x); }
  int ReverseFind(const char x) { return str_.rfind(x); }
  CString Mid(const int pos) { return str_.substr(pos); }
  bool operator!=(const CString &x) { return str_ != x.str_; }

  CString Left(const int pos) { return str_.substr(0, pos); }
  CString operator=(const char *p) {
    str_ = p;
    return str_;
  };

  int Compare(const CString &x) { return str_.compare(x.str_); }
  CString operator+=(const CString &str) { return str_.append(str.str_); }
  CString operator+=(const char x) { return str_ += x; }

  char operator[](int x) { return str_[x]; }

  bool operator==(const CString &x) { return str_ == x.str_; }

  CString operator+(const CString &x) { return str_ + x.str_; }
  friend ostream &operator<<(ostream &out, const CString &str) {
    out << str.str_;
    return out;
  }

  friend CString operator+(const char *x, const CString &y) { return x + y.str_; }
};
