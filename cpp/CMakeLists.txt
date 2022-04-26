cmake_minimum_required(VERSION 3.22.0)

SET(CMAKE_CXX_STANDARD 17)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(main  supp.cpp cat.cpp group.cpp expr.cpp item.cpp logop.cpp mexpr.cpp physop.cpp query.cpp rules.cpp ssp.cpp)