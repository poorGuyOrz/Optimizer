cmake_minimum_required(VERSION 3.16.0)


add_executable(main  supp.cpp cat.cpp group.cpp mexpression.cpp item.cpp logop.cpp  physop.cpp query.cpp rules.cpp ssp.cpp tasks.cpp mainOptimizer.cpp)

redefine_file_macro(main)
