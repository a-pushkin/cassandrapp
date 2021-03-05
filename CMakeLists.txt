cmake_minimum_required(VERSION 3.15)
project(cassandrapp VERSION 0.1 LANGUAGES CXX)

add_library(cassandrapp INTERFACE)
target_include_directories(cassandrapp INTERFACE include)
