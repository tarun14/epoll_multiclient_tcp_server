cmake_minimum_required(VERSION 3.23)

project(epoll_multiclient_tcp_server)

add_executable(nonblocking_epoll_tcp_server)
target_sources(nonblocking_epoll_tcp_server
  PRIVATE
    nonblocking_epoll_tcp_server.cpp
)


add_executable(periodic_tcp_client1)
target_sources(periodic_tcp_client1
  PRIVATE
    periodic_tcp_client1.cpp
)


add_executable(periodic_tcp_client2)
target_sources(periodic_tcp_client2
  PRIVATE
    periodic_tcp_client2.cpp
)
