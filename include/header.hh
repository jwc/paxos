#ifndef HEADER_HH
#define HEADER_HH

#include <deque>
#include <condition_variable>
#include <queue>
//#include <utility>

#include <string>
#include <unordered_map>
#include <iostream>
#include <set>

#include <cstdlib>
#include <cstring>
#include <cstdio>

#include <sys/socket.h>
#include <arpa/inet.h>

#include <thread>

#include <mutex>
#include <semaphore>

#include <unistd.h>

#include <signal.h>

#include <cstdint>
#include <cassert> 

#include <chrono>

#include <ncurses.h>
#include <locale.h>


#define NET_MAX_CONNECTIONS 4
#define NET_BROADCAST -1
#define NET_PORT 9999

#define PAXOS_LOG_SIZE 100 
#define PAXOS_MAX_INTERVALS_FROM_LEADER 4
#define PAXOS_TIMEOUT_MS 150
#define PAXOS_TIMEOUT_NS 1000*1000*PAXOS_TIMEOUT_MS
#define PAXOS_MAX_PENDING 50 

#define FLBUFFER_SIZE 50 

//#endif // HEADER_HH


#include "task.hh"
#include "interfaces.hh"
#include "ipv4.hh"

#include "paxos.hh"


/*
#include "message.hh"

#include "buffer.hh"
#include "svbuffer.hh"
#include "flbuffer.hh"

#include "ipv4.hh"
#include "localhost.hh"
#include "lan.hh"
#include "simnet.hh"

#include "paxos.hh"

#include "printable.hh"
#include "cards.hh"
#include "hearts.hh"
*/

#endif // HEADER_HH

