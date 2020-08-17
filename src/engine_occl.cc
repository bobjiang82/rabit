/*!
 *  Copyright (c) 2020 by Contributors
 * \file engine_occl.h
 * \brief this file provides an implementation of engine that uses OneCCL
 *  as the backend.
 */
#include <unistd.h>
#include <execinfo.h>
#include <iomanip>
#include <iostream>
#include <cstdlib>
#include <typeinfo>
#include "rabit/internal/engine_occl.h"
#include "rabit/internal/thread_local.h"
#include "rabit/internal/rabit-inl.h"

namespace rabit {

//int tracer_t::indentation = 0;

namespace utils {
  bool STOP_PROCESS_ON_ERROR = true;
}

namespace engine {
// singleton sync manager
using Manager = OneCCLEngine;

bool rabitinited = false;

//----- meta information-----
// list of enviroment variables that are of possible interest
std::vector<std::string> env_vars;
// uri of tracker
std::string tracker_uri = "NULL";
// port of tracker address
int tracker_port = 9000;
// uri of current host, to be set by Init
std::string host_uri = "";
// port of slave process
int slave_port=9010, nport_trial=1000;
// current rank
int rank = 0;
// world size
int world_size = -1;
// connect retry time
int connect_retry = 5;
// unique identifier of the possible job this process is doing
// used to assign ranks, optional, default to NULL
std::string task_id = "NULL";
// role in dmlc jobs
std::string dmlc_role = "worker";

// reduce buffer size
size_t reduce_buffer_size;
// reduction method
int reduce_method;
// mininum count of cells to use ring based method
size_t reduce_ring_mincount;
// enable bootstrap cache 0 false 1 true
bool rabit_bootstrap_cache = false;
// enable detailed logging
bool rabit_debug = false;
// by default, if rabit worker not recover in half an hour exit
int timeout_sec = 120;
// flag to enable rabit_timeout
bool rabit_timeout = false;
// Enable TCP node delay
bool rabit_enable_tcp_no_delay = false;

//---- data structure related to model ----
// call sequence counter, records how many calls we made so far
// from last call to CheckPoint, LoadCheckPoint
int seq_counter;
// version number of model
int version_number;
// whether the job is running in hadoop
// bool hadoop_mode;
//---- local data related to link ----
// index of parent link, can be -1, meaning this is root of the tree
int parent_index;
// rank of parent node, can be -1
int parent_rank;

/*! \brief entry to to easily hold returning information */
struct ThreadLocalEntry {
  /*! \brief stores the current engine */
  std::unique_ptr<Manager> engine;
  /*! \brief whether init has been called */
  bool initialized;
  /*! \brief constructor */
  ThreadLocalEntry() : initialized(false) {}
};

// define the threadlocal store.
using EngineThreadLocal = ThreadLocalStore<ThreadLocalEntry>;

// inline void TestMax(size_t n) {
//   int rank = rabit::GetRank();
//   std::cout << "rank: " << rank << std::endl;
//   using dt = unsigned long;
//   std::vector<dt> ndata(n);
//   //char c; unsigned char h; int i; unsigned int j; long l; unsigned long m; float f; double d;
//   //std::cout << "typeid: " << typeid(dt).name() << std::endl;
//   for (size_t i = 0; i < ndata.size(); ++i) {
//     ndata[i] = (i * (rank+1)) % 111;
//   }
//   rabit::Allreduce<op::Max>(&ndata[0], ndata.size());
// }

/*! \brief intiialize the synchronization module */
bool Init(int argc, char *argv[]) { RABIT_LOG_CALL;
//  std::cout << "xgbtck Init() engine_occl.cc" << std::endl;
  ThreadLocalEntry* e = EngineThreadLocal::Get();
  if (e->engine.get() == nullptr) {
    e->initialized = true;
    try {
      // std::cout << "xgbtck calling ccl_init()" << std::endl;
      // ccl_init();
      std::cout << "xgbtck new Manager() engine_occl.cc" << std::endl;
      e->engine.reset(new Manager());
      rabitinited = true;
      std::cout << "xgbtck new Manager() done engine_occl.cc" << std::endl;
      
      SetParam("rabit_reduce_buffer", "256MB");

      env_vars.push_back("DMLC_TASK_ID");
      env_vars.push_back("DMLC_ROLE");
      env_vars.push_back("DMLC_NUM_ATTEMPT");
      env_vars.push_back("DMLC_TRACKER_URI");
      env_vars.push_back("DMLC_TRACKER_PORT");
      env_vars.push_back("DMLC_WORKER_CONNECT_RETRY");
      env_vars.push_back("DMLC_WORKER_STOP_PROCESS_ON_ERROR");

      for (size_t i = 0; i < env_vars.size(); ++i) {
        const char *value = getenv(env_vars[i].c_str());
        if (value != NULL) {
          SetParam(env_vars[i].c_str(), value);
        }
      }
      // pass in arguments override env variable.
      for (int i = 0; i < argc; ++i) {
        char name[256], val[256];
        if (sscanf(argv[i], "%[^=]=%s", name, val) == 2) {
          SetParam(name, val);
        }
      }
      std::cout << "xgbtck task_id: " << task_id << std::endl;
      rank = -1;  //e->engine->GetRank();
      world_size = -1; //e->engine->GetWorldSize();
      utils::Socket::Startup();
      host_uri = utils::SockAddr::GetHostName();
      ReConnectLinks();
//      TestMax(28);
      return true;
    } catch (const ccl::ccl_error& e) {
      utils::Error("failed in OneCCLinit");
      return false;
    }
  } else {
//    TestMax(28);
    return true;
  }
}

/*! \brief finalize syncrhonization module */
bool Finalize(void) { RABIT_LOG_CALL;

  // debug print the stack trace
  size_t size_x;
  void *array_x[1024];
  size_x = backtrace(array_x, 1024);
  backtrace_symbols_fd(array_x, size_x, STDOUT_FILENO);

  ThreadLocalEntry* e = EngineThreadLocal::Get();
  if (e->engine.get() != nullptr) {
    e->engine.reset(nullptr);
    e->initialized = false;
    rabitinited = false;
    if (tracker_uri == "NULL") return true;
    // notify tracker rank i have shutdown
    utils::TCPSocket tracker = ConnectTracker();
    tracker.SendStr(std::string("shutdown"));
    tracker.Close();
    utils::TCPSocket::Finalize();
    // std::cout << "xgbtck calling ccl_finalize()" << std::endl;
    // ccl_finalize();
    return true;
  } else {
    return false;
  }
}

inline size_t ParseUnit(const char *name, const char *val) {
  char unit;
  unsigned long amt;  // NOLINT(*)
  int n = sscanf(val, "%lu%c", &amt, &unit);
  size_t amount = amt;
  if (n == 2) {
    switch (unit) {
      case 'B': return amount;
      case 'K': return amount << 10UL;
      case 'M': return amount << 20UL;
      case 'G': return amount << 30UL;
      default: utils::Error("invalid format for %s", name); return 0;
    }
  } else if (n == 1) {
    return amount;
  } else {
    utils::Error("invalid format for %s,"                               \
                 "shhould be {integer}{unit}, unit can be {B, KB, MB, GB}", name);
    return 0;
  }
}

void SetParam(const char *name, const char *val) {  RABIT_LOG_CALL;
  std::cout << "xgbtck " << name << ": " << val << std::endl;
  if (!strcmp(name, "rabit_tracker_uri")) tracker_uri = val;
  if (!strcmp(name, "rabit_tracker_port")) tracker_port = atoi(val);
  if (!strcmp(name, "rabit_task_id")) task_id = val;
  if (!strcmp(name, "DMLC_TRACKER_URI")) tracker_uri = val;
  if (!strcmp(name, "DMLC_TRACKER_PORT")) tracker_port = atoi(val);
  if (!strcmp(name, "DMLC_TASK_ID")) task_id = val;
  if (!strcmp(name, "DMLC_ROLE")) dmlc_role = val;
  if (!strcmp(name, "rabit_world_size")) world_size = atoi(val);
  // if (!strcmp(name, "rabit_hadoop_mode")) hadoop_mode = utils::StringToBool(val);
  // if (!strcmp(name, "rabit_reduce_ring_mincount")) {
  //   reduce_ring_mincount = atoi(val);
  //   utils::Assert(reduce_ring_mincount > 0, "rabit_reduce_ring_mincount should be greater than 0");
  // }
  // if (!strcmp(name, "rabit_reduce_buffer")) {
  //   reduce_buffer_size = (ParseUnit(name, val) + 7) >> 3;
  // }
  if (!strcmp(name, "DMLC_WORKER_CONNECT_RETRY")) {
    connect_retry = atoi(val);
  }
  if (!strcmp(name, "DMLC_WORKER_STOP_PROCESS_ON_ERROR")) {
    if (!strcmp(val, "true")) {
      rabit::utils::STOP_PROCESS_ON_ERROR = true;
    } else if (!strcmp(val, "false")) {
      rabit::utils::STOP_PROCESS_ON_ERROR = false;
    } else {
      throw std::runtime_error("invalid value of DMLC_WORKER_STOP_PROCESS_ON_ERROR");
    }
  }
  if (!strcmp(name, "rabit_bootstrap_cache")) {
    rabit_bootstrap_cache = utils::StringToBool(val);
  }
  if (!strcmp(name, "rabit_debug")) {
    rabit_debug = utils::StringToBool(val);
  }
  if (!strcmp(name, "rabit_timeout")) {
    rabit_timeout = utils::StringToBool(val);
  }
  if (!strcmp(name, "rabit_timeout_sec")) {
    timeout_sec = atoi(val);
    utils::Assert(timeout_sec >= 0, "rabit_timeout_sec should be non negative second");
  }
  if (!strcmp(name, "rabit_enable_tcp_no_delay")) {
    if (!strcmp(val, "true"))
      rabit_enable_tcp_no_delay = true;
    else
      rabit_enable_tcp_no_delay = false;
  }
}

utils::TCPSocket ConnectTracker(void) {  RABIT_LOG_CALL;
  int magic = kMagic;
  // get information from tracker
  utils::TCPSocket tracker;
  tracker.Create();

  int retry = 0;
  do {
    std::cout << "xgbtck tracker.Connect(), tracker_uri: " << tracker_uri.c_str() << std::endl;
    if (!tracker.Connect(utils::SockAddr(tracker_uri.c_str(), tracker_port))) {
      if (++retry >= connect_retry) {
        fprintf(stderr, "connect to (failed): [%s]\n", tracker_uri.c_str());
        utils::Socket::Error("Connect");
      } else {
        fprintf(stderr, "retry connect to ip(retry time %d): [%s]\n", retry, tracker_uri.c_str());
        sleep(retry << 1);
        continue;
      }
    }
    std::cout << "xgbtck Connected." << std::endl;
    break;
  } while (1);

  using utils::Assert;
  Assert(tracker.SendAll(&magic, sizeof(magic)) == sizeof(magic),
         "ReConnectLink failure 1");
  Assert(tracker.RecvAll(&magic, sizeof(magic)) == sizeof(magic),
         "ReConnectLink failure 2");
  utils::Check(magic == kMagic, "sync::Invalid tracker message, init failure");
  Assert(tracker.SendAll(&rank, sizeof(rank)) == sizeof(rank),
                "ReConnectLink failure 3");
  Assert(tracker.SendAll(&world_size, sizeof(world_size)) == sizeof(world_size),
         "ReConnectLink failure 3");
  tracker.SendStr(task_id);
  return tracker;
}

bool ReConnectLinks(const char *cmd) {  RABIT_LOG_CALL;
  // single node mode
  if (tracker_uri == "NULL") {
    rank = 0; world_size = 1; return true;
  }
  try {
    std::cout << "xgbtck task_id: " << task_id << std::endl;
    utils::TCPSocket tracker = ConnectTracker();
    fprintf(stdout, "xgbtck task %s connected to the tracker\n", task_id.c_str());
    tracker.SendStr(std::string(cmd));

    // the rank of previous link, next link in ring
    int prev_rank, next_rank;
    // the rank of neighbors
    // std::map<int, int> tree_neighbors;
    using utils::Assert;
    // get new ranks
    int newrank, num_neighbors;
    Assert(tracker.RecvAll(&newrank, sizeof(newrank)) == sizeof(newrank),
           "ReConnectLink failure 4");
    // Assert(tracker.RecvAll(&parent_rank, sizeof(parent_rank)) == \
    //      sizeof(parent_rank), "ReConnectLink failure 4");
    Assert(tracker.RecvAll(&world_size, sizeof(world_size)) == sizeof(world_size),
           "ReConnectLink failure 4");
    Assert(rank == -1 || newrank == rank,
           "must keep rank to same if the node already have one");
    rank = newrank;
    std::cout << "new rank=" << newrank << ", new world_size=" << world_size << std::endl;

    // tracker got overwhelemed and not able to assign correct rank
    if (rank == -1) exit(-1);

    fprintf(stdout, "task %s got new rank %d\n", task_id.c_str(), rank);

    // Assert(tracker.RecvAll(&num_neighbors, sizeof(num_neighbors)) == \
    //      sizeof(num_neighbors), "ReConnectLink failure 4");
    // for (int i = 0; i < num_neighbors; ++i) {
    //   int nrank;
    //   Assert(tracker.RecvAll(&nrank, sizeof(nrank)) == sizeof(nrank),
    //          "ReConnectLink failure 4");
    //   tree_neighbors[nrank] = 1;
    // }
    // Assert(tracker.RecvAll(&prev_rank, sizeof(prev_rank)) == sizeof(prev_rank),
    //        "ReConnectLink failure 4");
    // Assert(tracker.RecvAll(&next_rank, sizeof(next_rank)) == sizeof(next_rank),
    //        "ReConnectLink failure 4");

    // utils::TCPSocket sock_listen;
    // if (!sock_listen.IsClosed()) {
    //   sock_listen.Close();
    // }
    // // create listening socket
    // sock_listen.Create();
    // int port = sock_listen.TryBindHost(slave_port, slave_port + nport_trial);
    // utils::Check(port != -1, "ReConnectLink fail to bind the ports specified");
    // sock_listen.Listen();

    // // get number of to connect and number of to accept nodes from tracker
    // int num_conn, num_accept, num_error = 1;
    // do {
    //   // send over good links
    //   std::vector<int> good_link;
    //   for (size_t i = 0; i < all_links.size(); ++i) {
    //     if (!all_links[i].sock.BadSocket()) {
    //       good_link.push_back(static_cast<int>(all_links[i].rank));
    //     } else {
    //       if (!all_links[i].sock.IsClosed()) all_links[i].sock.Close();
    //     }
    //   }
    //   int ngood = static_cast<int>(good_link.size());
    //   Assert(tracker.SendAll(&ngood, sizeof(ngood)) == sizeof(ngood),
    //          "ReConnectLink failure 5");
    //   for (size_t i = 0; i < good_link.size(); ++i) {
    //     Assert(tracker.SendAll(&good_link[i], sizeof(good_link[i])) == \
    //          sizeof(good_link[i]), "ReConnectLink failure 6");
    //   }
    //   Assert(tracker.RecvAll(&num_conn, sizeof(num_conn)) == sizeof(num_conn),
    //          "ReConnectLink failure 7");
    //   Assert(tracker.RecvAll(&num_accept, sizeof(num_accept)) == \
    //        sizeof(num_accept), "ReConnectLink failure 8");
    //   num_error = 0;
    //   for (int i = 0; i < num_conn; ++i) {
    //     LinkRecord r;
    //     int hport, hrank;
    //     std::string hname;
    //     tracker.RecvStr(&hname);
    //     Assert(tracker.RecvAll(&hport, sizeof(hport)) == sizeof(hport),
    //            "ReConnectLink failure 9");
    //     Assert(tracker.RecvAll(&hrank, sizeof(hrank)) == sizeof(hrank),
    //            "ReConnectLink failure 10");

    //     r.sock.Create();
    //     if (!r.sock.Connect(utils::SockAddr(hname.c_str(), hport))) {
    //       num_error += 1;
    //       r.sock.Close();
    //       continue;
    //     }
    //     Assert(r.sock.SendAll(&rank, sizeof(rank)) == sizeof(rank),
    //            "ReConnectLink failure 12");
    //     Assert(r.sock.RecvAll(&r.rank, sizeof(r.rank)) == sizeof(r.rank),
    //            "ReConnectLink failure 13");
    //     utils::Check(hrank == r.rank,
    //                  "ReConnectLink failure, link rank inconsistent");
    //     bool match = false;
    //     for (size_t i = 0; i < all_links.size(); ++i) {
    //       if (all_links[i].rank == hrank) {
    //         Assert(all_links[i].sock.IsClosed(),
    //                "Override a link that is active");
    //         all_links[i].sock = r.sock;
    //         match = true;
    //         break;
    //       }
    //     }
    //     if (!match) all_links.push_back(r);
    //   }
    //   Assert(tracker.SendAll(&num_error, sizeof(num_error)) == sizeof(num_error),
    //          "ReConnectLink failure 14");
    // } while (num_error != 0);
    // // send back socket listening port to tracker
    // Assert(tracker.SendAll(&port, sizeof(port)) == sizeof(port),
    //        "ReConnectLink failure 14");
    // // close connection to tracker
    // tracker.Close();
    // // listen to incoming links
    // for (int i = 0; i < num_accept; ++i) {
    //   LinkRecord r;
    //   r.sock = sock_listen.Accept();
    //   Assert(r.sock.SendAll(&rank, sizeof(rank)) == sizeof(rank),
    //          "ReConnectLink failure 15");
    //   Assert(r.sock.RecvAll(&r.rank, sizeof(r.rank)) == sizeof(r.rank),
    //          "ReConnectLink failure 15");
    //   bool match = false;
    //   for (size_t i = 0; i < all_links.size(); ++i) {
    //     if (all_links[i].rank == r.rank) {
    //       utils::Assert(all_links[i].sock.IsClosed(),
    //                     "Override a link that is active");
    //       all_links[i].sock = r.sock;
    //       match = true;
    //       break;
    //     }
    //   }
    //   if (!match) all_links.push_back(r);
    // }
    // sock_listen.Close();
    // parent_index = -1;
    // // setup tree links and ring structure
    // tree_links.plinks.clear();
    // int tcpNoDelay = 1;
    // for (size_t i = 0; i < all_links.size(); ++i) {
    //   utils::Assert(!all_links[i].sock.BadSocket(), "ReConnectLink: bad socket");
    //   // set the socket to non-blocking mode, enable TCP keepalive
    //   all_links[i].sock.SetNonBlock(true);
    //   all_links[i].sock.SetKeepAlive(true);
    //   if (rabit_enable_tcp_no_delay) {
    //     setsockopt(all_links[i].sock, IPPROTO_TCP,
    //                TCP_NODELAY, reinterpret_cast<void *>(&tcpNoDelay), sizeof(tcpNoDelay));
    //   }
    //   if (tree_neighbors.count(all_links[i].rank) != 0) {
    //     if (all_links[i].rank == parent_rank) {
    //       parent_index = static_cast<int>(tree_links.plinks.size());
    //     }
    //     tree_links.plinks.push_back(&all_links[i]);
    //   }
    //   if (all_links[i].rank == prev_rank) ring_prev = &all_links[i];
    //   if (all_links[i].rank == next_rank) ring_next = &all_links[i];
    // }
    // Assert(parent_rank == -1 || parent_index != -1,
    //        "cannot find parent in the link");
    // Assert(prev_rank == -1 || ring_prev != NULL,
    //        "cannot find prev ring in the link");
    // Assert(next_rank == -1 || ring_next != NULL,
    //        "cannot find next ring in the link");
    return true;
  } catch (const std::exception& e) {
    fprintf(stderr, "failed in ReconnectLink %s\n", e.what());
    return false;
  }
}

/*! \brief singleton method to get engine */
IEngine *GetEngine(void) {
  // if (!rabitinited)
  // {
  //   RABIT_LOG_CALL;
  //   // debug print the stack trace
  //   size_t size_x;
  //   void *array_x[1024];
  //   size_x = backtrace(array_x, 1024);
  //   backtrace_symbols_fd(array_x, size_x, STDOUT_FILENO);
  // }

  ThreadLocalEntry* e = EngineThreadLocal::Get();
  IEngine* ptr = e->engine.get();
  if (ptr == nullptr) {
    utils::Check(!e->initialized, "the rabit has not been initialized");
    return nullptr;
  } else {
    return ptr;
  }
}

// perform in-place allgather, on sendrecvbuf
void Allgather(void *sendrecvbuf_, size_t total_size,
                   size_t slice_begin,
                   size_t slice_end,
                   size_t size_prev_slice,
                   const char* _file,
                   const int _line,
                   const char* _caller) {
  GetEngine()->Allgather(sendrecvbuf_, total_size, slice_begin,
    slice_end, size_prev_slice, _file, _line, _caller);
}

/*
    coll_request_t bcast(void* buf, size_t count,
                         ccl::datatype dtype,
                         size_t root,
                         const ccl::coll_attr* attr = nullptr,
                         const ccl::stream_t& stream = ccl::stream_t());
*/
void Broadcast_(void* buf, size_t count, int root) { RABIT_LOG_CALL;
  // std::cout << "xgbtck count: " << count << std::endl;
  rabit::utils::BufPrint("xgbtck   bc   in :", buf, count);

  // debug print the stack trace
  // size_t size_x;
  // void *array_x[1024];
  // size_x = backtrace(array_x, 1024);
  // backtrace_symbols_fd(array_x, size_x, STDOUT_FILENO);

  if (count<=0) return;
  ThreadLocalEntry* e = EngineThreadLocal::Get();
  auto& communicator = e->engine->GetCommunicator();
  auto& stream = e->engine->GetStream();
  communicator->barrier(stream);
  communicator->bcast(buf, count, ccl::dt_char, root, nullptr, stream)->wait();
  communicator->barrier(stream);
}
/*
    template<class buffer_type, class = typename std::enable_if<ccl::is_native_type_supported<buffer_type>()>::type>
    coll_request_t bcast(buffer_type* buf, size_t count,
                         size_t root,
                         const ccl::coll_attr* attr = nullptr,
                         const ccl::stream_t& stream = ccl::stream_t());
*/
template<typename BufType>
void Broadcast_(BufType* buf, size_t count, int root) { RABIT_LOG_CALL;
  // std::cout << "xgbtck sizeof(BufType): " << sizeof(BufType) << std::endl;
  // std::cout << "xgbtck count: " << count << std::endl;
  rabit::utils::BufPrint("xgbtck   bc   in :", buf, count);

  // debug print the stack trace
  // size_t size_x;
  // void *array_x[1024];
  // size_x = backtrace(array_x, 1024);
  // backtrace_symbols_fd(array_x, size_x, STDOUT_FILENO);

  if (count<=0) return;
  ThreadLocalEntry* e = EngineThreadLocal::Get();
  //ccl::coll_attr attr;
  auto& communicator = e->engine->GetCommunicator();
  auto& stream = e->engine->GetStream();
  communicator->barrier(stream);
  //communicator->bcast(buf, count, root, &attr, stream)->wait();
  communicator->bcast(buf, count, root, nullptr, stream)->wait();
  communicator->barrier(stream);
}

// perform in-place allreduce, on sendrecvbuf
template<typename OP, typename BufType>
void Allreduce_(BufType* buf, size_t count) {  RABIT_LOG_CALL;
  // std::cout << "xgbtck rabitinited: " << rabitinited << std::endl;
  // std::cout << "xgbtck sizeof(BufType): " << sizeof(BufType) << std::endl;
  // std::cout << "xgbtck count: " << count << std::endl;
  rabit::utils::BufPrint("xgbtck   ar   in :", buf, sizeof(BufType)*count);

  // debug print the stack trace
  // size_t size_x;
  // void *array_x[1024];
  // size_x = backtrace(array_x, 1024);
  // backtrace_symbols_fd(array_x, size_x, STDOUT_FILENO);
  // puts("");

  if (!rabitinited) return;
  if (count<=0) return;

  ThreadLocalEntry* e = EngineThreadLocal::Get();
  auto& communicator = e->engine->GetCommunicator();
  auto& stream = e->engine->GetStream();

  ccl::reduction reduce = ccl::reduction::last_value;
  if (std::is_same<OP, rabit::op::Max>::value) {
    reduce = ccl::reduction::max;
  } else if (std::is_same<OP, rabit::op::Min>::value) {
    reduce = ccl::reduction::min;
  } else if (std::is_same<OP, rabit::op::Sum>::value) {
    reduce = ccl::reduction::sum;
  } else {
    utils::Error("Unsupported reduce op");
  }
  //sum = 0, prod = 1, min = 2, max = 3, custom = 4, ...
  const char* reducename[] = {"sum", "prod", "min", "max", "custom", "unknown"};
  // std::cout << "xgbtck reduce: " << (int)reduce << " " << reducename[(int)reduce] << std::endl;

  ccl::datatype dtype = ccl::dt_last_value;
  if ((typeid(BufType)==typeid(char)) || (typeid(BufType)==typeid(unsigned char))) {
    dtype = ccl::dt_char;
  } else if (typeid(BufType)==typeid(int) || typeid(BufType)==typeid(unsigned int)) {
    dtype = ccl::dt_int;
  } else if (typeid(BufType)==typeid(float)) {
    dtype = ccl::dt_float;
  } else if (typeid(BufType)==typeid(double)) {
    dtype = ccl::dt_double;
  } else if (typeid(BufType)==typeid(long)) {
    dtype = ccl::dt_int64;
  } else if (typeid(BufType)==typeid(unsigned long)) {
    dtype = ccl::dt_uint64;
  } else {
    utils::Error("Unsupported reduce datatype");
  }
  const char* dtypename[] = {"char", "int", "bfp16", "float", "double", "int64", "uint64", "unknown"};
  // std::cout << "xgbtck dtype: " << dtype << " " << dtypename[dtype] << std::endl;

//  std::vector<BufType> recvbuf(count);
//  std::vector<char> recvbuf(sizeof(BufType*count));
  communicator->barrier(stream);
/*
    template<class buffer_type, class = typename std::enable_if<ccl::is_native_type_supported<buffer_type>()>::type>
    coll_request_t allreduce(const buffer_type* send_buf,
                             buffer_type* recv_buf,
                             size_t count,
                             ccl::reduction reduction,
                             const ccl::coll_attr* attr = nullptr,
                             const ccl::stream_t& stream = ccl::stream_t());
    ccl::communicator::coll_request_t CCL_API
    ccl::communicator::allreduce(const void* send_buf,
                             void* recv_buf,
                             size_t count,
                             ccl::datatype dtype,
                             ccl::reduction reduction,
                             const ccl::coll_attr* attr,
                             const ccl::stream_t& stream);
*/
//  std::cout << "xgbtck Allreduce_() engine_occl.cc, ";
  // std::cout << "xgbtck Allreduce_() communicator->allreduce()..." << std::endl;
//  communicator->allreduce(buf, recvbuf.data(), count, reduce, nullptr, stream)->wait();
//  communicator->allreduce(buf, (void *)recvbuf.data(), count, dtype, reduce, nullptr, stream)->wait();
  communicator->allreduce(buf, buf, count, dtype, reduce, nullptr, stream)->wait();
  // std::cout << "xgbtck Allreduce_() ...communicator->allreduce()" << std::endl;
  communicator->barrier(stream);
//  std::copy(recvbuf.cbegin(), recvbuf.cend(), buf);
  rabit::utils::BufPrint("xgbtck   ar   out:", buf, sizeof(BufType)*count);
}

template void Broadcast_<double>(double*, size_t, int);
template void Broadcast_<float>(float*, size_t, int);
template void Broadcast_<char>(char*, size_t, int);

template void Allreduce_<rabit::op::Max, double>(double*, size_t);
template void Allreduce_<rabit::op::Max, float>(float*, size_t);
template void Allreduce_<rabit::op::Max, unsigned long>(unsigned long*, size_t);
template void Allreduce_<rabit::op::Max, long>(long*, size_t);
template void Allreduce_<rabit::op::Max, unsigned int>(unsigned int*, size_t);
template void Allreduce_<rabit::op::Max, int>(int*, size_t);
template void Allreduce_<rabit::op::Max, unsigned char>(unsigned char*, size_t);
template void Allreduce_<rabit::op::Max, char>(char*, size_t);

template void Allreduce_<rabit::op::Min, double>(double*, size_t);
template void Allreduce_<rabit::op::Min, float>(float*, size_t);
template void Allreduce_<rabit::op::Min, unsigned long>(unsigned long*, size_t);
template void Allreduce_<rabit::op::Min, long>(long*, size_t);
template void Allreduce_<rabit::op::Min, unsigned int>(unsigned int*, size_t);
template void Allreduce_<rabit::op::Min, int>(int*, size_t);
template void Allreduce_<rabit::op::Min, unsigned char>(unsigned char*, size_t);
template void Allreduce_<rabit::op::Min, char>(char*, size_t);

template void Allreduce_<rabit::op::Sum, double>(double*, size_t);
template void Allreduce_<rabit::op::Sum, float>(float*, size_t);
template void Allreduce_<rabit::op::Sum, unsigned long>(unsigned long*, size_t);
template void Allreduce_<rabit::op::Sum, long>(long*, size_t);
template void Allreduce_<rabit::op::Sum, unsigned int>(unsigned int*, size_t);
template void Allreduce_<rabit::op::Sum, int>(int*, size_t);
template void Allreduce_<rabit::op::Sum, unsigned char>(unsigned char*, size_t);
template void Allreduce_<rabit::op::Sum, char>(char*, size_t);

template void Allreduce_<rabit::op::BitOR, double>(double*, size_t);
template void Allreduce_<rabit::op::BitOR, float>(float*, size_t);
template void Allreduce_<rabit::op::BitOR, unsigned long>(unsigned long*, size_t);
template void Allreduce_<rabit::op::BitOR, long>(long*, size_t);
template void Allreduce_<rabit::op::BitOR, unsigned int>(unsigned int*, size_t);
template void Allreduce_<rabit::op::BitOR, int>(int*, size_t);
template void Allreduce_<rabit::op::BitOR, unsigned char>(unsigned char*, size_t);
template void Allreduce_<rabit::op::BitOR, char>(char*, size_t);


// code for reduce handle
ReduceHandle::ReduceHandle(void)
    : handle_(NULL), redfunc_(NULL), htype_(NULL) { RABIT_LOG_CALL;
}

ReduceHandle::~ReduceHandle(void) { RABIT_LOG_CALL;
//void datatype_free(ccl::datatype dtype);
  datatype_free(*(ccl::datatype *)htype_);
}

int ReduceHandle::TypeSize(const ccl::datatype &dtype) { RABIT_LOG_CALL;
  return datatype_get_size(dtype);
}

void ReduceHandle::Init(ccl_reduction_fn_t cclredfunc, size_t type_nbytes) { RABIT_LOG_CALL;

  std::cout << "xgbtck ReduceHandle::Init... OCCL" << std::endl;
  // store the custom reduce function pointer
  redfunc_ = cclredfunc;

  if (type_nbytes != 0) {
    // store the custom datatype
    ccl::datatype_attr dt_attr = { .size = type_nbytes };
    std::cout << "xgbtck dt_attr.size: " << dt_attr.size << std::endl;
    //ccl::datatype datatype_create(const ccl::datatype_attr* attr);
    ccl::datatype dtype_custom = ccl::datatype_create(&dt_attr);
    //std::cout << "xgbtck dtype_custom: " << dtype_custom << std::endl;
    htype_ = (void *)(new ccl::datatype(dtype_custom));
    created_type_nbytes_ = type_nbytes;
    std::cout << "xgbtck *htype_: " << *(ccl::datatype *)htype_ << std::endl;
  }
}

void coll_attr_set_default(ccl::coll_attr *coll_atr)
{
    coll_atr->prologue_fn = NULL;
    coll_atr->epilogue_fn = NULL;
    coll_atr->reduction_fn = NULL;
    coll_atr->priority = 0;
    coll_atr->synchronous = 0;
    coll_atr->to_cache = 0;
    coll_atr->vector_buf = 0;
    coll_atr->match_id = NULL;
}

void ReduceHandle::Allreduce(void *sendrecvbuf,
                             size_t type_nbytes, size_t count,
                             IEngine::PreprocFunction prepare_fun,
                             void *prepare_arg,
                             const char* _file,
                             const int _line,
                             const char* _caller) { RABIT_LOG_CALL;

//  std::cout << "xgbtck ReduceHandle::Allreduce() OCCL" << std::endl;

  if (prepare_fun != NULL)
    prepare_fun(prepare_arg);

  // std::cout << "xgbtck type_nbytes: " << type_nbytes << std::endl;
  // std::cout << "xgbtck count: " << count << std::endl;
  rabit::utils::BufPrint("xgbtck rhar   in :", sendrecvbuf, type_nbytes*count);

  if (created_type_nbytes_ != type_nbytes || htype_ == NULL) {
    if (htype_ != NULL) {
      datatype_free(*(ccl::datatype *)htype_);
    }
    // store the custom datatype
    ccl::datatype_attr dt_attr = { .size = type_nbytes };
    // std::cout << "xgbtck new dt_attr.size: " << dt_attr.size << std::endl;
    //ccl::datatype datatype_create(const ccl::datatype_attr* attr);
    ccl::datatype dtype_custom = ccl::datatype_create(&dt_attr);
    //std::cout << "xgbtck dtype_custom: " << dtype_custom << std::endl;
    htype_ = (void *)(new ccl::datatype(dtype_custom));
    created_type_nbytes_ = type_nbytes;
  }

  ThreadLocalEntry* e = EngineThreadLocal::Get();
  auto& communicator = e->engine->GetCommunicator();
  auto& stream = e->engine->GetStream();
  // build attr with ccl_reduction_fn, prepare_fun, etc
  ccl::coll_attr attr;
  coll_attr_set_default(&attr);
  attr.reduction_fn = redfunc_;
//   attr.prologue_fn = 
// ccl_status_t do_prologue_2x(const void* in_buf, size_t in_count, ccl_datatype_t in_dtype,
//                             void** out_buf, size_t* out_count, ccl_datatype_t* out_dtype,
//                             const ccl_fn_context_t* context)

  // debug print the stack trace
  // size_t size_x;
  // void *array_x[1024];
  // size_x = backtrace(array_x, 1024);
  // backtrace_symbols_fd(array_x, size_x, STDOUT_FILENO);
  // puts("");

//  const char* dtypename[] = {"char", "int", "bfp16", "float", "double", "int64", "uint64", "unknown"};
//  std::cout << "xgbtck dtype: " << dtype << " " << dtypename[dtype] << std::endl;
  // std::cout << "xgbtck htype_: " << *(ccl::datatype *)htype_ << std::endl;
  // call allreduce() with attr as a parameter
  communicator->barrier(stream);
  // TODO: try to use buffer_type template allreduce() later
  // std::cout << "xgbtck communicator->allreduce()..." << std::endl;
  communicator->allreduce(sendrecvbuf, sendrecvbuf, count,
                          (*(ccl::datatype *)htype_),
                          ccl::reduction::custom,
                          &attr,
                          stream)->wait();
  // std::cout << "xgbtck ...communicator->allreduce()" << std::endl;
  communicator->barrier(stream);
  rabit::utils::BufPrint("xgbtck rhar   out:", sendrecvbuf, type_nbytes*count);

} // ReduceHandle::Allreduce()
}  // namespace engine
}  // namespace rabit
