/*!
 *  Copyright (c) 2020 by Contributors
 * \file engine_occl.h
 * \brief this file provides a implementation of engine that uses OneCCL
 *  as the backend.
 */
#ifndef RABIT_INTERNAL_ENGINE_OCCL_H_
#define RABIT_INTERNAL_ENGINE_OCCL_H_
#include <ccl.hpp>
#include <unistd.h>
#include <execinfo.h>
#include "rabit/internal/engine.h"
#include "rabit/internal/utils.h"
#include "rabit/internal/socket.h"

namespace MPI {
// MPI data type to be compatible with existing MPI interface
class Datatype {
 public:
  size_t type_size;
  explicit Datatype(size_t type_size) : type_size(type_size) {}
};
}

namespace rabit {
namespace engine {

static const int kMagic = 0xff99;

utils::TCPSocket ConnectTracker(void);

/*! \brief OneCCLEngine */
class OneCCLEngine : public IEngine {
 public:
  OneCCLEngine(void) {}

  virtual void Allgather(void *sendrecvbuf,
                         size_t total_size,
                         size_t slice_begin,
                         size_t slice_end,
                         size_t size_prev_slice,
                         const char* _file = _FILE,
                         const int _line = _LINE,
                         const char* _caller = _CALLER) {
    utils::Error("OneCCLEngine:: Allgather is not supported,"\
                 "use Allgather_ instead");
  }

  virtual void Allreduce(void *sendrecvbuf_,
                         size_t type_nbytes,
                         size_t count,
                         ReduceFunction reducer,
                         PreprocFunction prepare_fun,
                         void *prepare_arg,
                         const char* _file,
                         const int _line,
                         const char* _caller) {
    // debug print the stack trace
    size_t size_x;
    void *array_x[1024];
    size_x = backtrace(array_x, 1024);
    backtrace_symbols_fd(array_x, size_x, STDOUT_FILENO);
    // puts("");
    utils::Error("OneCCLEngine:: Allreduce is not supported,"\
                 "use Allreduce_ instead");
  }
  virtual void Broadcast(void *sendrecvbuf_, size_t size, int root,
                         const char* _file, const int _line, const char* _caller) {
    // debug print the stack trace
    size_t size_x;
    void *array_x[1024];
    size_x = backtrace(array_x, 1024);
    backtrace_symbols_fd(array_x, size_x, STDOUT_FILENO);
    // puts("");
    utils::Error("OneCCLEngine:: Broadcast is not supported,"\
                 "use Broadcast_ instead");
  }
  virtual void InitAfterException(void) {
    utils::Error("OneCCLEngine fault tolerance not implemented");
  }
  virtual int LoadCheckPoint(Serializable *global_model,
                             Serializable *local_model = NULL) {
    return 0;
  }
  virtual void CheckPoint(const Serializable *global_model,
                          const Serializable *local_model = NULL) {
    version_number += 1;
  }
  virtual void LazyCheckPoint(const Serializable *global_model) {
    version_number += 1;
  }
  virtual int VersionNumber(void) const {
    return version_number;
  }
  virtual int GetRingPrevRank(void) const {
  	return 0;
  }
  /*! \brief get rank of current node */
  virtual int GetRank(void) const {
    int r = static_cast<int>(communicator->rank());
    // std::cout << "xgbtck rabitoccl rank: " << r << std::endl;
    return r;
  }
  /*! \brief get total number of */
  virtual int GetWorldSize(void) const {
    return static_cast<int>(communicator->size());
  }
  /*! \brief whether it is distributed */
  virtual bool IsDistributed(void) const {
    return true;
  }
  /*! \brief get the host name of current node */
  virtual std::string GetHost(void) const {
    char hostname[64];
    gethostname(hostname, 64);
    return std::string(hostname);
  }
  virtual void TrackerPrint(const std::string &msg) {
    // simply print information into the tracker
    std::cout << msg << std::endl;
    // utils::Printf("%s", msg.c_str());
    // if (tracker_uri == "NULL") {
    //   utils::Printf("%s", msg.c_str());
    //   return;
    // }
    utils::TCPSocket tracker = engine::ConnectTracker();
    tracker.SendStr(std::string("print"));
    tracker.SendStr(msg);
    tracker.Close();
  }
  ccl::communicator_t& GetCommunicator() {
    return communicator;
  }
  ccl::stream_t& GetStream() {
    return stream;
  }

 private:
  int version_number{0};
  ccl::stream_t stream{ccl::environment::instance().create_stream()};
  ccl::communicator_t communicator{ccl::environment::instance().create_communicator()};
};  //class OneCCLEngine

/*! \brief intiialize the synchronization module */
bool Init(int argc, char *argv[]);

/*! \brief finalize syncrhonization module */
bool Finalize(void);

void SetParam(const char *name, const char *val);

// utils::TCPSocket ConnectTracker(void);
bool ReConnectLinks(const char *cmd = "start");

/*! \brief singleton method to get engine */
IEngine *GetEngine(void);

void Broadcast_(void* buf, size_t count, int root);
template<typename BufType>
void Broadcast_(BufType* buf, size_t count, int root);

// perform in-place allreduce, on sendrecvbuf
template<typename OP, typename BufType>
void Allreduce_(BufType* buf, size_t count); 

}  // namespace engine
}  // namespace rabit
#endif  // RABIT_INTERNAL_ENGINE_OCCL_H_
