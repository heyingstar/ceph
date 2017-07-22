#ifndef CEPH_OSDINSTANCE_H
#define CEPH_OSDINSTANCE_H

#include "include/utime.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "global/global_context.h"

class OSDMessengers;
class MonClient;
class ObjectStore;
class OSD;

typedef ceph::shared_ptr<OSD> OSDRef;

class OSDInstance : public FailureHandler, public md_config_obs_t {
  int id;

  CephContext *cct;
  md_config_t *conf;
  ObjectStore *store;
  OSDMessengers *messengers;
  bool local_msgrs;
  MonClient *monc;
  OSDRef osd;

  enum {
    STATE_PREPARING = 0,
    STATE_STARTING,
    STATE_RUNNING,
    STATE_STOPPING,
    STATE_STOPPED
  };

  static const char *get_state_name(int _state) {
    switch (_state) {
    case STATE_PREPARING: return "preparing";
    case STATE_STARTING: return "starting";
    case STATE_RUNNING: return "running";
    case STATE_STOPPING: return "stopping";
    default: return "stopped"; // STATE_STOPPED
    }
  }

  Mutex lock; // to protect cct, osd and last_tx
  Cond cond;
  atomic_t state;
  bool start_ok; // return value of _start()
  utime_t last_tx; // time of last pseudo command

  void update_last_tx() {
    Mutex::Locker l(lock);
    last_tx = ceph_clock_now(g_ceph_context);
  }

  int init_cct(ostringstream& err_msg);
  int create_store(ostringstream& err_msg);

  bool _start();
  void _wait();
  void _clean_up();

  class OSDThread : public Thread {
    OSDInstance *instance;
  public:
    explicit OSDThread(OSDInstance *i) : instance(i) {}
    void *entry() {
      instance->osd_thread_entry();
      return nullptr;
    }
  } osd_thread;

  char thread_name[16];
  void osd_thread_entry();

public:
  OSDInstance(int _id, OSDMessengers *_messengers);
  ~OSDInstance();

  int start();
  int stop(bool from_pseudo);
  int get_status(string& msg);
  bool is_stopped() {
    return STATE_STOPPED == state.read();
  }

  const char** get_tracked_conf_keys() const;
  void handle_conf_change(const struct md_config_t *conf,
    const std::set <std::string> &changed);
  void handle_failure(int err, const char *desc);
};

#endif
