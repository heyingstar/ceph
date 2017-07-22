#ifndef CEPH_OSDMANAGER_H
#define CEPH_OSDMANAGER_H

#include "common/admin_socket.h"

class OSDMessengers;
class OSDInstance;
class ClassHandler;

typedef ceph::shared_ptr<OSDInstance> OSDInstanceRef;

class OSDManager : public AdminSocketHook {
  vector<const char*> args;
  vector<const char*> def_args;
  string conf_file_list;

  void preprocess_args();

  CephContext *cct;
  md_config_t *conf;
  OSDMessengers *messengers;
  ClassHandler *class_handler;

  Mutex lock;
  Cond cond;

  enum {
    NOT_STOPPING = 0,
    STOPPING,
    STOPPED
  };
  atomic_t state;

  bool is_stopping() {
    return state.read() == STOPPING;
  }

  Mutex instance_lock;
  map<int, OSDInstanceRef> instance_map;

  int start_osd(int id);
  int get_osd_status(int id, std::string& msg);
  void list_all_osds(stringstream& ss);
  void stop_all_osds();

public:
  // return value of 'osd status <id>' command
  enum {
    OSD_STATUS_NORMAL = 0, // running or stopping
    OSD_STATUS_NOT_EXIST, // not exist
    OSD_STATUS_STOPPED, // normally stopped
    OSD_STATUS_FAILED, // stopped for failure
    OSD_STATUS_STARTING, // in starting process
    OSD_STATUS_START_FAILED // start failed
  };

  OSDManager(vector<const char *>& _args, vector<const char *>& _def_args);
  ~OSDManager();

  int init();
  void start();
  void final_init();
  void wait();
  int shutdown();

  void handle_signal(int signum);

  bool call(string cmd, cmdmap_t& cmdmap, string fmt, bufferlist& out);

  bool share_messengers() const {
    return conf->osd_share_messengers;
  }

  void copy_args(vector<const char *>& _args, vector<const char *>& _def_args,
    string& _conf_file_list) const {
    _args = args;
    _def_args = def_args;
    _conf_file_list = conf_file_list;
  }

  ClassHandler *get_class_handler() const {
    return class_handler;
  }

  int stop_osd(int id, bool from_pseudo);
};

extern OSDManager *g_osd_manager;

#endif