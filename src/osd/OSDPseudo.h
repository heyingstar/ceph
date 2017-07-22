#ifndef CEPH_OSDPSEUDO_H
#define CEPH_OSDPSEUDO_H

#include "common/Timer.h"
#include "common/Mutex.h"
using namespace std;

class OSDPseudo {
  mutable Mutex lock;
  Cond cond;
  SafeTimer  timer;
  
  // Ticker
  class C_OSDPseudo_Tick : public Context {
      OSDPseudo *pseudo;
  public:
    explicit C_OSDPseudo_Tick(OSDPseudo *p) : pseudo(p) {}
    void finish(int r) {
      assert(pseudo->lock.is_locked_by_me());
      pseudo->tick_event = 0;
      pseudo->tick();
    }
  } *tick_event;
  
  void reset_timer();
  void tick();

  enum {
    NOT_STOPPING = 0,
    STOPPING,
    STOPPED
  };
  atomic_t state;
  
  string osdid;
  string adminid;
  string admin_asok_path;
  int start_times;
  int try_times;
  
  int do_command(const char *cmd, string *result);
  int osd_start();
  int osd_stop();
  int osd_status();
  
  atomic_t osd_state;
  
public:
  OSDPseudo();
  ~OSDPseudo();
  
  int init();
  void start();
  void final_init(){};
  void wait();
  
  void handle_signal(int signum);
  int get_osd_state(){return osd_state.read();}
  
};

#endif
