#include "common/errno.h"
#include "common/debug.h"
#include "common/ceph_argparse.h"
#include "global/signal_handler.h"
#include "common/admin_socket_client.h"

#include "OSDPseudo.h"
#include "OSDManager.h"

#define dout_subsys ceph_subsys_osd

OSDPseudo::OSDPseudo():
  lock("OSDPseudo:lock"),
  timer(g_ceph_context, lock),
  state(NOT_STOPPING),
  osdid(""),
  adminid(""),
  admin_asok_path(""),
  start_times(0),
  try_times(0),
  osd_state(0)
{
  tick_event = NULL;
}

OSDPseudo::~OSDPseudo()
{   
}

int OSDPseudo::init()
{
  //0 osd id
  //0.2 0: admin id, 2 osd id
  string idparam = g_conf->name.get_id();
  
  int n = idparam.find('.');
  if(n<0){
    osdid = idparam;
  } else {
    adminid = idparam.substr(0, n);
    osdid = idparam.substr(n+1);
  }
    
  char buf[255] = {0};
 
  if(adminid.length()>0){
    sprintf(buf, "-osd.admin.%s.asok", adminid.c_str());
  } else {
    sprintf(buf, "-osd.admin.asok");
  }
  
  admin_asok_path = g_conf->run_dir + "/" + g_conf->cluster + buf;
  dout(10) << "admin asok path: " << admin_asok_path << dendl;
  
  timer.init();
  
  return 0;
}

void OSDPseudo::start()
{
  for(int i=0;i<3;i++){
    if(0 == osd_start())break;
    sleep(10);
  }
  lock.Lock();
  reset_timer();
  lock.Unlock();
}

void OSDPseudo::wait()
{
  lock.Lock();
  while (state.read() != STOPPED)
    cond.Wait(lock);

  if (tick_event) {
    timer.cancel_event(tick_event);
    tick_event = NULL;
  }
  timer.shutdown();
  
  lock.Unlock();
}

int OSDPseudo::do_command(const char *cmd, string *result)
{
  char buf[255] = {0};
  sprintf(buf, "{\"prefix\": \"%s\", \"id\": %s}", cmd, osdid.c_str());
  string request(buf);

  dout(10) << "request: " << request << dendl;
  AdminSocketClient asok_client(admin_asok_path);
  string errmsg = asok_client.do_request(request, result);
  dout(10) << "result: " << *result << dendl;
  if(errmsg != ""){
      dout(10) << "errno: " << errno << ", return: " << errmsg << dendl;
      return errno;
  }

  return 0;
}

int OSDPseudo::osd_start()
{
  string result;
  int err = do_command("osd start", &result);
  if(0 != err) {
      return -1;
  }
  if(0 == result.compare(0, strlen("1"), "1")) {
      return 1; // already started
  }
  if(0 == result.compare(0, strlen("2"), "2")) {
      return 2; // fail
  }
  return 0; // success
}

int OSDPseudo::osd_stop()
{
  string result;
  int err = do_command("osd stop", &result);
  if(0 != err) {
      return -1;
  }
  if(0 == result.compare(0, strlen("1"), "1")) {
      return 1; // fail
  }
  return 0;  // success or already stopped
}

int OSDPseudo::osd_status()
{
  string result;
  int err = do_command("osd status", &result);
  if(11 == err) {//Resource temporarily unavailable 超时
    return -1;
  }    
  size_t len = strlen("val=");
  if(result.length() < len){ //admin未启动等
    return -2;
  }
  return atoi(result.substr(len).c_str());
}

void OSDPseudo::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** osdpseudo." << osdid << " got signal " << sig_str(signum) << " ***" << dendl;
  osd_stop();
  lock.Lock();
  state.set(STOPPING);
  lock.Unlock();
  derr << "osdpseudo." << osdid << " state stopping" << dendl;
  // waiting for the real osd stopping
#if 0  
  lock.Lock();
  state.set(STOPPED);
  cond.Signal();
  lock.Unlock();
#endif

}

void OSDPseudo::reset_timer()
{
  if (tick_event) {
    timer.cancel_event(tick_event);
  }

  tick_event = new C_OSDPseudo_Tick(this);
  timer.add_event_after(g_conf->osd_pseudo_tick_interval, tick_event); 
}

void OSDPseudo::tick()
{
  int r = osd_status();
  //derr << "tick osd." << osdid << " status:" << r << dendl;
  if(OSDManager::OSD_STATUS_NORMAL == r) {
    start_times = 0;
    try_times = 0;
  } else if(-2 == r) { //admin未启动或重启间隙
    try_times++;
    if(try_times>=2) {
      state.set(STOPPED);
      cond.Signal();
      return;
    }
  } else if(OSDManager::OSD_STATUS_STOPPED == r) {
    derr << "tick osd." << osdid << " stopping" << dendl;
    state.set(STOPPED);
    cond.Signal();
    return;
  } else if(OSDManager::OSD_STATUS_NOT_EXIST == r) {
    osd_start();
  } else if(OSDManager::OSD_STATUS_FAILED == r) {
    if(g_conf->osd_pseudo_restart_osd) {
      derr << "tick osd." << osdid << " restart" << dendl;
      if(start_times >= 3) {
        state.set(STOPPED);
        cond.Signal();
        return;
      }
      osd_start();
      start_times++;
    } else {
      derr << "tick osd." << osdid << " exception stopping" << dendl;
      state.set(STOPPED);
      cond.Signal();
      return;
    }
  } else if(OSDManager::OSD_STATUS_START_FAILED == r) {
      derr << "tick osd." << osdid << " start failed" << dendl;
      osd_state.set(r);
      state.set(STOPPED);
      cond.Signal();
      return;
  } else if(-1 == r) { //超时
  }
  
  reset_timer();
}


