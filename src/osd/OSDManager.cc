#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/signal_handler.h"
#include "include/str_list.h"

#include "OSDMessengers.h"
#include "OSDInstance.h"
#include "OSDManager.h"
#include "ClassHandler.h"

#define dout_subsys ceph_subsys_osd_admin
#undef dout_prefix
#define dout_prefix *_dout << __func__ << " "

OSDManager *g_osd_manager = nullptr;

OSDManager::OSDManager(vector<const char *>& _args, vector<const char *>& _def_args) :
  cct(g_ceph_context),
  conf(g_ceph_context->_conf),
  messengers(nullptr),
  class_handler(nullptr),
  lock("OSDManager:lock"),
  state(NOT_STOPPING),
  instance_lock("OSDManager:instance_lock")
{
  args.swap(_args);
  def_args.swap(_def_args);
  if (conf->osd_share_messengers)
    messengers = new OSDMessengers(-1, cct);
}

OSDManager::~OSDManager()
{
  instance_map.clear();
  if (class_handler)
    delete class_handler;
  if (messengers)
    delete messengers;
}

void OSDManager::preprocess_args()
{
  dout(20) << "orig_args: " << args << dendl;

  string val;
  for (auto i = args.begin(); i != args.end(); ) {
    if (strcmp(*i, "--") == 0) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--conf", "-c", nullptr)) {
      dout(10) << "conf_file_list: " << val << dendl;
      conf_file_list = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--cluster", nullptr)) {
      // ignore, use g_conf->cluster directly
    } else if (ceph_argparse_witharg(args, i, &val, "--name", "-n", nullptr)) {
      // ignore, use "osd" directly
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--id", nullptr)) {
      // ignore, this is the id of admin process, not the id of osd
    } else if (ceph_argparse_flag(args, i, "--show_args", nullptr)) {
      // ignore, has done in global_init()
    } else if (ceph_argparse_witharg(args, i, &val, "--setuser", nullptr)) {
      // ignore, has done in global_init()
    } else if (ceph_argparse_witharg(args, i, &val, "--setgroup", nullptr)) {
      // ignore, has done in global_init()
    } else {
      ++i;
    }
  }

  dout(10) << "args: " << args << dendl;
}

int OSDManager::start_osd(int id)
{
  Mutex::Locker l(lock);
  if (is_stopping())
    return -EINTR;

  Mutex::Locker il(instance_lock);
  OSDInstanceRef& instance = instance_map[id];
  if (!instance) {
    dout(5) << "add osd." << id << " to map" << dendl;
    instance.reset(new OSDInstance(id, messengers));
  }
  return instance->start();
}

int OSDManager::stop_osd(int id, bool from_pseudo)
{
  if (is_stopping())
    return -EINTR;

  Mutex::Locker l(instance_lock);
  auto itr = instance_map.find(id);
  if (itr != instance_map.end()) {
    if (itr->second->is_stopped()) {
      dout(5) << "remove osd." << id << " from map" << dendl;
      instance_map.erase(itr);
      return -ENOENT;
    } else {
      return itr->second->stop(from_pseudo);
    }
  } else {
    return -ENOENT;
  }
}

int OSDManager::get_osd_status(int id,  string& msg)
{
  Mutex::Locker l(instance_lock);
  auto itr = instance_map.find(id);
  if (itr != instance_map.end()) {
    int ret = itr->second->get_status(msg);
    if (ret == OSD_STATUS_STOPPED || 
        ret == OSD_STATUS_FAILED ||
        ret == OSD_STATUS_START_FAILED) {
      dout(5) << "remove osd." << id << " from map" << dendl;
      instance_map.erase(itr);
    }
    return ret;
  } else {
    msg = "not exist";
    return OSD_STATUS_NOT_EXIST;
  }
}

void OSDManager::list_all_osds(stringstream& ss)
{
  if (is_stopping())
    ss << "admin is stopping" << std::endl;

  string msg;
  Mutex::Locker l(instance_lock);
  ss << "totally " << instance_map.size() << " osds";
  auto itr = instance_map.begin();
  while (itr != instance_map.end()) {
    int ret = itr->second->get_status(msg);
    ss << std::endl << "  osd." << itr->first << " " << msg;
    if (ret == OSD_STATUS_STOPPED || 
        ret == OSD_STATUS_FAILED ||
        ret == OSD_STATUS_START_FAILED) {
      dout(5) << "remove osd." << itr->first << " from map" << dendl;
      instance_map.erase(itr++);
    } else {
      ++itr;
    }
  }
}

void OSDManager::stop_all_osds()
{
  dout(5) << dendl;
  int timeout = conf->osd_admin_stop_osds_timeout;
  if (timeout <= 0)
    timeout = -1; // infinite

  while (true) {
    bool all_stopped(true);
    instance_lock.Lock();
    auto itr = instance_map.begin();
    while (itr != instance_map.end()) {
      if (itr->second->is_stopped()) {
        dout(5) << "remove osd." << itr->first << " from map" << dendl;
        instance_map.erase(itr++);
      } else {
        all_stopped = false;
        itr->second->stop(false);
        ++itr;
      }
    }
    instance_lock.Unlock();

    if (all_stopped) {
      dout(5) << "done" << dendl;
      return;
    }

    if (timeout == 0)
      break;

    utime_t(1, 0).sleep();
    if (timeout > 0)
      --timeout;
  }

  string msg;
  instance_lock.Lock();
  derr << instance_map.size() << " osds still active" << dendl;
  for (auto & i: instance_map) {
    i.second->get_status(msg);
    derr << "  osd." << i.first << " " << msg << dendl;
  }
  instance_lock.Unlock();

  assert(0 == "stop_all_osds timed out");
}

void cls_initialize(ClassHandler *ch);

void handle_sigquit(int signum) {
  BackTrace *bt = new BackTrace(1);
  derr << "*** Got signal " << sig_str(signum) << " ***" << std::endl;
  bt->print(*_dout);
  *_dout << dendl;
  delete bt;
}

int OSDManager::init()
{
  if (messengers) {
    ostringstream out;
    int ret = messengers->init(out);
    if (ret < 0) {
      derr << out.str() << dendl;
      return ret;
    }
  }

  preprocess_args();

  class_handler = new ClassHandler(cct);
  cls_initialize(class_handler);
  if (conf->osd_open_classes_on_start) {
    int ret = class_handler->open_all_classes();
    if (ret) {
      dout(1) << "warning: got an error loading one or more classes: "
           << cpp_strerror(ret) << dendl;
    }
  }

  install_sighandler(SIGQUIT, handle_sigquit, 0);
  return 0;
}

void OSDManager::start()
{
  if (messengers)
    messengers->start();
}

void OSDManager::final_init()
{
  AdminSocket *asok = cct->get_admin_socket();
  int r = asok->register_command("osd start",
      "osd start name=id,type=CephInt,range=0", 
      this, "start osd <id>");
  assert(r == 0);
  r = asok->register_command("osd stop",
      "osd stop name=id,type=CephInt,range=0",
      this, "stop osd <id>");
  assert(r == 0);
  r = asok->register_command("osd status",
      "osd status name=id,type=CephInt,range=0",
      this, "low-level status of osd <id>");
  assert(r == 0);
  r = asok->register_command("osd ls", "osd ls", this, "list all osds");
  assert(r == 0);
}

void OSDManager::wait()
{
  lock.Lock();
  while (state.read() != STOPPED)
    cond.Wait(lock);
  lock.Unlock();

  if (messengers)
    messengers->wait();
}

int OSDManager::shutdown()
{
  lock.Lock();
  if (state.read() != NOT_STOPPING) {
    lock.Unlock();
    return 0;
  }
  state.set(STOPPING);
  lock.Unlock();

  derr << dendl;

  AdminSocket *asok = cct->get_admin_socket();
  asok->unregister_command("osd start");
  asok->unregister_command("osd stop");

  stop_all_osds();
  asok->unregister_command("osd status");
  asok->unregister_command("osd ls");

  class_handler->shutdown();
  if (messengers)
    messengers->shutdown();

  Mutex::Locker l(lock);
  state.set(STOPPED);
  cond.Signal();
  return 0;
}

void OSDManager::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got signal " << sig_str(signum) << " ***" << dendl;
  shutdown();
}

bool OSDManager::call(string cmd, cmdmap_t& cmdmap, string fmt, bufferlist& out)
{
  int64_t id(-1);
  cmd_getval(cct, cmdmap, "id", id);
  if (id != -1)
    dout(10) << cmd << " " << id << dendl;

  stringstream ss;
  if (cmd == "osd start") {
    int ret = start_osd(id);
    if (ret >= 0)
      ret = 0; // success
    else if (ret == -EEXIST)
      ret = 1; // already started
    else
      ret = 2; // fail
    ss << ret;
  } else if (cmd == "osd stop") {
    int ret = stop_osd(id, true);
    if (ret >= 0 || ret == -ENOENT)
      ret = 0; // success or already stopped
    else
      ret = 1; // fail
    ss << ret;
  } else if (cmd == "osd status") {
    string msg;
    int ret = get_osd_status(id, msg);
    ss << "val=" << ret << ",msg=" << msg;
  } else if (cmd == "osd ls") {
    list_all_osds(ss);
  } else {
    assert(0 == "broken asok registration");
  }

  if (id != -1)
    dout(10) << cmd << " " << id << " return [" << ss.str() << "]" << dendl;
  out.append(ss);
  return true;
}

