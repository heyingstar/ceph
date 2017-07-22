#include "common/common_init.h"
#include "common/errno.h"
#include "common/io_priority.h"
#include "common/version.h"

#include "mon/MonClient.h"
#include "OSD.h"
#include "OSDMessengers.h"
#include "OSDInstance.h"
#include "OSDManager.h"
#include "global/signal_handler.h"

#define dout_subsys ceph_subsys_osd_admin
#undef dout_prefix
#define dout_prefix *_dout << "osd." << id << " " << __func__ << " "

OSDInstance::OSDInstance(int _id, OSDMessengers *_messengers) :
  id(_id),
  cct(nullptr),
  conf(nullptr),
  store(nullptr),
  messengers(_messengers),
  local_msgrs(!_messengers),
  monc(nullptr),
  lock("OSDInstance:lock"),
  state(STATE_STOPPED),
  start_ok(true),
  osd_thread(this)
{
  snprintf(thread_name, sizeof(thread_name), "osd.%d", id);
}

OSDInstance::~OSDInstance()
{
  assert(!cct);
  assert(!store);
  assert(!local_msgrs || !messengers);
  assert(!monc);
  assert(!osd);
}

int OSDInstance::init_cct(ostringstream& err_msg)
{
  cct = new CephContext(CEPH_ENTITY_TYPE_OSD, 0);
  conf = cct->_conf;
  conf->add_observer(this);

  ostringstream ss;
  ss << id;
  conf->name.set(CEPH_ENTITY_TYPE_OSD, ss.str());
  conf->data_dir_option = "osd_data";
  conf->cluster = g_conf->cluster;

  conf->set_val_or_die("log_to_stderr", "false");
  conf->set_val_or_die("err_to_stderr", "false");
  conf->set_val_or_die("flog_file", "");
  conf->set_val("keyring", "$osd_data/keyring", false);

  vector<const char *> args;
  vector<const char *> def_args;
  string conf_files;
  g_osd_manager->copy_args(args, def_args, conf_files);
  if (def_args.size())
    conf->parse_argv(def_args);

  ostringstream warnings;
  const char *files = conf_files.length() ? conf_files.c_str() : nullptr;
  int retval = conf->parse_config_files(files, &warnings, 0);
  if (-EDOM == retval) {
    err_msg << "error parsing config file";
  } else if (-EINVAL == retval) {
    if (files) {
      err_msg << "unable to open config file from search list " << files;
    } else {
      derr << "did not load config file, using default settings." << dendl;
      retval = 0;
    }
  } else if (retval) {
    err_msg << "error reading config file";
  }

  if (retval) {
    return retval;
  } else if (warnings.tellp() > 0) {
    dout(2) << "warning when parsing config file: " << warnings.str() << dendl;
  }

  conf->parse_env(); // environment variables override
  if (args.size()) // argv override
    conf->parse_argv(args);

  if (conf->log_flush_on_exit)
    cct->_log->set_flush_on_exit();

  conf->apply_changes(nullptr);
  add_local_context(cct);
  conf->call_all_observers();
  conf->complain_about_parse_errors(cct);

  // output ceph version
  ldout(cct, 0) << pretty_version_to_str()
       << ", process ceph-osd, pid " << ::getpid()
       << ", thread " << get_process_name_cpp() 
       << ", tid " << ceph_gettid() << dendl;

  common_init_finish(cct);
  return 0;
}

int OSDInstance::create_store(ostringstream& err_msg)
{
  if (conf->osd_data.empty()) {
    err_msg << "must specify '--osd-data=foo' data path";
    return -EINVAL;
  }

  string store_type = cct->_conf->osd_objectstore;
  {
    char fn[PATH_MAX];
    snprintf(fn, sizeof(fn), "%s/type", conf->osd_data.c_str());
    int fd = ::open(fn, O_RDONLY);
    if (fd >= 0) {
      bufferlist bl;
      bl.read_fd(fd, 64);
      if (bl.length()) {
        store_type = string(bl.c_str(), bl.length() - 1);  // drop \n
        ldout(cct, 5) << "object store type is " << store_type << dendl;
      }
      TEMP_FAILURE_RETRY(::close(fd));
    }
  }

  store = ObjectStore::create(cct, store_type, 
    conf->osd_data, conf->osd_journal, conf->osd_os_flags);
  if (!store) {
    err_msg << "unable to create object store";
    return -ENODEV;
  }

  string magic;
  uuid_d cluster_fsid, osd_fsid;
  int w;
  int retval = OSD::peek_meta(store, magic, cluster_fsid, osd_fsid, w);
  if (retval < 0) {
    err_msg << "unable to open OSD superblock on " << conf->osd_data;
    if (-ENOTSUP == retval) {
      err_msg << "\nplease verify that underlying storage supports xattrs";
    }
    return retval;
  }

  if (w != id) {
    err_msg << "OSD id " << w << " != my id " << id;
    return -EINVAL;
  }

  if (magic != CEPH_OSD_ONDISK_MAGIC) {
    err_msg << "OSD magic " << magic << " != my " << CEPH_OSD_ONDISK_MAGIC;
    return -EINVAL;
  }

  return 0;
}

bool OSDInstance::_start()
{
  assert(state.read() == STATE_PREPARING);

  ostringstream err_msg;
  int retval = init_cct(err_msg);
  if (retval)
    goto start_failed;

  retval = create_store(err_msg);
  if (retval)
    goto start_failed;

  if (local_msgrs) {
    messengers = new OSDMessengers(id, cct);
    retval = messengers->init(err_msg);
    if (retval)
      goto start_failed;
  }

  monc = new MonClient(cct, id);
  retval = monc->build_initial_monmap(&err_msg);
  if (retval)
    goto start_failed;

  osd.reset(new OSD(cct, store, id,
    messengers->ms_cluster,
    messengers->ms_public,
    messengers->ms_hbclient,
    messengers->ms_hb_front_server,
    messengers->ms_hb_back_server,
    messengers->ms_objecter,
    monc,
    conf->osd_data,
    conf->osd_journal));
  store = nullptr;

  ldout(cct, 5) << "preparing -> starting" << dendl;
  state.set(STATE_STARTING);

  retval = osd->local_pre_init(err_msg);
  if (retval)
    goto start_failed;

  if (local_msgrs)
    messengers->start();

  cct->set_failure_handler(this);
  retval = osd->local_init(err_msg);
  if (retval) {
    if (local_msgrs)
      messengers->shutdown();
    goto start_failed;
  }

  osd->final_init();
  return true;

start_failed:
  string msg = err_msg.str();
  if ('\n' == msg.back())
    msg.back() = '\0';
  lderr(cct) << "failed: " << cpp_strerror(retval) << ", " << msg << dendl;
  derr << "failed: " << cpp_strerror(retval) << ", " << msg << dendl;
  return false;
}

void OSDInstance::_wait()
{
  int timeout = conf->osd_admin_pseudo_status_timeout;
  ldout(cct, 5) << "starting -> running" << dendl;
  state.set(STATE_RUNNING);

  Mutex::Locker l(lock);
  while (state.read() == STATE_RUNNING) {
    if (cct->is_failed()) {
      state.set(STATE_STOPPING);
    } else if (timeout > 0) {
      utime_t when = last_tx + utime_t(timeout, 0);
      if ((ceph_clock_now(g_ceph_context) > when) && 
          state.compare_and_swap(STATE_RUNNING, STATE_STOPPING)) {
        derr << "timedout, no pseudo command since " << last_tx << dendl;
        lderr(cct) << "timedout, no pseudo command since " << last_tx << dendl;
      } else {
        ldout(cct, 10) << "wait until " << when << dendl;
        cond.WaitUntil(lock, when);
      }
    } else {
      ldout(cct, 10) << "wait" << dendl;
      cond.Wait(lock);
    }
  }
}

void OSDInstance::_clean_up()
{
  {
    ldout(cct, 0) << "~OSD" << dendl;
    Mutex::Locker l(lock);
    osd.reset();
  }

  if (local_msgrs) {
    delete messengers;
    messengers = nullptr;
  }
  if (monc) {
    delete monc;
    monc = nullptr;
  }
  if (store) {
    delete store;
    store = nullptr;
  }

  conf->remove_observer(this);
  {
    ldout(cct, 0) << "~CephContext" << dendl;
    Mutex::Locker l(lock);
    delete_local_context(cct);
    cct->put();
    cct = nullptr;
    conf = nullptr;
  }

  state.set(STATE_STOPPED);
}

void OSDInstance::osd_thread_entry()
{
  derr << "start" << dendl;
  start_ok = _start();
  if (start_ok) {
    _wait();
    osd->shutdown();
  }
  _clean_up();
  derr << "stop" << dendl;
}

int OSDInstance::start()
{
  update_last_tx();

  switch (state.read()) {
  case STATE_PREPARING:
  case STATE_STARTING:
  case STATE_RUNNING:
    return -EEXIST;
  case STATE_STOPPING:
    dout(5) << "cannot start osd at state stopping" << dendl;
    return -EAGAIN;
  default: // STATE_STOPPED
    failed.set(0); // reset to no failure
    state.set(STATE_PREPARING);
    osd_thread.create(thread_name);
    osd_thread.detach();
    return 0;
  }
}

int OSDInstance::stop(bool from_pseudo)
{
  if (from_pseudo)
    update_last_tx();

  int _state = state.read();
  switch (_state) {
  case STATE_PREPARING:
  case STATE_STARTING:
    dout(5) << "cannot stop osd at state " << get_state_name(_state) << dendl;
    return -EBUSY;
  case STATE_RUNNING:
    if (state.compare_and_swap(STATE_RUNNING, STATE_STOPPING)) {
      derr << (from_pseudo ? "pseudo" : "mark_down") << dendl;
      cond.SloppySignal();
    }
  default: // STATE_STOPPING/STOPPED
    return 0;
  }
}

int OSDInstance::get_status(string& msg)
{
  update_last_tx();

  int val(OSDManager::OSD_STATUS_NORMAL);
  int _state = state.read();
  switch (_state) {
  case STATE_PREPARING:
  case STATE_STARTING:
    val = OSDManager::OSD_STATUS_STARTING;
  case STATE_RUNNING:
  case STATE_STOPPING:
    {
      Mutex::Locker l(lock);
      if (osd)
        msg = OSD::get_state_name(osd->get_state());
      else
        msg = get_state_name(_state);
    }
    break;
  default: // STATE_STOPPED
    if (!start_ok) {
      val = OSDManager::OSD_STATUS_START_FAILED;
      msg = "start failed";
    } else if (is_failed()) {
      val = OSDManager::OSD_STATUS_FAILED;
      msg = "failed";
    } else {
      val = OSDManager::OSD_STATUS_STOPPED;
      msg = "stopped";
    }
  }

  return val;
}

const char** OSDInstance::get_tracked_conf_keys() const
{
  static const char *KEYS[] = {
    "osd_local_assert_fail_at",
    nullptr };
  return KEYS;
}

void OSDInstance::handle_conf_change(const struct md_config_t *conf,
  const std::set <std::string> &changed)
{
  failed_at.set(conf->osd_local_assert_fail_at);
}

void OSDInstance::handle_failure(int err, const char *desc)
{
  if (err) {
    derr << cpp_strerror(err) << ", " << desc << dendl;
    lderr(cct) << cpp_strerror(err) << ", " << desc << dendl;
  } else {
    derr << desc << dendl;
    lderr(cct) << desc << dendl;
  }

  {
    BackTrace bt(1);
    lderr(cct);
    bt.print(*_dout);
    *_dout << dendl;
  }

  cct->_log->dump_recent();
  cond.SloppySignal();
}
