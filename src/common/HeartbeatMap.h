// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_HEARTBEATMAP_H
#define CEPH_HEARTBEATMAP_H

#include <pthread.h>

#include <string>
#include <list>
#include <time.h>

#include "include/atomic.h"
#include "Mutex.h"
#include "RWLock.h"

class CephContext;

namespace ceph {

/*
 * HeartbeatMap -
 *
 * Maintain a set of handles for internal subsystems to periodically
 * check in with a health check and timeout.  Each user can register
 * and get a handle they can use to set or reset a timeout.  
 *
 * A simple is_healthy() method checks for any users who are not within
 * their grace period for a heartbeat.
 */

struct heartbeat_handle_d {
  const std::string name;
  atomic_t timeout, suicide_timeout;
  time_t grace, suicide_grace;
  std::list<heartbeat_handle_d*>::iterator list_item;
  pthread_t tid; // thread id

  Mutex lock;
  enum State {
    STATE_NORMAL = 0,
    STATE_TIMEDOUT,
    STATE_SUICIDE_TIMEDOUT
  } state;

  explicit heartbeat_handle_d(const std::string& n)
    : name(n), grace(0), suicide_grace(0), tid(pthread_self())
    , lock("heartbeat_handle_d:lock"), state(STATE_NORMAL)
  { }

  bool set_state(State _state) {
    Mutex::Locker l(lock);
    if (state == _state)
      return false;
    state = _state;
    return true;
  }

  bool check(CephContext *cct, const char *who, time_t now);
};

class HeartbeatMap {
 public:
  // register/unregister
  heartbeat_handle_d *add_worker(const std::string& name);
  void remove_worker(const heartbeat_handle_d *h);

  // reset the timeout so that it expects another touch within grace amount of time
  void reset_timeout(heartbeat_handle_d *h, time_t grace, time_t suicide_grace);
  // clear the timeout so that it's not checked on
  void clear_timeout(heartbeat_handle_d *h);

  // return false if any of the timeouts are currently expired.
  bool is_healthy();

  // touch cct->_conf->heartbeat_file if is_healthy()
  void check_touch_file();

  // get the number of unhealthy workers
  int get_unhealthy_workers() const;

  // get the number of total workers
  int get_total_workers() const;

  explicit HeartbeatMap(CephContext *cct);
  ~HeartbeatMap();

 private:
  CephContext *m_cct;
  RWLock m_rwlock;
  time_t m_inject_unhealthy_until;
  std::list<heartbeat_handle_d*> m_workers;
  atomic_t m_unhealthy_workers;
  atomic_t m_total_workers;
};

}
#endif
