#ifndef CEPH_OSDMESSENGERS_H
#define CEPH_OSDMESSENGERS_H

#include "include/types.h"

class CephContext;
class Throttle;
class Messenger;

class OSDMessengers {
  int id;
  CephContext *cct;
  Throttle *client_byte_throttler;
  Throttle *client_msg_throttler;

public:
  Messenger *ms_public;
  Messenger *ms_cluster;
  Messenger *ms_hbclient;
  Messenger *ms_hb_back_server;
  Messenger *ms_hb_front_server;
  Messenger *ms_objecter;

  explicit OSDMessengers(int _id, CephContext *_cct);
  ~OSDMessengers();

  int init(ostream& out);
  void start();
  void wait();
  void shutdown();
};

#endif