#include "include/color.h"
#include "common/errno.h"
#include "common/io_priority.h"
#include "common/pick_address.h"

#include "OSD.h"
#include "OSDMessengers.h"

#define dout_subsys ceph_subsys_osd_admin
#undef dout_prefix
#define dout_prefix *_dout << "osd." << id << " " << __func__ << " "

OSDMessengers::OSDMessengers(int _id, CephContext * _cct) : 
  id(_id), cct(_cct),
  client_byte_throttler(nullptr),
  client_msg_throttler(nullptr),
  ms_public(nullptr),
  ms_cluster(nullptr),
  ms_hbclient(nullptr),
  ms_hb_back_server(nullptr),
  ms_hb_front_server(nullptr),
  ms_objecter(nullptr)
{
  assert(id >= -1);
  assert((id == -1) == (cct == g_ceph_context));
}

OSDMessengers::~OSDMessengers()
{
  delete ms_public;
  delete ms_hbclient;
  delete ms_hb_front_server;
  delete ms_hb_back_server;
  delete ms_cluster;
  delete ms_objecter;

  if (id >= 0) {
    delete client_byte_throttler;
    delete client_msg_throttler;
  }
}

int OSDMessengers::init(ostream& out)
{
  ldout(cct, 5) << dendl;
  pick_addresses(cct, CEPH_PICK_ADDRESS_PUBLIC | CEPH_PICK_ADDRESS_CLUSTER, id);

  md_config_t *conf = cct->_conf;
  if (conf->public_addr.is_blank_ip() && !conf->cluster_addr.is_blank_ip()) {
    lderr(cct) << (id == -1 ? TEXT_YELLOW : "") 
         << " ** WARNING: specified cluster addr but not public addr; we recommend **\n"
	       << " **          you specify neither or both.                             **"
	       << (id == -1 ? TEXT_NORMAL : "") << dendl;
  }

  uint64_t nonce = (id == -1) ? ::getpid() : ceph_gettid();
  ms_public = Messenger::create(cct, conf->ms_type,
    entity_name_t::OSD(id), "client", nonce);
  ms_cluster = Messenger::create(cct, conf->ms_type,
    entity_name_t::OSD(id), "cluster", nonce, CEPH_FEATURES_ALL);
  ms_hbclient = Messenger::create(cct, conf->ms_type,
    entity_name_t::OSD(id), "hbclient", nonce);
  ms_hb_back_server = Messenger::create(cct, conf->ms_type,
    entity_name_t::OSD(id), "hb_back_server", nonce);
  ms_hb_front_server = Messenger::create(cct, conf->ms_type,
    entity_name_t::OSD(id), "hb_front_server", nonce);
  ms_objecter = Messenger::create(cct, conf->ms_type,
    entity_name_t::OSD(id), "ms_objecter", nonce);
  if (!ms_public || !ms_cluster ||
      !ms_hbclient || !ms_hb_back_server || !ms_hb_front_server ||
      !ms_objecter) {
    out << "failed to create messengers";
    return -EINVAL;
  }

  ms_cluster->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hbclient->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_back_server->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_front_server->set_cluster_protocol(CEPH_OSD_PROTOCOL);

  if (id == -1) {
    cout << "starting osd.admin at " << ms_public->get_myaddr() << std::endl;
  } else {
    ldout(cct, 0) << "starting at " << ms_public->get_myaddr() << dendl;
  }

  // SP-TODO: should add virtual messenger as a feature?
  uint64_t supported =
    CEPH_FEATURE_UID | 
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_MSG_AUTH |
    CEPH_FEATURE_OSD_ERASURE_CODES;

  // All feature bits 0 - 34 should be present from dumpling v0.67 forward
  uint64_t osd_required =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_OSDENC;

  ms_public->set_default_policy(
    Messenger::Policy::stateless_server(supported, 0));
  // SP-TODO: how to set throttle in shared mode?
  if (id >= 0) {
    client_byte_throttler = new Throttle(cct, "osd_client_bytes", 
      conf->osd_client_message_size_cap);
    client_msg_throttler = new Throttle(cct, "osd_client_messages", 
      conf->osd_client_message_cap);
    ms_public->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
      client_byte_throttler, client_msg_throttler);
  }
  ms_public->set_policy(entity_name_t::TYPE_MON,
    Messenger::Policy::lossy_client(supported, osd_required));
  //try to poison pill any OSD connections on the wrong address
  ms_public->set_policy(entity_name_t::TYPE_OSD,
	  Messenger::Policy::stateless_server(0, 0));

  ms_cluster->set_default_policy(
    Messenger::Policy::stateless_server(0, 0));
  ms_cluster->set_policy(entity_name_t::TYPE_MON,
    Messenger::Policy::lossy_client(0, 0));
  ms_cluster->set_policy(entity_name_t::TYPE_OSD,
    Messenger::Policy::lossless_peer(supported, osd_required));
  ms_cluster->set_policy(entity_name_t::TYPE_CLIENT,
    Messenger::Policy::stateless_server(0, 0));

  ms_hbclient->set_policy(entity_name_t::TYPE_OSD,
    Messenger::Policy::lossy_client(0, 0));
  ms_hb_back_server->set_policy(entity_name_t::TYPE_OSD,
    Messenger::Policy::stateless_server(0, 0));
  ms_hb_front_server->set_policy(entity_name_t::TYPE_OSD,
    Messenger::Policy::stateless_server(0, 0));

  ms_objecter->set_default_policy(
    Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));

  int ret = ms_public->bind(conf->public_addr);
  if (ret < 0) {
    out << "failed to bind " << conf->public_addr << ": " << cpp_strerror(ret);
    return ret;
  }

  ret = ms_cluster->bind(conf->cluster_addr);
  if (ret < 0) {
    out << "failed to bind " << conf->cluster_addr << ": " << cpp_strerror(ret);
    return ret;
  }

  if (conf->osd_heartbeat_use_min_delay_socket) {
    ms_hbclient->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
    ms_hb_back_server->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
    ms_hb_front_server->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
  }

  // hb back should bind to same ip as cluster_addr (if specified)
  entity_addr_t hb_back_addr = conf->osd_heartbeat_addr;
  if (hb_back_addr.is_blank_ip()) {
    hb_back_addr = conf->cluster_addr;
    if (hb_back_addr.is_ip())
      hb_back_addr.set_port(0);
  }
  ret = ms_hb_back_server->bind(hb_back_addr);
  if (ret < 0) {
    out << "failed to bind " << hb_back_addr << ": " << cpp_strerror(ret);
    return ret;
  }

  // hb front should bind to same ip as public_addr
  entity_addr_t hb_front_addr = conf->public_addr;
  if (hb_front_addr.is_ip())
    hb_front_addr.set_port(0);
  ret = ms_hb_front_server->bind(hb_front_addr);
  if (ret < 0) {
    out << "failed to bind " << hb_front_addr << ": " << cpp_strerror(ret);
    return ret;
  }

  return 0;
}

void OSDMessengers::start()
{
  ldout(cct, 10) << dendl;
  ms_public->start();
  ms_hbclient->start();
  ms_hb_front_server->start();
  ms_hb_back_server->start();
  ms_cluster->start();
  ms_objecter->start();
}

void OSDMessengers::wait()
{
  ldout(cct, 10) << dendl;
  ms_public->wait();
  ms_hbclient->wait();
  ms_hb_front_server->wait();
  ms_hb_back_server->wait();
  ms_cluster->wait();
  ms_objecter->wait();
}

void OSDMessengers::shutdown()
{
  ldout(cct, 10) << dendl;
  ms_public->shutdown();
  ms_cluster->shutdown();
  ms_hbclient->shutdown();
  ms_objecter->shutdown();
  ms_hb_front_server->shutdown();
  ms_hb_back_server->shutdown();
}
