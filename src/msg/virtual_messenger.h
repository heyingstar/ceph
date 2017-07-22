#ifndef VRITUAL_MESSENGER_H
#define VRITUAL_MESSENGER_H

#include "include/types.h"
#include "include/xlist.h"
#include <sys/time.h>
#include <time.h>

#include <list>
#include <map>
using namespace std;
#include "include/unordered_map.h"
#include "msg_types.h"
#include "Dispatcher.h"
#include "include/atomic.h"
#include "include/Spinlock.h"
#include "include/unordered_map.h"
#include "common/ceph_context.h"
#include "common/RWLock.h"
#include "Dispatcher.h"
#include "Messenger.h"
#include "common/Mutex.h"
#include "include/msgr.h"
#include "Message.h"
#include "common/dout.h"
#include "virtual_connection.h"
#include "messages/MVconnect.h"
#include "messages/MVaccept.h"
#include "messages/MVremove_ack.h"
#include "auth/Crypto.h"
#include "auth/Auth.h"
#include "messages/MVremove.h"
#include "include/utime.h"

typedef ceph::unordered_map<entity_inst_t , ceph::unordered_map<entity_inst_t , Vri_ConnectionRef> >  CON_MAP;
typedef ceph::unordered_map<int64_t , ceph::unordered_map<entity_inst_t , Vri_ConnectionRef> > NUM_MAP;
typedef map<Vri_ConnectionRef, entity_inst_t> CON_RESET;
typedef map<int, list<Dispatcher*>> DISPATCHER_MAP;
typedef map<uint64_t, Vri_ConnectionRef> SEQ_MAP;
typedef ceph::unordered_map<entity_addr_t, std::set<uint64_t>> CON_TO_VCON_MAP;


#define HANDLE_RESET 0
#define HANDLE_REMOTE_RESET 1

#define FAST_DISPATCH 0
#define MS_DISPATCH 1
#define FAST_PREPROCESS 2

enum  v_stop{
    V_MESSENGER_INIT,
    V_MESSENGER_START,
    V_MESSENGER_STOP
};

struct ceph_entity_inst get_ceph_inst(entity_inst_t inst);
entity_inst_t get_message_inst(struct ceph_entity_inst  ceph_inst);

class msg_timeout_detail{
public:
   /* recv_stamp is set when the Messenger starts reading the
    * Message off the wire */
    utime_t recv_stamp;
   /* throttle_stamp is the point at which we got throttle */
   utime_t throttle_stamp;
   /* time at which message was fully read */
   utime_t recv_complete_stamp;
   /* dispatch_stamp is set when the Messenger starts calling dispatch() on
   * its endpoints */
   utime_t preprocess_end_stamp;

   msg_timeout_detail(){}
   ~msg_timeout_detail(){}
   
};

class dispatch_timeout_detail{
public:
   /* recv_stamp is set when the Messenger starts reading the
    * Message off the wire */
    utime_t recv_stamp;
   /* throttle_stamp is the point at which we got throttle */
   utime_t throttle_stamp;
   /* time at which message was fully read */
   utime_t recv_complete_stamp;
   /* dispatch_stamp is set when the Messenger starts calling dispatch() on
   * its endpoints */
   utime_t preprocess_end_stamp;

   utime_t fast_dispatcher_begin_stamp;

   utime_t fast_dispatch_end_stamp;

   utime_t dispatcher_begin_stamp;
   
   utime_t dispatch_end_stamp;

   stringstream msg_detai;

   uint64_t handle_msg_time;

   dispatch_timeout_detail(){}
   ~dispatch_timeout_detail(){}
   
};

class dfx_vcon{
public:
	uint64_t fast_preprocess_handle_max;
	uint64_t fast_preprocess_dispatcher_handle_max;
	uint64_t fast_dispatch_handle_max;
	uint64_t fast_dispatch_dispatcher_handle_max;
	uint64_t ms_dispatch_handle_max;
	uint64_t ms_dispatch_dispatcher_handle_max;
	atomic_t fast_preprocess_handle_sum;
	atomic_t fast_preprocess_handle_cnt;
	atomic_t fast_preprocess_dispatcher_handle_sum;
	atomic_t fast_preprocess_dispatcher_handle_cnt;
	atomic_t fast_dispatch_handle_sum;
	atomic_t fast_dispatch_handle_cnt;
	atomic_t fast_dispatch_dispatcher_handle_sum;
	atomic_t fast_dispatch_dispatcher_handle_cnt;
	atomic_t ms_dispatch_handle_sum;
	atomic_t ms_dispatch_handle_cnt;
	atomic_t ms_dispatch_dispatcher_handle_sum;
	atomic_t ms_dispatch_dispatcher_handle_cnt;
	list<dispatch_timeout_detail*> msg_type_list;
	Mutex type_lock;

	dfx_vcon()
	:fast_preprocess_handle_max(0),fast_preprocess_dispatcher_handle_max(0),
	fast_dispatch_handle_max(0),fast_dispatch_dispatcher_handle_max(0),
	ms_dispatch_handle_max(0),ms_dispatch_dispatcher_handle_max(0),
	fast_preprocess_handle_sum(0),fast_preprocess_handle_cnt(0),
	fast_preprocess_dispatcher_handle_sum(0),fast_preprocess_dispatcher_handle_cnt(0),
	fast_dispatch_handle_sum(0),fast_dispatch_handle_cnt(0),
	fast_dispatch_dispatcher_handle_sum(0),fast_dispatch_dispatcher_handle_cnt(0),
	ms_dispatch_handle_sum(0),ms_dispatch_handle_cnt(0),
	ms_dispatch_dispatcher_handle_sum(0),ms_dispatch_dispatcher_handle_cnt(0),
	type_lock("dfx_vcon::lock")
        {
        }

	~dfx_vcon();
	
};

class virtual_messenger: public Messenger,public Dispatcher{
    public:
        CephContext *cct;
        RWLock dispatch_lock;
        RWLock lock;
        mutable DISPATCHER_MAP v_dispatchers;
        mutable DISPATCHER_MAP v_fast_dispatchers;
        NUM_MAP share_con;
        CON_MAP con_bak;
        SEQ_MAP seqMap;
        CON_TO_VCON_MAP  con_to_vcon_map;
        //map<virtual_connection * , entity_inst_t> reset_con;
        Messenger *messgr;
        string  messenger_name;
        v_stop vmessenger_state;
        atomic_t vcon_seq;
        atomic_t msg_id;
        uint64_t g_var_1s_jiffies;
        dfx_vcon *dfx_status;
        
        Mutex async_reset_lock;
        Cond async_reset_cond;
        bool stop_async_reset;
        queue<Vri_ConnectionRef> reset_vcon_que;
        class AsyncResetVconThread : public Thread {
                virtual_messenger *messenger;
         public:
         explicit AsyncResetVconThread(virtual_messenger *messenger) : messenger(messenger) {}
         void *entry() {
                 messenger->run_delivery();
                 return 0;
              }
         } async_reset_vcon_thread;
        

        virtual_messenger(CephContext *cct, entity_name_t name,
        string mname, uint64_t _nonce, uint64_t features);
        void insert_vcon_to_sharecon(Vri_ConnectionRef vcon,CON_MAP& con_map);
        void insert_vcon_to_sharecon(Vri_ConnectionRef vcon, NUM_MAP& con_map);
        void delete_con_from_sharecon(virtual_connection *vcon);
        void delete_con_from_sharecon_lite(virtual_connection *vcon);
        void print_map_con();
        uint64_t create_msg_id(virtual_connection *vcon);
        Vri_ConnectionRef find_vcon_from_sharecon(entity_inst_t src, entity_inst_t dst,CON_MAP& con_map);
        Vri_ConnectionRef find_vcon_from_sharecon(entity_inst_t src, entity_inst_t dst,NUM_MAP& con_map);
        Vri_ConnectionRef find_virtual_connection(const entity_inst_t  &src, const entity_inst_t  &dst);
        ConnectionRef get_connection(const entity_inst_t  &src, const entity_inst_t  &dst);
        Vri_ConnectionRef get_vconnection(const entity_inst_t  &src, const entity_inst_t  &dst);
        ConnectionRef get_connection(const entity_inst_t  &dst);
        ConnectionRef get_loopback_connection(const entity_inst_t &src);
        ConnectionRef get_loopback_connection();
        void delete_dispatch(int osd_id);
        void add_dispatcher_head(Dispatcher *d);
        void add_dispatcher_tail(Dispatcher *d);
        bool ms_can_fast_dispatch_any()const {return true;}
        bool ms_can_fast_dispatch(Message *m)const;
        void ms_fast_dispatch(Message *m);
        void ms_fast_preprocess(Message *m);
        void ms_preprocess_vcon(Message *m);
        bool ms_dispatch(Message *m);
        void ms_handle_connect(Connection *con);
        void ms_handle_fast_connect(Connection *con);
        void ms_handle_accept(Connection *con);
        void ms_handle_fast_accept(Connection *con);
        bool ms_handle_reset(Connection *con);
        void ms_handle_remote_reset(Connection *con);
        bool ms_verify_authorizer(Connection *con, int peer_type,
            int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
            bool& isvalid, CryptoKey& session_key);
        bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
        void handle_reset_dispatcher(virtual_connection *con);
		void handle_remote_reset_dispatcher(virtual_connection *con);
        bool ms_get_authorizer_vcon(int peer_type, AuthAuthorizer **a, bool force_new, int type, int osd_id);
        //authorizer_variable *find_au(Connection *con);
        bool handle_vcon_accept_data(vaccept *m);
        bool handle_vcon_connect_data(vconnect *m);
        bool handle_vcon_no_existent_data(Message *m);
        void handle_new_vcon(Message *m);
        void send_vconnect_message(int type, virtual_connection *vcon, entity_inst_t src, entity_inst_t dst);
        void send_accept_message(int type, virtual_connection *vcon,  bufferlist reply,entity_inst_t src, entity_inst_t dst);
        void send_remove_vcon_message(int type, virtual_connection *vcon,  entity_inst_t src, entity_inst_t dst);
        void send_remove_ack_message(int type, virtual_connection *vcon,  entity_inst_t src, entity_inst_t dst);
        int get_vcon_num_from_map(virtual_connection* vcon);
        bool is_stop()const {return V_MESSENGER_STOP == vmessenger_state;}
        bool is_loacl_con(ConnectionRef con);

        int send_message(Message *m, const entity_inst_t& dest) ;
        int send_message(Message *m, const entity_inst_t& src, const entity_inst_t& dst);
        const entity_inst_t& get_myinst() { return messgr->get_myinst(); }
        uint32_t get_magic() { return messgr->get_magic(); }
        const entity_addr_t& get_myaddr() { return messgr->get_myaddr(); }
        const entity_name_t& get_myname() { return messgr->get_myname(); }
        void set_myname(const entity_name_t& m) {my_inst.name = m; return messgr->set_myname(m);}
        int get_mytype() const {return my_inst.name.type();}
        void set_addr_unknowns(const entity_addr_t& addr){return messgr->set_addr_unknowns(addr);}
        int get_default_send_priority() { return messgr->get_default_send_priority(); }
        int get_dispatch_queue_len(){return messgr->get_dispatch_queue_len();}
        double get_dispatch_queue_max_age(utime_t now) {return messgr->get_dispatch_queue_max_age(now);}
        void set_cluster_protocol(int p){return messgr->set_cluster_protocol(p);}
        void set_default_policy(Policy p){return messgr->set_default_policy(p);}
        void set_policy(int type, Policy p){return messgr->set_policy(type,p);}
        Policy get_policy(int t) {return messgr->get_policy(t);}
        Policy get_default_policy() {return messgr->get_default_policy();}
        int get_socket_priority() {
            return messgr->get_socket_priority();
        }
        void set_policy_throttlers(int type,Throttle *bytes,Throttle *msgs=NULL){return messgr->set_policy_throttlers(type,bytes,msgs);}
        void set_socket_priority(int prio) {
            return messgr->set_socket_priority(prio);
        }
        void set_default_send_priority(int p){return messgr->set_default_send_priority(p);}
        int bind(const entity_addr_t& bind_addr) {return messgr->bind(bind_addr);}
        int rebind(const set<int>& avoid_ports) { return 0; }
        int start() { return messgr->start();vmessenger_state = V_MESSENGER_START;}
        void wait() {return messgr->wait();}
        int shutdown();
        void mark_down_impl(const entity_addr_t& a);
        void mark_down(const entity_addr_t& a, entity_inst_t src_osd, entity_inst_t dest_osd);
        void mark_down_all();
        void mark_down_all(int id);
        void mark_down(virtual_connection* vcon);
        void ms_handle_remote_reset_vcon(virtual_connection* vcon);
        void ms_handle_reset_vcon(virtual_connection* vcon);
        bool handle_vcon_remove_ack_data(Message *m);
        void mark_down_all_delete_dispatch(int osd_id);
        uint64_t get_time_space(uint64_t time1, uint64_t time2);
        void push_msg_type_to_list(entity_inst_t  src , entity_inst_t  dst , string msg_detail, msg_timeout_detail time_tmp,utime_t begin, utime_t end, int type);
        void clear_status(ceph::Formatter *f);
        void show_status(ceph::Formatter *f);
        void show_msg_status(ceph::Formatter *f);
        
        void vcon_start();
        void run_delivery();
        void in_vcon_que(Vri_ConnectionRef vcon);
        void clear_reset_vcon_que();
        //map<Connection *,authorizer_variable *> mauthorizer;
        ~virtual_messenger();

        uint64_t get_available_seqId(Vri_ConnectionRef vcon);
        void  insert_to_con_vcon_map(Vri_ConnectionRef  vcon);
        void delete_vcon_from_map(Vri_ConnectionRef vcon);
        void delete_vcon_from_seqMap(Vri_ConnectionRef vcon);

};

#endif
