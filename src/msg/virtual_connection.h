#ifndef VRITUAL_CON_H
#define VRITUAL_CON_H

#include <boost/intrusive_ptr.hpp>
#include <queue>
#include "Connection.h"
#include "Message.h"
#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "include/msgr.h"
#include "msg_types.h"
#include "Messenger.h"
#include "auth/Auth.h"


enum VCON_STATE{
    VCON_CONNECTING = 0,
    VCON_CONNECTED,
    VCON_MARK_DOWNING,
    VCON_OSD_STOP
};

enum VCON_DIRECTION{
	VCON_INIT = 0,
	VCON_CLIENT,
	VCON_SERVER
};


class virtual_connection: public Connection{
    public:
        entity_inst_t  src;
        entity_inst_t  dst;
        ConnectionRef con;
        CephContext *cct;
        VCON_STATE con_state;
        AuthAuthorizer *auth;
        Mutex lock;
        //list<MessageRef> delay_msg_list;
        queue<MessageRef> delay_msg_list;
        uint64_t src_id;
        uint64_t dst_id;
        VCON_DIRECTION vcon_dir;
        virtual_connection(CephContext *cct, Messenger *m)
        :Connection(cct, m),cct(cct) ,con_state(VCON_CONNECTING),auth(NULL),
        lock("virtual_connection::lock"),src_id(0),dst_id(0),vcon_dir(VCON_INIT)
        {
        }
        int send_message(Message *m);
        int send_delay_list_msg();
        void clear_delay_list_msg();
        void mark_down_impl();
        uint64_t get_features() const { 
            if(!con) return 0;
            return con->get_features(); 
        }
        bool is_connected() {
            if(!con) return 0;
            return con->is_connected();
        }
        void send_keepalive() {
            if(!con) return; 
            return con->send_keepalive();
        }
        void mark_disposable() {
            if(!con) return; 
            return con->mark_disposable();
        }
        int get_peer_type() const {
            if(!con) return dst.addr.type;
            return con->get_peer_type(); 
        }
        const entity_addr_t& get_peer_addr() const {
            if(!con) return dst.addr;
            return con->get_peer_addr();
        }
        bool has_feature(uint64_t f) const {
            if(!con) return false;
            return con->has_feature(f);

        }  
        bool peer_is_mon() const {
            if(!con) return dst.addr.type == CEPH_ENTITY_TYPE_MON; 
            return con->peer_is_mon(); 
        }
        bool peer_is_mds() const {
            if(!con) 
                return dst.addr.type == CEPH_ENTITY_TYPE_MDS; 
            return con->peer_is_mds(); 
        }
        bool peer_is_osd() const {
            if(!con) return dst.addr.type == CEPH_ENTITY_TYPE_OSD; 
            return con->peer_is_osd(); 
        }
        bool peer_is_client() const {
            if(!con) return dst.addr.type == CEPH_ENTITY_TYPE_CLIENT; 
            return con->peer_is_client(); 
        }
	
	
        utime_t get_last_keepalive() const {
            if(con)
            {
                return con->get_last_keepalive();
            }
            return 0;
        }
        utime_t get_last_keepalive_ack() const {
            if(con)
            {
                return con->get_last_keepalive_ack();
            }
            return 0;
        }
	    void post_rx_buffer(ceph_tid_t tid, bufferlist& bl) {
            if(con)
            {
                return con->post_rx_buffer(tid,bl);
            }
            return;
        }

        void revoke_rx_buffer(ceph_tid_t tid) {
            if(con)
            {
                return con->revoke_rx_buffer(tid); 
            }
            return;
        }

        int get_osd_num() {return dst.name.num();}

        ~virtual_connection() {}

};

typedef boost::intrusive_ptr<virtual_connection> Vri_ConnectionRef;

#endif
