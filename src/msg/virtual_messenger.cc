#include <errno.h>
#include <iostream>
#include <fstream>
#include <thread>
#include "common/config.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "common/valgrind.h"
#include "include/Spinlock.h"
#include "include/unordered_map.h"
#include "virtual_messenger.h"
#include "msg/simple/SimpleMessenger.h"
#include "msg/async/AsyncMessenger.h"
#ifdef HAVE_XIO
#include "msg/xio/XioMessenger.h"
#endif
#include "common/Clock.h"

#define  PREPOCESS_TIMEDOUT (10*1000)

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, virtual_messenger *msgr) {
  return *_dout << "--( " << msgr->get_myinst() << " -- " << msgr->messenger_name <<" )--";
}


virtual_messenger::virtual_messenger(CephContext *cct, entity_name_t name,
    string mname, uint64_t _nonce, uint64_t features)
    :Messenger(cct, name),Dispatcher(cct),
    cct(cct),
    dispatch_lock("virtual_messenger dispatcher::rwlock"),
    lock("virtual_messenger::rwlock"),
    messenger_name(mname),
    vmessenger_state(V_MESSENGER_INIT),
    vcon_seq(0),
    msg_id(0),
    async_reset_lock("virtual_messenger reset vcon::lock"),
    stop_async_reset(false),
    async_reset_vcon_thread(this)
{
    dfx_status = new dfx_vcon;
    g_var_1s_jiffies = get_cycle();
    string type = cct->_conf->ms_type;
    int r = -1;
    if (type == "random") 
    {
        thread_local unsigned seed = (unsigned) time(nullptr) +
            (unsigned) std::hash<std::thread::id>()(std::this_thread::get_id());
        r = rand_r(&seed) % 2; // random does not include xio
    }

    if (r == 0 || type == "simple")
        messgr = new SimpleMessenger(cct, name, mname, _nonce, features);
    else if (r == 1 || type == "async")
        messgr = new AsyncMessenger(cct, name, mname, _nonce, features);
#ifdef HAVE_XIO
    else if ((type == "xio") && cct->check_experimental_feature_enabled("ms-type-xio"))
        messgr = new XioMessenger(cct, name, mname, _nonce, features);
#endif
    if(messgr)
    {
        cct->vcon_msger.push_back(this);
        vcon_start();
        return;
    }
    lderr(cct) << "unrecognized ms_type '" << type << "'" << dendl;
}

uint64_t virtual_messenger::get_time_space(uint64_t time1, uint64_t time2) 
{
    if(g_var_1s_jiffies > 0)
        return (time2-time1)/g_var_1s_jiffies;	
    else
        return 0;  
}

void virtual_messenger::push_msg_type_to_list(entity_inst_t  src , entity_inst_t  dst , string msg_detail, msg_timeout_detail time_tmp,utime_t begin, utime_t end, int type)
{

    dispatch_timeout_detail *handle_time = new dispatch_timeout_detail();
    handle_time->recv_stamp = time_tmp.recv_stamp;
    handle_time->throttle_stamp = time_tmp.throttle_stamp;
    handle_time->recv_complete_stamp = time_tmp.recv_complete_stamp;
    handle_time->preprocess_end_stamp = time_tmp.preprocess_end_stamp;
    if(FAST_DISPATCH == type)
    {
        handle_time->fast_dispatcher_begin_stamp = begin;
        handle_time->fast_dispatch_end_stamp = end;
        handle_time->handle_msg_time = handle_time->fast_dispatch_end_stamp.to_msec()- handle_time->recv_stamp.to_msec();
    }
    else if(MS_DISPATCH == type)
    {
        handle_time->dispatcher_begin_stamp = begin;
        handle_time->dispatch_end_stamp = end;
        handle_time->handle_msg_time = handle_time->dispatch_end_stamp.to_msec()- handle_time->recv_stamp.to_msec();
    }
    else if(FAST_PREPROCESS == type)
    {
        handle_time->preprocess_end_stamp = end;
        handle_time->handle_msg_time = handle_time->preprocess_end_stamp.to_msec()- handle_time->recv_stamp.to_msec();
    }
    handle_time->msg_detai<< "msg detail: [ "<< msg_detail << " ]"<< " from " << src << " to "<< dst;
    
    dfx_status->type_lock.Lock();
    if(dfx_status->msg_type_list.size() <= 100)
    {
        dfx_status->msg_type_list.push_back(handle_time);
        dfx_status->type_lock.Unlock();
        return;
    }
    else
    {
        dispatch_timeout_detail *tmp = dfx_status->msg_type_list.front();
        delete tmp;
        dfx_status->msg_type_list.pop_front();
        dfx_status->msg_type_list.push_back(handle_time);
        dfx_status->type_lock.Unlock();
        return;
    }

    dfx_status->type_lock.Unlock();
    return;
    	
}

void virtual_messenger::print_map_con()
{
    NUM_MAP::iterator p;
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q;

    for(p = share_con.begin(); p != share_con.end(); p++)
    {
        for(q = p->second.begin(); q!= p->second.end();q++)
        {
            virtual_connection* vcon = q->second.get();
            ldout(cct, 10)<< " print_map vcon: "<<vcon  << " con: " << vcon->con << " src:"<< vcon->src <<"  dst:" << vcon->dst <<  dendl;
        }
    }
}


void virtual_messenger::insert_vcon_to_sharecon(Vri_ConnectionRef vcon, CON_MAP& con_map)
{
    ldout(cct, 10)<< "insert vcon: "<<vcon << " src:"<< vcon->src <<"  dst:" << vcon->dst << "  into con_map" <<  dendl;
    con_map[vcon->src][vcon->dst] = vcon;
    //print_map_con();
}

void virtual_messenger::insert_vcon_to_sharecon(Vri_ConnectionRef vcon, NUM_MAP& con_map)
{
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == vcon->src.name.type())
    {
        index = vcon->src.name.num();
    }
    con_map[index][vcon->dst] = vcon;
}

void virtual_messenger::delete_con_from_sharecon(virtual_connection *vcon)
{
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == vcon->src.name.type())
    {
        index = vcon->src.name.num();
    }
    NUM_MAP::iterator p = share_con.find(index);
    if (p != share_con.end())
    {
        ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q = p->second.find(vcon->dst);
        if(q != p->second.end())
        {
            if(q->second.get() == vcon)
            {
                ldout(cct, 1)<< "delete src:"<< vcon->src <<"  dst:" << vcon->dst << "  from con_map" 
                     << " vcon is:"<< vcon << " con:" << vcon->con<<  dendl;
                vcon->clear_delay_list_msg();
                delete_vcon_from_seqMap(q->second);
                if(vcon->con)
                {
                    delete_vcon_from_map(vcon);
                }
                
                p->second.erase(q);
            }
        }
    }
}


void virtual_messenger::delete_con_from_sharecon_lite(virtual_connection *vcon)
{
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == vcon->src.name.type())
    {
        index = vcon->src.name.num();
    }
    NUM_MAP::iterator p = share_con.find(index);
    if (p != share_con.end())
    {
        ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q = p->second.find(vcon->dst);
        if(q != p->second.end())
        {
            if(q->second.get() == vcon)
            {
                ldout(cct, 1)<< "delete src:"<< vcon->src <<"  dst:" << vcon->dst << "  from con_map" 
                     << " vcon is:"<< vcon << " con:" << vcon->con<<  dendl;
                vcon->clear_delay_list_msg();              
                p->second.erase(q);
            }
        }
    }
}


Vri_ConnectionRef virtual_messenger::find_vcon_from_sharecon(entity_inst_t src, entity_inst_t dst, CON_MAP& con_map)
{
    //ldout(cct, 10)<< "begin to find virtual_connection src:"<< src << "  dst:" << dst <<  dendl;
    CON_MAP::iterator p = con_map.find(src);
    if (p == con_map.end())
    {
        //ldout(cct, 10)<< "not find src "<< src << "  from share_con " <<dendl;
        return NULL;
    }
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef> &temp = p->second;
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q = temp.find(dst);
    if(q == temp.end())
    {
        //ldout(cct, 10)<< "not find dst: "<< dst << "  from share_con" <<dendl;
        return NULL;
    }
    //ldout(cct, 10)<< "find virtual_connection: " <<q->second <<dendl;
    return q->second;
}


Vri_ConnectionRef virtual_messenger::find_vcon_from_sharecon(entity_inst_t src, entity_inst_t dst,NUM_MAP& con_map)
{
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == src.name.type())
    {
        index = src.name.num();
    }
    NUM_MAP::iterator p = con_map.find(index);
    if (p == con_map.end())
    {
        //ldout(cct, 10)<< "not find src "<< src << "  from share_con " <<dendl;
        return NULL;
    }
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef> &temp = p->second;
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q = temp.find(dst);
    if(q == temp.end())
    {
        //ldout(cct, 10)<< "not find dst: "<< dst << "  from share_con" <<dendl;
        return NULL;
    }
    //ldout(cct, 10)<< "find virtual_connection: " <<q->second <<dendl;
    return q->second;
}

uint64_t virtual_messenger::create_msg_id(virtual_connection *vcon)
{
    uint64_t id = 0;
    uint64_t type = vcon->src.name.type();
    const entity_inst_t& myinst = get_myinst();
    id |= type << 60;
    if(CEPH_ENTITY_TYPE_OSD == vcon->src.name.type())
    {
        id |= vcon->src.name.num() << 40;
    }
    else
    {
        if(-1 != myinst.name.num())
        {
            id |= myinst.name.num() << 40;
        }
    }
    id |= msg_id.inc();

    ldout(cct, 10)<< "create msg id: " << hex << id <<dendl;
    return id;
	
}

void virtual_messenger::send_vconnect_message(int type, virtual_connection *vcon, entity_inst_t src, entity_inst_t dst)
{
    if(NULL == vcon->con)
    {
        ldout(cct,0)<< "con is null, return "<<dendl;
        return;
    }
    ldout(cct, 30)<< "begin to send_vconnect_message  src " << src  << "dst is  "<< dst <<dendl;
    vconnect *mge = new vconnect(type);
    AuthAuthorizer *authorizer = NULL;
    ldout(cct, 30)<< "begin to get authorizer" <<  dendl;
    ms_get_authorizer_vcon(vcon->get_peer_type(), &authorizer, false,get_myinst().name.type(), src.name.num());
    vcon->auth = authorizer;
    if(authorizer)
    {
  
        ldout(cct, 30)<< "begin to set authorizer, protocol is " << authorizer->protocol << " length is " << authorizer->bl.length() << dendl;
        mge->length = authorizer->bl.length();
        mge->authorizer_protocol =  authorizer->protocol;
        mge->auth_req = authorizer->bl;
    }
    mge->set_header_src(get_ceph_inst(src));
    mge->set_header_dst(get_ceph_inst(dst));
    mge->set_vcon_seq(vcon->src_id);
    mge->set_msg_seq(create_msg_id(vcon));
    ldout(cct, 1)<< "begin to get send vconnect msg "<<" src: "<< src <<" dst: "<<dst <<" msg id  "<<hex << mge->get_msg_seq() << " vcon_seq:" << mge->get_vcon_seq() <<  dendl;
    vcon->con->send_message(mge);
}

void virtual_messenger::send_accept_message(int type,  virtual_connection *vcon,  bufferlist reply,entity_inst_t src, entity_inst_t dst)
{
    if(NULL == vcon->con)
    {
        ldout(cct,0)<< "con is null, return "<<dendl;
        return;
    }
    ldout(cct, 30)<< "begin to send_accept_message  src " << src  << " dst is  "<< dst <<dendl;
    vaccept *mge = new vaccept(type);
    mge->length = reply.length();
    mge->auth_reply = reply;
    mge->set_header_src(get_ceph_inst(src));
    mge->set_header_dst(get_ceph_inst(dst));
    mge->set_vcon_seq(vcon->src_id);
    mge->set_msg_seq(create_msg_id(vcon));
	
    ldout(cct, 1)<< "begin to get send vaccept msg"<< " src: " << src<<" dst: "<< dst<< " msg id  "<<hex << mge->get_msg_seq() <<  dendl;
    vcon->con->send_message(mge);
}

void virtual_messenger::send_remove_vcon_message(int type, virtual_connection *vcon,  entity_inst_t src, entity_inst_t dst)
{
    if(NULL == vcon->con)
    {
        ldout(cct,0)<< "con is null, return "<<dendl;
        return;
    }
    ldout(cct, 30)<< "begin to send_reset_message  src " << src  << " dst is  "<< dst <<dendl;
    vremove *mge = new vremove(type);
    mge->set_header_src(get_ceph_inst(src));
    mge->set_header_dst(get_ceph_inst(dst));
    mge->set_vcon_seq(vcon->src_id);
    mge->set_msg_seq(create_msg_id(vcon));
	
    ldout(cct, 1)<< "begin to get send remove msg"<< " src: "<<src << " dst: "<< dst << " msg id  "<<hex << mge->get_msg_seq() <<  dendl;
    vcon->con->send_message(mge);
}

void virtual_messenger::send_remove_ack_message(int type, virtual_connection *vcon,  entity_inst_t src, entity_inst_t dst)
{
    if(NULL == vcon->con)
    {
        ldout(cct,0)<< "con is null, return "<<dendl;
        return;
    }
    ldout(cct, 30)<< "begin to send_remove_ack_message  src " << src  << " dst is  "<< dst <<dendl;
    vremove_ack *msg = new vremove_ack(type);
    msg->set_header_src(get_ceph_inst(src));
    msg->set_header_dst(get_ceph_inst(dst));
    msg->set_vcon_seq(vcon->src_id);
    msg->set_msg_seq(create_msg_id(vcon));
	
    ldout(cct, 1)<< "begin to get send remove ack msg"<< " src: "<<src << " dst: "<< dst << " msg id  "<<hex << msg->get_msg_seq() <<  dendl;
    vcon->con->send_message(msg);
}
	

ConnectionRef virtual_messenger::get_connection(const entity_inst_t  &src, const entity_inst_t  &dst)
{
     if(is_stop())
     {
        return NULL;
     }
	 
     Vri_ConnectionRef find_con = NULL;
loop:
     find_con = get_vconnection(src,dst);
     if(find_con->con)
     {
         ldout(cct, 10)<< "find virtual_connection:"<< find_con 
         << "  from  share_con return it con is: "<<find_con->con  << " vcon seq: "<< find_con->src_id<<  dendl;
         return find_con;
     }
     else
     {
         ldout(cct, 10)<<" find con is null, find next" <<dendl;
         usleep(10);
         goto loop;
     }
}

Vri_ConnectionRef virtual_messenger::get_vconnection(const entity_inst_t  &src, const entity_inst_t  &dst)
{
   
    ldout(cct, 1)<< "begin to get virtual_connection src:"<< src << "  dst:" << dst <<  dendl;
    Vri_ConnectionRef v = NULL;
    lock.get_read();
    Vri_ConnectionRef find_con = NULL;
    if(!src.addr.is_ip() || src.addr.is_blank_ip())
    {
        find_con = find_vcon_from_sharecon(src, dst,con_bak);
    }
    else
    {
        find_con = find_vcon_from_sharecon(src, dst,share_con);
    }
    lock.put_read();
    if(find_con)
    {
        ldout(cct, 10)<< "find virtual_connection:"<< find_con << "  from  share_con return it" <<  dendl;
        if(VCON_MARK_DOWNING == find_con->con_state)
        {
#if 0
            v = find_con;
            v->con_state  = VCON_CONNECTING;
            goto vcon_reuse;
#endif 
            lock.get_write();
            delete_con_from_sharecon(find_con.get());
            lock.put_write();
            ldout(cct, 10)<< "get_vconnection mark_downing" << dendl;
            goto mark_downing;
        }
        return find_con;
    }
mark_downing:
    ldout(cct,10) << "not find virtual_connection, create it " << dendl;

    lock.get_write();
    v = new virtual_connection(cct,this);
    v->src = src;
    v->dst = dst;
    
    v->vcon_dir = VCON_CLIENT;
    /*new virtual_con, add to share_con*/
    if(!src.addr.is_ip() || src.addr.is_blank_ip())
    {
        ldout(cct,10) << " insert into con_bak src: "<< src << dendl;
        find_con = find_vcon_from_sharecon(src, dst,con_bak);
        if(find_con)
        {
            lock.put_write();
            return find_con;
        }
        v->src_id = get_available_seqId(v);//vcon_seq.inc();//get_available_seqId(v);
        insert_vcon_to_sharecon(v, con_bak);
    }
    else
    {
    	find_con = find_vcon_from_sharecon(src, dst,share_con);
    	if(find_con)
    	{
    	    lock.put_write();
    	    return find_con;
    	}
    	v->src_id = get_available_seqId(v);//vcon_seq.inc();//get_available_seqId(v);
       insert_vcon_to_sharecon(v, share_con);
    }
    lock.put_write();
//vcon_reuse:	
    v->con = messgr->get_connection(dst);
    lock.get_write();

    insert_to_con_vcon_map(v);  
    
    lock.put_write();
    
    ldout(cct,1) << "new virtual_con: "<< v << " con is: "<< v->con << " src: "  << src << "  dst:" << dst << " vcon seq: "<< v->src_id<<"  we need to send MSG_CONNECT msg" << dendl;
    /*new virtual_con  handle connect*/
    if(src.addr.is_ip() && !src.addr.is_blank_ip())
       send_vconnect_message(MSG_CONNECT, v.get(), src, dst);

    ldout(cct,30)<< "out get_connection  peer_type is "<< v->get_peer_type() << dendl;
    return v;

}


ConnectionRef virtual_messenger::get_connection(const entity_inst_t  &dst)
{
    if(is_stop())
    {
        return NULL;
    }
    ldout(cct, 10)<< "begin to get virtual_connection src:"<< get_myinst() << "  dst:" << dst <<  dendl;
    entity_inst_t src = get_myinst();
    return get_connection(src,dst);
}



/*get_lookback_connection, src=dst */
ConnectionRef virtual_messenger::get_loopback_connection(const entity_inst_t &inst)
{
    if(is_stop())
    {
        return NULL;
    }
    Vri_ConnectionRef v = NULL;
    entity_inst_t dst = inst;
    ldout(cct, 1)<< "begin to get virtual_connection src:"<< inst << "  dst:" << dst <<  dendl;

    lock.get_read();
    Vri_ConnectionRef find_con = NULL; 
    if(!inst.addr.is_ip() || inst.addr.is_blank_ip())
    {
        find_con = find_vcon_from_sharecon(inst, dst,con_bak);
    }
    else
    {
        find_con = find_vcon_from_sharecon(inst, dst,share_con);
    }
    if(find_con)
    {
        ldout(cct, 10)<< "find virtual_connection:"<< find_con << "  from  share_con return it" <<  dendl;
        lock.put_read();
        if(VCON_MARK_DOWNING == find_con->con_state)
        {
 #if 0
            v = find_con;
            v->con_state  = VCON_CONNECTING;
            goto vcon_reuse;
 #endif
            lock.get_write();
            delete_con_from_sharecon(find_con.get());
            lock.put_write();
            goto mark_downing;
        }

        return find_con;
    }
mark_downing:
    lock.put_read();
    ldout(cct,10) << "not find con, new it " << dendl;
    lock.get_write();

    v = new virtual_connection(cct,this);
    v->src = inst;
    v->dst = dst;
    
    v->dst_id = v->src_id;
    v->vcon_dir = VCON_CLIENT;
    /*new virtual_con, add to share_con*/
    if(!inst.addr.is_ip() || inst.addr.is_blank_ip())
    {
    	find_con = find_vcon_from_sharecon(inst, dst,con_bak);
        if(find_con)
        {
            lock.put_write();
            return find_con;
        }
        v->src_id = get_available_seqId(v);//vcon_seq.inc(); 
        insert_vcon_to_sharecon(v,con_bak);
    }
    else
    {
       find_con = find_vcon_from_sharecon(inst, dst,share_con);
    	if(find_con)
    	{
    	    lock.put_write();
    	    return find_con;
    	}
    	v->src_id = get_available_seqId(v);//vcon_seq.inc(); 
       insert_vcon_to_sharecon(v, share_con);
    }
    lock.put_write();

//vcon_reuse:
    v->con = messgr->get_connection(dst);
    lock.get_write();
    insert_to_con_vcon_map(v); 
    lock.put_write();
    
    ldout(cct,10) << "new virtual_con: "<< v <<"  we need to handle connect and fast connect" << dendl;
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == inst.name.type())
    {
        index = inst.name.num();
    }

    dispatch_lock.get_read();
    list<Dispatcher*> &fastDispatchers = v_fast_dispatchers[index];
    for (auto p : fastDispatchers) 
    {
        p->ms_handle_fast_connect(v.get());
    }
    #if 0

    if(CEPH_ENTITY_TYPE_OSD == inst.name.type())
    {
        ldout(cct,10) << "virtual_con: "<< v << " src is osd (src:  "<< inst <<" )"  << "  find dispatcher"<< dendl;
        for (list<Dispatcher*>::iterator p = v_fast_dispatchers.begin();p != v_fast_dispatchers.end();++p) 
        {
            if(inst.name.num() == (*p)->osd_id)
            {
                ldout(cct,10) << "find dispatcher src num is: "<< inst.name.num() << " dispatcher osd_id is: "<< (*p)->osd_id << dendl;
                (*p)->ms_handle_fast_connect(v.get());
            }
        }
    }
    else
    {
        ldout(cct,10) << " virtual_con: "<< v << " src type is not osd, ergodic v_dispatchers to handle connect" << dendl;
        for (list<Dispatcher*>::iterator p = v_fast_dispatchers.begin();p != v_fast_dispatchers.end();++p) 
        {
            (*p)->ms_handle_fast_connect(v.get());
        }
    }
    #endif

    dispatch_lock.put_read();
    v->con_state = VCON_CONNECTED;
    return v;
}

ConnectionRef virtual_messenger::get_loopback_connection()
{
    if(is_stop())
    {
        return NULL;
    }
    entity_inst_t src = get_myinst();
    return get_loopback_connection(src);
}

void virtual_messenger::add_dispatcher_head(Dispatcher *d)
{
    if(is_stop())
    {
        return;
    }
    
    int index = d->osd_id;

    dispatch_lock.get_write();
    bool first = v_dispatchers.empty();

    /*other module etc:osd,  add dispatcher to virtual_messenger dispatcher list*/
    v_dispatchers[index].push_front(d);
    if (d->ms_can_fast_dispatch_any())
        v_fast_dispatchers[index].push_front(d);
    if (first)
    {
        /*add virtual_messenger dispatcher to messenger dispatch list*/
        messgr->add_dispatcher_head(this);
    }
    dispatch_lock.put_write();

}

void virtual_messenger::add_dispatcher_tail(Dispatcher *d)
{
    if(is_stop())
    {
        return;
    }

    int index = d->osd_id;
    
    dispatch_lock.get_write();
    bool first = v_dispatchers.empty();

    /*other module etc:osd,  add dispatcher to virtual_messenger dispatcher list*/
    v_dispatchers[index].push_back(d);
    if (d->ms_can_fast_dispatch_any())
        v_fast_dispatchers[index].push_back(d);
    if (first)
    {
        /*add virtual_messenger dispatcher to messenger dispatcher list*/
        messgr->add_dispatcher_tail(this);
    }
    dispatch_lock.put_write();

}

void virtual_messenger::delete_dispatch(int osd_id)
{
    if(is_stop())
    {
        return;
    }
    mark_down_all_delete_dispatch(osd_id);
	
    ldout(cct,1) << "delete dispatcher osd id is: " << osd_id << "  from virtual_messenger " << dendl;
    dispatch_lock.get_write();
    int index = osd_id;
    v_dispatchers[index].clear();
    v_fast_dispatchers[index].clear();
    
    #if 0
    for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();) 
    {
        if((*p)->osd_id == osd_id)
        {
            ldout(cct,10) << "find dispatche from v_dispatchers  delete it " << dendl;
            p = v_dispatchers.erase(p);
        }
        else
        {
            p++;
        }

    }

    for (list<Dispatcher*>::iterator q = v_fast_dispatchers.begin();q != v_fast_dispatchers.end();) 
    {
        if((*q)->osd_id == osd_id)
        {
            ldout(cct,10) << "find dispatche from v_fast_dispatchers  delete it " << dendl;
            q = v_fast_dispatchers.erase(q);
        }
        else
        {
            q++;
        }
    }
    #endif
    
    dispatch_lock.put_write();
}

entity_inst_t get_message_inst(struct ceph_entity_inst  ceph_inst)
{
    entity_inst_t inst;
    
    inst.addr.type = ceph_inst.addr.type;
    inst.addr.nonce = ceph_inst.addr.nonce;
    memcpy(&(inst.addr.addr), &(ceph_inst.addr.in_addr), sizeof(ceph_inst.addr.in_addr));
    inst.name._num = ceph_inst.name.num;
    inst.name._type = ceph_inst.name.type;
    
    return inst;
}

struct ceph_entity_inst get_ceph_inst(entity_inst_t inst)
{
    struct ceph_entity_inst ceph_inst = {0};
    ceph_inst.name.num = inst.name.num();
    ceph_inst.name.type = inst.name._type;
    
    ceph_inst.addr.type = inst.addr.type;
    ceph_inst.addr.nonce = inst.addr.nonce;
    memcpy(&(ceph_inst.addr.in_addr), &(inst.addr.addr), sizeof(ceph_inst.addr.in_addr));
    
    return ceph_inst;
}

bool virtual_messenger::ms_can_fast_dispatch(Message *m) const
{
    if(is_stop())
    {
        return true;
    }

    if ( MSG_CONNECT == m->get_type() || MSG_ACCEPT == m->get_type() || MSG_VCON_NO_EXISTENT == m->get_type() || MSG_REMOVE_VCON_ACK == m->get_type() )
    {
        return false;	
    }

    entity_inst_t dst = get_message_inst(m->get_header_dst());
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
    {
        index = dst.name.num();
    }

    dispatch_lock.get_read();
    if ( CEPH_ENTITY_TYPE_OSD == get_mytype() && v_fast_dispatchers[index].empty() )
    {
        dispatch_lock.put_read();
        return true;
    }

    list<Dispatcher*> &fastDispatchers = v_fast_dispatchers[index];
    for (auto p : fastDispatchers) 
    {
        if (p->ms_can_fast_dispatch(m))
        {
            dispatch_lock.put_read();
            return true;
        }
    }
    dispatch_lock.put_read();
    return false;
}


void virtual_messenger::ms_fast_dispatch(Message *m) 
{
    if(is_stop())
    {
        return;
    }

    m->set_dispatch_stamp(ceph_clock_now(cct));
	
    ldout(cct,30)<< " ### msg type is "<< m->get_type() << "from vcon_seq: "<< m->get_vcon_seq()<<dendl;
    ldout(cct,30)<< "msg id is: "<< hex <<m->get_msg_seq() <<dendl;
    entity_inst_t  src = get_message_inst(m->get_header_src());
    entity_inst_t  dst = get_message_inst(m->get_header_dst());
    stringstream msg_detail;
    msg_detail<< *m;
    string msg_tmp = msg_detail.str();
    msg_timeout_detail time_tmp;
    time_tmp.recv_stamp = m->get_recv_stamp();
    time_tmp.throttle_stamp = m->get_throttle_stamp();
    time_tmp.recv_complete_stamp = m->get_recv_complete_stamp();
    time_tmp.preprocess_end_stamp = m->get_dispatch_stamp();
    ldout(cct,1)<< " ### msg type is "<< m->get_type() << " from vcon_seq: "<< m->get_vcon_seq() 
		 <<" from src: "<< src << " to: "<< dst << " msg id is"<< hex << m->get_msg_seq()<<dendl;
	
    uint64_t time1 = get_jiffies();
    dfx_status->fast_dispatch_handle_cnt.inc();

    if(NULL == m->get_connection())
    {
        ldout(cct,0) <<"drop msg"<< " from: "<< src << " to: "<< dst << "msg type is:"<< m->get_type()
              << hex << " msg id: "<< m->get_msg_seq()<<dendl;
        m->put();
        return;
    }

    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
    {
        index = dst.name.num();
    }
	
    uint64_t time2 = get_jiffies();
    utime_t begin_dispatcher = ceph_clock_now(cct);
    dispatch_lock.get_read();
    list<Dispatcher*> &fastDispatchers = v_fast_dispatchers[index];
    if ( CEPH_ENTITY_TYPE_OSD == dst.name.type() && fastDispatchers.empty() )
    {
        ldout(cct, 0) << "v_fast_dispatchers is empty, drop msg"<< " from: "<< src << " to: "<< dst << "msg type is:"<< m->get_type()
             << hex << " msg id: "<< m->get_msg_seq()<<dendl;
        m->put();
        dispatch_lock.put_read();
        return;
    }

    for (auto p : fastDispatchers) 
    {
        p->ms_fast_dispatch(m);
        dispatch_lock.put_read();
		
	 utime_t end_dispatcher = ceph_clock_now(cct);		
        uint64_t time3 = get_jiffies();
        dfx_status->fast_dispatch_dispatcher_handle_cnt.inc();
        uint64_t period1 = get_time_space(time1,time3);
        uint64_t period2 = get_time_space(time2,time3);
        dfx_status->fast_dispatch_handle_sum.add(period1);
        dfx_status->fast_dispatch_handle_max< period1 ?  dfx_status->fast_dispatch_handle_max = period1 : 0;
        dfx_status->fast_dispatch_dispatcher_handle_sum.add(period2);
        dfx_status->fast_dispatch_dispatcher_handle_max< period2 ?  dfx_status->fast_dispatch_dispatcher_handle_max = period2 : 0;
        if(period1 >= cct->_conf->vcon_handle_status_timeout * 1000 || period2 >= cct->_conf->vcon_handle_status_timeout * 1000)
        {
            push_msg_type_to_list(src, dst, msg_tmp, time_tmp,begin_dispatcher, end_dispatcher, FAST_DISPATCH);
            ldout(cct, 0) << "handle timeout  period1:" << period1 << "  period2: "<< period2 << " " << msg_detail.str() << dendl;
        }
        return;
    }

    #if 0
    for (list<Dispatcher*>::iterator p = v_fast_dispatchers.begin();p != v_fast_dispatchers.end();++p) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30)<< "msg to OSD, find dispatcher  according to osd_id: "<< src.name.num() << dendl;
            if(dst.name.num() == (*p)->osd_id)
            {
                ldout(cct,10)<< " find dispatcher, handle msg" << dendl;
                (*p)->ms_fast_dispatch(m);
                dispatch_lock.put_read();
                return;
            }
        }
        else
        {
            ldout(cct,10)<< " mag not to osd, handle msg" << dendl;
            (*p)->ms_fast_dispatch(m);
            dispatch_lock.put_read();
            return;
        }
    }
    #endif
    dispatch_lock.put_read();
    utime_t end_dispatcher = ceph_clock_now(cct);
    uint64_t time5 = get_jiffies();
    uint64_t period5 = get_time_space(time1,time5);
    dfx_status->fast_dispatch_handle_sum.add(period5);
    dfx_status->fast_dispatch_handle_max< period5 ?  dfx_status->fast_dispatch_handle_max = period5 : 0;
    if(period5 >= cct->_conf->vcon_handle_status_timeout * 1000)
    {
        push_msg_type_to_list(src, dst, msg_tmp, time_tmp,begin_dispatcher, end_dispatcher, FAST_DISPATCH);
        ldout(cct, 0) << "handle timeout  period5:" << period5 << " " << msg_detail.str()<< dendl;
    }
    ldout(cct,0)<<__func__<<" msg not handle type: "<< m->get_type() << " src: "<< src << " dst: "<< dst <<dendl;
    m->put();
}

void virtual_messenger::ms_preprocess_vcon(Message *m) 
{
    entity_inst_t  src = get_message_inst(m->get_header_src());
    entity_inst_t  dst = get_message_inst(m->get_header_dst());
    Vri_ConnectionRef delete_con = NULL;

    lock.get_write();
    Vri_ConnectionRef find_con = find_vcon_from_sharecon(dst, src,share_con);
    
    if(!find_con)
    {
        if(MSG_CONNECT == m->get_type())
        {
            ldout(cct,10)<< "not find virtual_con ,new it" << dendl;
            Vri_ConnectionRef v= new virtual_connection(cct, this);      
            v->src = dst;
            v->dst = src;
            v->src_id = get_available_seqId(v);//vcon_seq.inc();//get_available_seqId(v);
            v->con = m->get_connection();
   			
            insert_to_con_vcon_map(v);  
    		
            v->vcon_dir = VCON_SERVER;
            insert_vcon_to_sharecon(v, share_con);
            m->set_connection(v);
            ldout(cct,10)<< "new virtual vcon " << v << " con is "<< v->con << " v->src_id is: " << v->src_id << " msg id:"  <<m->get_msg_seq() <<  dendl; 
        }
        else
        {
            ldout(cct,0) <<"set msg null"<< " from: "<< src << " to: "<< dst << hex << " msg id: "<< m->get_msg_seq()<<dendl;
            m->set_connection(NULL);
            lock.put_write();
            return;
        }

    }
    else
    {
        if(0 != find_con->dst_id && find_con->dst_id < m->get_vcon_seq())
        {
            delete_con = find_con;
            delete_con_from_sharecon(find_con.get());
            Vri_ConnectionRef v= new virtual_connection(cct, this);      
            v->src = dst;
            v->dst = src;
            v->src_id = get_available_seqId(v);//vcon_seq.inc();//get_available_seqId(v);
            v->con = m->get_connection();
            insert_to_con_vcon_map(v);  
    		
            v->vcon_dir = VCON_SERVER;
            insert_vcon_to_sharecon(v, share_con);
            m->set_connection(v);
            ldout(cct,10)<< "new virtual vcon " << v << " con is "<< v->con<< dendl; 
            lock.put_write();
            //ms_handle_reset_vcon(delete_con.get());
            in_vcon_que(delete_con);
            return;
        }
        else
        {
            if(m->get_connection() != find_con->con)
            { 
                if(MSG_CONNECT == m->get_type() && NULL == find_con->con)
                {
                    find_con->con = m->get_connection();
                    insert_to_con_vcon_map(find_con);  		
                }
            	
                ldout(cct,10)<< "find virtual_con  con is: " << find_con->con << " msg con is:" << m->get_connection()<< dendl;
            }
            ldout(cct,10)<< "find virtual_con: " << find_con << " con is: "<< find_con->con << "  set msg con"<< dendl;
            m->set_connection(find_con);
            if (NULL == find_con->con)
            {
                m->set_connection(NULL);
                ldout(cct,0) <<"set msg null" << " vcon: " << find_con << " from: "<< src << " to: "<< dst << hex << " msg id: "<< m->get_msg_seq()<<dendl;
                lock.put_write();
                return;
            }
        }

    }
		
    lock.put_write();
    
	
}

void virtual_messenger::ms_fast_preprocess(Message *m) 
{
    if(is_stop())
    {
        return;
    }
    entity_inst_t  src = get_message_inst(m->get_header_src());
    entity_inst_t  dst = get_message_inst(m->get_header_dst());

    stringstream msg_detail;
    msg_detail<<*m;
    string msg_tmp = msg_detail.str();
    msg_timeout_detail time_tmp;
    time_tmp.recv_stamp = m->get_recv_stamp();
    time_tmp.throttle_stamp = m->get_throttle_stamp();
    time_tmp.recv_complete_stamp = m->get_recv_complete_stamp();
    time_tmp.preprocess_end_stamp = m->get_dispatch_stamp();

    const entity_inst_t& t_myinst = get_myinst();
    assert(t_myinst.name.type() == dst.name.type());
    if(CEPH_ENTITY_TYPE_OSD == src.name.type())
    {
        m->get_header().src = m->get_header_src().name;	
    }
    int msg_type = m->get_type();
    ldout(cct,30)<< " find virtual_connection  src: "<< dst << "  dst:"<< src <<dendl;
    ldout(cct,1)<< " ### msg type is "<< msg_type << " from vcon_seq: "<< m->get_vcon_seq() 
		 <<" from src: "<< src << " to: "<< dst << " msg id is"<< hex << m->get_msg_seq()<<dendl;
    ldout(cct,30)<< "msg id is: "<< hex <<m->get_msg_seq() <<dendl;
	
    if(MSG_CONNECT == msg_type)
    {
        return;
    }

    uint64_t time1 = get_jiffies();
    dfx_status->fast_preprocess_handle_cnt.inc();
    lock.get_read();
    Vri_ConnectionRef find_con = NULL;//find_vcon_from_sharecon(dst, src,share_con);
    if(m->get_vcon_dst_id())
    {
        SEQ_MAP::iterator it = seqMap.find(m->get_vcon_dst_id());
        if( it != seqMap.end())
        {
            find_con = it->second;
        }
    }
    else
    {
        find_con = find_vcon_from_sharecon(dst, src, share_con);
    }
    lock.put_read();
    if(!find_con)
    {
        ldout(cct,0)<<"not MSG_CONNECT msg, set con null " << m << " " << *m << hex << " msg id: "<< m->get_msg_seq() <<dendl;
        m->set_connection(NULL);
        //lock.Unlock();
        return;
    }
    else
    {
#if 0   //this part has no chance to excute, because when msg_tpye == MSG_CONNECT,it will return at top.//wqh
        if(m->get_connection() != find_con->con)
        { 
            if(MSG_CONNECT == msg_type && NULL == find_con->con)
            {
                find_con->con = m->get_connection();
            }
			
            ldout(cct,10)<< "find virtual_con  con is: " << find_con->con << " msg con is:" << m->get_connection()<< dendl;
        }
#endif       
        m->set_connection(find_con);
        if (NULL == find_con->con)
        {
            m->set_connection(NULL);
            ldout(cct,0) <<"set msg null"<< " from: "<< src << " to: "<< dst << " " << m << " " << *m << hex << " msg id: "<< m->get_msg_seq()<<dendl;
            //lock.Unlock();
            return;
        }
        ldout(cct,10)<< "find virtual_con: " << find_con << " con is: "<< find_con->con << "  set msg con" << "  msg id: "<< m->get_msg_seq() << dendl;
    }
		

    Connection *con = m->get_connection().get();
    virtual_connection *v = static_cast<virtual_connection *>(con);
    if(MSG_CONNECT != msg_type && MSG_ACCEPT != msg_type && 0 != v->dst_id)
    {    
        if(v->dst_id != m->get_vcon_seq())
        {
            m->set_connection(NULL);
            ldout(cct,0) <<"set msg null"<< " from: "<< src << " to: "<< dst << " " << m << " " << *m << hex << " msg id: "<< m->get_msg_seq() << " vcon: "<< v <<dendl;
            ldout(cct,0)<<"not the vcon" << " m->get_vcon_seq(): "<< m->get_vcon_seq() << " v->dst_id: " << v->dst_id<< " vcon: " <<v <<  dendl;
            return;
        }
    }

    if(MSG_CONNECT ==msg_type || MSG_ACCEPT == msg_type || 
       MSG_REMOVE_VCON_ACK == msg_type || MSG_VCON_NO_EXISTENT == msg_type)
    {
        return;
    }

    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
    {
        index = dst.name.num();
    }

    uint64_t time2 = get_jiffies();
    dispatch_lock.get_read();
    list<Dispatcher*> &fastDispatchers = v_fast_dispatchers[index];
    for (auto p : fastDispatchers) 
    {
        p->ms_fast_preprocess(m);
    }

    #if 0
    for (list<Dispatcher*>::iterator p = v_fast_dispatchers.begin();p != v_fast_dispatchers.end();++p) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30) <<"msg is to osd: "<< dst.name << " find dispatcher id:" <<(*p)->osd_id << dendl;
            if(static_cast<signed>(m->get_header_dst().name.num) == (*p)->osd_id)
            {
                ldout(cct,10) <<"find  dispatcher, osd id is:"<< (*p)->osd_id << dendl;
                (*p)->ms_fast_preprocess(m);
            }
        }
        else
        {
            (*p)->ms_fast_preprocess(m);
        }
    }
    #endif

    dispatch_lock.put_read();
    utime_t end_dispatcher = ceph_clock_now(cct);
    uint64_t time3 = get_jiffies();
    uint64_t period3 = get_time_space(time1,time3);
    uint64_t period4 = get_time_space(time2,time3);
    dfx_status->fast_preprocess_handle_sum.add(period3);
    dfx_status->fast_preprocess_handle_max< period3 ?  dfx_status->fast_preprocess_handle_max = period3 : 0;
    dfx_status->fast_preprocess_dispatcher_handle_sum.add(period4);
    dfx_status->fast_preprocess_dispatcher_handle_max< period4 ?  dfx_status->fast_preprocess_dispatcher_handle_max = period4 : 0;
    if(period3 >= cct->_conf->vcon_handle_status_timeout * 1000 || period4 >= cct->_conf->vcon_handle_status_timeout * 1000)
    {
        push_msg_type_to_list(src,dst,msg_tmp, time_tmp,m->get_recv_complete_stamp(),end_dispatcher, FAST_PREPROCESS);
        ldout(cct, 0) << "handle timeout  period3:" << period3 << "  period4: "<< period4 << " " << msg_detail.str()<< dendl;
    }

}

bool virtual_messenger::handle_vcon_accept_data(vaccept *m)
{
    entity_inst_t  src = get_message_inst(m->get_header_src());
    entity_inst_t  dst = get_message_inst(m->get_header_dst());
    Connection *vcon = m->get_connection().get();
    virtual_connection *v = static_cast<virtual_connection *>(vcon);

    if(0 != v->dst_id && v->dst_id >=m->get_vcon_seq())
    {
        ldout(cct,0)<< " vcon is complete :"<< v << " msg from: "<< src << " to "<< dst
             << "msg id is: " <<m->get_msg_seq() << " v-dst_id: " << v->dst_id << " m->get_vcon_seq() "  << m->get_vcon_seq() <<dendl;
        m->put();
        return true;
    }

    if (v->auth) {
        bufferlist::iterator iter = m->auth_reply.begin();
        if (!v->auth->verify_reply(iter)) {
            ldout(cct,0) << "failed verifying authorize reply" << dendl;
        }
    }

    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
    {
        index = dst.name.num();
    }

    dispatch_lock.get_read();

    list<Dispatcher*> &fastDispatchers = v_fast_dispatchers[index];
    for (auto p : fastDispatchers) 
    {
        ldout(cct,10)<< __func__<<" handle fast connect vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
        p->ms_handle_fast_connect(v);
    }

    list<Dispatcher*> &dispatchers = v_dispatchers[index];
    for (auto p : dispatchers) 
    {
        ldout(cct,10)<< __func__<<" msg to "<< dst.name <<" handle connect  "<< v << dendl;
        p->ms_handle_connect(v);
    }
    #if 0
    for (list<Dispatcher*>::iterator q = v_fast_dispatchers.begin(); q != v_fast_dispatchers.end(); ++q) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30) << __func__<<" msg is to osd: "<< dst.name << " find dispatcher to handle vaccept ##" << dendl;
            if(dst.name.num() == (*q)->osd_id)
            {
                ldout(cct,10)<< __func__<<" handle fast connect vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
                (*q)->ms_handle_fast_connect(v);                  
            }
        }
        else
        {
             assert(CEPH_ENTITY_TYPE_OSD != dst.name.type());
             ldout(cct,10)<< __func__<<" handle fast connect vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
             (*q)->ms_handle_fast_connect(v);
        }
    }

    for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30) <<__func__<<" msg is to osd: "<< dst.name << " find dispatcher to handle connect" << dendl;
            if(dst.name.num() == (*p)->osd_id)
            {
                ldout(cct,10)<< __func__<<" find dispatcher to handle connect osd id is "<< (*p)->osd_id << " vcon is "<< v  << dendl;
                (*p)->ms_handle_connect(v);               
            }
        }
        else
        {
            assert(CEPH_ENTITY_TYPE_OSD != dst.name.type());
            ldout(cct,10)<< __func__<<" msg to "<< dst.name<<" handle connect  "<< v << dendl;
            (*p)->ms_handle_connect(v);
        }
    }
    #endif

    dispatch_lock.put_read();
    v->con_state = VCON_CONNECTED;
    v->dst_id = m->get_vcon_seq();
    ldout(cct,1)<< "set vcon con_state is "<< v->con_state << " vcon is: "<< v
		<<" con is: "<< v->con << " dst_id is: " << v->dst_id <<dendl;
    v->send_delay_list_msg();
    m->put();
    return true;
}

bool virtual_messenger::handle_vcon_connect_data(vconnect *m)
{
    bufferlist auth_reply;
    CryptoKey vsession_key;
    bool visvalid = false;
    entity_inst_t  src = get_message_inst(m->get_header_src());
    entity_inst_t  dst = get_message_inst(m->get_header_dst());
    Connection *vcon = m->get_connection().get();
    virtual_connection *v = static_cast<virtual_connection *>(vcon);

    ldout(cct,10)<< "get vritual con from message "<< v << dendl;
    if(0 != v->dst_id && v->dst_id >= m->get_vcon_seq())
    {
        ldout(cct,0)<< " vcon is complete :"<< v << " msg from: "<< src << " to "<< dst
             << "msg id is: "<< hex <<m->get_msg_seq() << " v-dst_id: " << v->dst_id << " m->get_vcon_seq() "  << m->get_vcon_seq()  <<dendl;
        m->put();
        return true;
    }

    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
    {
        index = dst.name.num();
    }

    dispatch_lock.get_read();
    list<Dispatcher*> &dispatchers = v_dispatchers[index];
    for (auto p : dispatchers) 
    {
        if(0 != m->length)
        {
            if(p->ms_verify_authorizer(v, v->get_peer_type(), m->authorizer_protocol, m->auth_req, auth_reply, visvalid, vsession_key))
            {
                ldout(cct,10)<< __func__<<" ms_verify_authorizer is ok, visvlid is  " << visvalid <<dendl;
                break;
            }
        }
    }

    list<Dispatcher*> &fastDispatchers = v_fast_dispatchers[index];
    for(auto p : fastDispatchers)
    {
        ldout(cct,10)<< __func__<<" handle fast accept vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
        p->ms_handle_fast_accept(v);  
    }

    for (auto p : dispatchers) 
    {
        ldout(cct,10)<< __func__ <<" handle accept vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
        p->ms_handle_accept(v);
    }
    #if 0
    for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30) <<__func__<<" msg is to osd: "<< dst.name << " find dispatcher to handle verify##" << dendl;
            if(dst.name.num() == (*p)->osd_id)
            {
                ldout(cct,10)<< __func__<<" find dispatcher osd id is "<< (*p)->osd_id << " vcon is "<< v  << " auth len is "<< m->length << " proto is "<< m->authorizer_protocol << dendl;
                if(0 != m->length)
                {
                    if((*p)->ms_verify_authorizer(v, v->get_peer_type(), m->authorizer_protocol, m->auth_req, auth_reply, visvalid, vsession_key))
                    {
                        ldout(cct,10)<< __func__<<" ms_verify_authorizer is ok, visvlid is  " << visvalid <<dendl;
                        break;
                    }
                }
            }
        }
        else
        {
             assert(CEPH_ENTITY_TYPE_OSD != dst.name.type());
             ldout(cct,30)<< __func__<<" find dispatcher id is "<< (*p)->osd_id << " vcon is "<< v  << " auth len is "<< m->length << " proto is "<< m->authorizer_protocol << dendl;
             if(0 != m->length)
             {
                 if((*p)->ms_verify_authorizer(v, v->get_peer_type(), m->authorizer_protocol, m->auth_req, auth_reply, visvalid, vsession_key))
                 {
                     ldout(cct,10)<< __func__<<" ms_verify_authorizer is ok"<<dendl;
                     break;
                 }
             }
        }
    }

    for (list<Dispatcher*>::iterator q = v_fast_dispatchers.begin();q != v_fast_dispatchers.end();++q) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30) << __func__ <<" msg is to osd: "<< dst.name << " find dispatcher to handle fast accept##" << dendl;
            if(dst.name.num() == (*q)->osd_id)
            {
                ldout(cct,10)<< __func__<<" handle fast accept vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
                (*q)->ms_handle_fast_accept(v);                  
            }
        }
        else
        {
             assert(CEPH_ENTITY_TYPE_OSD != dst.name.type());
             ldout(cct,10)<< __func__<<" handle fast accept vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
             (*q)->ms_handle_fast_accept(v);
        }
    }

    for (list<Dispatcher*>::iterator r = v_dispatchers.begin();r != v_dispatchers.end();++r) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30) <<__func__ <<" msg is to osd: "<< dst.name << " find dispatcher to handle accept##" << dendl;
            if(dst.name.num() == (*r)->osd_id)
            {
                ldout(cct,10)<< __func__ <<" handle accept vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
                (*r)->ms_handle_accept(v);
            }
        }
        else
        {
             assert(CEPH_ENTITY_TYPE_OSD != dst.name.type());
             ldout(cct,10)<< __func__<<" handle accept vcon src: "<< v->src << " dst: "<< v->dst<<dendl;
             (*r)->ms_handle_accept(v);
        }
    }
    #endif

    dispatch_lock.put_read();

    ldout(cct,1)<< "begin to send  MSG_ACCEPT to "<<src << " from "<< dst <<" vcon: " << v <<" v->con is "<< v->con << dendl;
    send_accept_message(MSG_ACCEPT, v, auth_reply, dst, src);
    
    v->con_state = VCON_CONNECTED;
    v->dst_id = m->get_vcon_seq();
    ldout(cct,1)<< "set vcon con_state is "<< v->con_state << "vcon is:"<< v
		<<" con is "<< v->con<< " v->dst_id is: " << v->dst_id << dendl;
    v->send_delay_list_msg();
    m->put();
    return true;
}

bool virtual_messenger::handle_vcon_no_existent_data(Message *m)
{
    entity_inst_t  src = get_message_inst(m->get_header_src());
    entity_inst_t  dst = get_message_inst(m->get_header_dst());	
    ldout(cct,10)<< __func__<< " begin to handle vcon no existent" <<dendl;
    Connection *vcon = m->get_connection().get();
    virtual_connection *v = static_cast<virtual_connection *>(vcon);
    lock.get_read();
    Vri_ConnectionRef conref = find_vcon_from_sharecon(v->src, v->dst, share_con);
    lock.put_read();
    ldout(cct,10)<< __func__ <<" begin to reset vcon:"<< v<<dendl;
    lock.get_write();
    delete_con_from_sharecon(v);
    lock.put_write();
    send_remove_ack_message(MSG_REMOVE_VCON_ACK, v, dst, src);
    //ms_handle_remote_reset_vcon(v);
    if(conref)
        in_vcon_que(conref);
    m->put();
    return true;
    
}

bool virtual_messenger::handle_vcon_remove_ack_data(Message *m)
{
    ldout(cct,10)<< __func__<< " begin to handle vcon no existent" <<dendl;
    Connection *vcon = m->get_connection().get();
    virtual_connection *v = static_cast<virtual_connection *>(vcon);
    if(VCON_MARK_DOWNING == v->con_state)
    {
        lock.get_write();
        delete_con_from_sharecon(v);
        lock.put_write();
    }
    m->put();
    return true;
    
}

#if 0
void virtual_messenger::handle_new_vcon(Message *m) 
{
    entity_inst_t  src = get_message_inst(m->get_header_src());
    entity_inst_t  dst = get_message_inst(m->get_header_dst());	
    ldout(cct,10)<<"begin to handle_new_con msg type is "<<m->get_type()<< " from "<<src <<" to "<< dst<<dendl;
	
    Connection *vcon = m->get_connection().get();
    virtual_connection *v = static_cast<virtual_connection *>(vcon);
    v->con_state = VCON_CONNECTED;
    ldout(cct,10)<< "set vcon con_state is "<< v->con_state << dendl;
	
    dispatch_lock.get_read();
    for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30) <<"msg is to osd: "<< dst.name << " find dispatcher to handle accept" << dendl;
            if(dst.name.num() == (*p)->osd_id)
            {
                (*p)->ms_handle_accept(v);
                (*p)->ms_handle_fast_accept(v);    
               
            }
        }
        else
        {
            ldout(cct,10) <<"msg is to: "<< dst.name <<" handle accept and fast accept"<< dendl;
            (*p)->ms_handle_accept(v);
            (*p)->ms_handle_fast_accept(v);
        }
    }
    dispatch_lock.put_read();
    v->send_delay_list_msg();
}
#endif

bool virtual_messenger::ms_dispatch(Message *m) 
{
    if(is_stop())
    {
        return false;
    }
    m->set_dispatch_stamp(ceph_clock_now(cct)); 
	
    ldout(cct,30)<< " ### msg type is "<< m->get_type() << "from vcon_seq: "<< m->get_vcon_seq()<<dendl;
    ldout(cct,30)<< "msg id is: "<< hex <<m->get_msg_seq() <<dendl;
    entity_inst_t  src = get_message_inst(m->get_header_src());
    entity_inst_t  dst = get_message_inst(m->get_header_dst());
    stringstream msg_detail;
    msg_detail<<*m;
    string msg_tmp = msg_detail.str();
    msg_timeout_detail time_tmp;
    time_tmp.recv_stamp = m->get_recv_stamp();
    time_tmp.throttle_stamp = m->get_throttle_stamp();
    time_tmp.recv_complete_stamp = m->get_recv_complete_stamp();
    time_tmp.preprocess_end_stamp = m->get_dispatch_stamp();
     
    ldout(cct,1)<< " ### msg type is "<< m->get_type() << " from vcon_seq: "<< m->get_vcon_seq() 
    <<" from src: "<< src << " to: "<< dst  << " msg id is "<< hex << m->get_msg_seq()<<dendl;

    bool ret = false;
    bool handle_flag = false;
    uint64_t time1 = get_jiffies();
    dfx_status->ms_dispatch_handle_cnt.inc();
	
    if(MSG_CONNECT == m->get_type())
    {
        ms_preprocess_vcon(m);
    }

     if(NULL == m->get_connection())
    {
        ldout(cct,0) <<"drop msg, connection is NULL "<< " from: "<< src << " to: "<< dst << "msg type: "<< m->get_type()
               <<hex << " msg id: "<< m->get_msg_seq()<<dendl;
        return false;
    }

    if(MSG_REMOVE_VCON_ACK == m->get_type())
    {
        handle_flag = true;
        ret = handle_vcon_remove_ack_data(static_cast<vconnect*>(m));
    }

    if(MSG_VCON_NO_EXISTENT == m->get_type())
    {
        handle_flag = true;
        ret =  handle_vcon_no_existent_data(m);
    }
    
    if(MSG_ACCEPT == m->get_type())
    {
        handle_flag = true;
        ret =  handle_vcon_accept_data(static_cast<vaccept*>(m));
    }

    if(MSG_CONNECT == m->get_type())
    {
        handle_flag = true;
        ret = handle_vcon_connect_data(static_cast<vconnect*>(m));
    }

    if(handle_flag)
    {
        utime_t end_dispatcher = ceph_clock_now(cct);
        uint64_t time2 = get_jiffies();
        uint64_t period1 = get_time_space(time1,time2);
        dfx_status->ms_dispatch_handle_sum.add(period1);
        dfx_status->ms_dispatch_dispatcher_handle_max< period1 ?  dfx_status->ms_dispatch_dispatcher_handle_max = period1 : 0;
        dfx_status->ms_dispatch_handle_sum.add(period1);
        dfx_status->ms_dispatch_handle_max< period1 ?  dfx_status->ms_dispatch_handle_max = period1 : 0;
        if(period1 >= cct->_conf->vcon_handle_status_timeout * 1000 )
        {
            push_msg_type_to_list(src, dst, msg_tmp,time_tmp,m->get_dispatch_stamp(), end_dispatcher,MS_DISPATCH);
            ldout(cct, 0) << "handle timeout  period1:" << period1 << " " << msg_detail.str() << dendl;
        }
        return ret;
    }
 
    Connection *vcon = m->get_connection().get();
    virtual_connection *v = static_cast<virtual_connection *>(vcon);
    if(VCON_CONNECTING == v->con_state)
    {
       /*new vcon state is VCON_CONNECTING(ms_fast_process create it), handle it,*/
        //handle_new_vcon(m);
    }

    if ( VCON_OSD_STOP == v->con_state )
    {
        ldout(cct,0) <<"drop msg, msg is old"<< " from: "<< src << " to: "<< dst << "msg type: "<< m->get_type()
             <<hex << " msg id: "<< m->get_msg_seq()<<dendl;
        return false;
    }

    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
    {
        index = dst.name.num();
    }
    uint64_t time3 = get_jiffies();
	
    dispatch_lock.get_read();
    utime_t begin_dispatcher = ceph_clock_now(cct);
    list<Dispatcher*> &dispatchers = v_dispatchers[index];
    for (auto p : dispatchers) 
    {
        if (p->ms_dispatch(m))
        {
            dispatch_lock.put_read();
            utime_t end_dispatcher = ceph_clock_now(cct);
            uint64_t time4 = get_jiffies();
            dfx_status->ms_dispatch_dispatcher_handle_cnt.inc();
            uint64_t period2 = get_time_space(time1,time4);
            uint64_t period3 = get_time_space(time3,time4);
            dfx_status->ms_dispatch_handle_sum.add(period2);
            dfx_status->ms_dispatch_handle_max< period2 ?  dfx_status->ms_dispatch_handle_max = period2 : 0;
            dfx_status->ms_dispatch_dispatcher_handle_sum.add(period3);
            dfx_status->ms_dispatch_dispatcher_handle_max< period3 ?  dfx_status->ms_dispatch_dispatcher_handle_max = period3 : 0;
            if(period3 >= cct->_conf->vcon_handle_status_timeout * 1000 || period2 >= cct->_conf->vcon_handle_status_timeout * 1000)
            {
                push_msg_type_to_list(src, dst, msg_tmp, time_tmp,begin_dispatcher, end_dispatcher, MS_DISPATCH);
                ldout(cct, 0) << "handle timeout  period2:" << period2 << "  period3: "<< period3 << " " << msg_detail.str()<< dendl;
            }
            ldout(cct,10) <<"ms_dispatch return true "<<" msg type is: "<< m->get_type()<< dendl;
            return true;
        }
	  else
	  {
	      dfx_status->ms_dispatch_dispatcher_handle_cnt.inc();
	  }
    }
    #if 0
    for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p) 
    {
        if(CEPH_ENTITY_TYPE_OSD == dst.name.type())
        {
            ldout(cct,30) <<"msg is to osd: "<< dst.name << " find dispatcher id:"<<(*p)->osd_id << dendl;
            if(dst.name.num() == (*p)->osd_id)
            {
                ldout(cct,10) <<"find  dispatcher, osd id is:"<< (*p)->osd_id << dendl;
                if ((*p)->ms_dispatch(m))
                {
                    dispatch_lock.put_read();
                    return true;
                }
            }
        }
        else
        {
            ldout(cct,10) <<"msg is to: "<< dst.name <<" handle it"<< dendl;
            bool flag = (*p)->ms_dispatch(m);
            if (flag)
            {
                ldout(cct,10) <<"ms_dispatch return true "<<" msg type is: "<< m->get_type()<< dendl;
                dispatch_lock.put_read();
                return true;
            }
            else
            {		    
                ldout(cct,10) <<"ms_dispatch return false "<<" msg type is: "<< m->get_type()<< dendl;
            }
        }
    }
    #endif

    dispatch_lock.put_read();

    utime_t end_dispatcher = ceph_clock_now(cct);
    uint64_t time6 = get_jiffies();
    uint64_t period6 = get_time_space(time1,time6);
    dfx_status->ms_dispatch_handle_sum.add(period6);
    dfx_status->ms_dispatch_handle_max< period6 ?  dfx_status->ms_dispatch_handle_max = period6 : 0;
    if(period6 >= cct->_conf->vcon_handle_status_timeout * 1000)
    {
        push_msg_type_to_list(src, dst, msg_tmp, time_tmp,begin_dispatcher, end_dispatcher, MS_DISPATCH);
        ldout(cct, 0) << "handle timeout  period6:" << period6 << " " << msg_detail.str()<< dendl;
    }
    ldout(cct, 0) << "ms_deliver_dispatch: unhandled message " << m << " " << *m << " from "
    << m->get_source_inst() << " to " << dst<< dendl;

    return false;
}

void virtual_messenger::ms_handle_connect(Connection *con)
{
    CON_MAP::iterator p;
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    list<virtual_connection *> msg_connect_list;

    lock.get_write();
    for(p = con_bak.begin(); p != con_bak.end(); p++)
    {
        for(q = p->second.begin(); q!= p->second.end();q++)
        {
            Vri_ConnectionRef v = q->second;
            v->src.addr.addr = get_myinst().addr.addr;
            insert_vcon_to_sharecon(v, share_con);
            insert_to_con_vcon_map(v);
            seqMap[v->src_id] = v;  //added 20170525
            msg_connect_list.push_back(v.get());
        }
    }
    con_bak.clear();
    lock.put_write();
    list<virtual_connection *>::iterator iter = msg_connect_list.begin();
    for(;iter != msg_connect_list.end(); iter++)
    {
         virtual_connection *vcon = *iter;      
         ldout(cct,10) << "send VCONNECT con msg  src is : "<< vcon->src << " dst is: "<< vcon->dst << dendl;
         if(vcon->con)
         {
             send_vconnect_message(MSG_CONNECT, vcon, vcon->src, vcon->dst);
         }
    }
    msg_connect_list.clear();
    ldout(cct, 10)<< "ms handle connect src:"<< get_myinst() << "  dst addr:" << con->get_peer_addr() <<  dendl;
    return;
}

void virtual_messenger::ms_handle_fast_connect(Connection *con)
{
    ldout(cct, 30)<< "ms handle fast connect src:"<< get_myinst() << "  dst addr:" << con->get_peer_addr() <<  dendl;
    return;
}

void virtual_messenger::ms_handle_accept(Connection *con)
{
    ldout(cct, 30)<< "ms handle accept src:"<< get_myinst() << "  dst addr:" << con->get_peer_addr() <<  dendl;
    return;
}

void virtual_messenger::ms_handle_fast_accept(Connection *con)
{
    ldout(cct, 30)<< "ms handle fast accept src:"<< get_myinst() << "  dst addr:" << con->get_peer_addr() <<  dendl;
    return;
}

void virtual_messenger::handle_remote_reset_dispatcher(virtual_connection *con)
{
    int index = -1;
    dispatch_lock.get_read();
    
    if(CEPH_ENTITY_TYPE_OSD == con->src.name.type())
    {
        index = con->src.name.num();
    }
    list<Dispatcher*> &dispatchers = v_dispatchers[index];
    for (auto p : dispatchers) 
    {
        ldout(cct,10)<< __func__ << "begin to handle reset vcon: "<< con
            	<< " con is: "<< con->con<<dendl;
        p->ms_handle_remote_reset(con);
    }
    #if 0  
        for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p) 
        {
            if(CEPH_ENTITY_TYPE_OSD == q->second.name.type())
            {
                if(q->second.name.num() == (*p)->osd_id)
                {
                    ldout(cct,10) <<"ergodic entity_inst_t src "<< q->second << "handle dispatcher " << *p 
						<< " osd id is:"<<(*p)->osd_id << dendl;
                    ldout(cct,10)<< __func__ << " begin to handle remote reset vcon: "<< q->first
						<< " con is: "<< q->first->con<<dendl;
                    (*p)->ms_handle_remote_reset(q->first.get());			
                }
            }
            else
            {
                  
                ldout(cct,10)<< __func__ << "begin to handle reset vcon: "<< q->first
                	<< " con is: "<< q->first->con<<dendl;
                (*p)->ms_handle_remote_reset(q->first.get());			
            }
        }
    #endif

    lock.get_write();
    delete_con_from_sharecon(con);
    lock.put_write();
    
    dispatch_lock.put_read();

}


void virtual_messenger::handle_reset_dispatcher(virtual_connection *con)
{
    int index = -1;
    dispatch_lock.get_read();

    if(CEPH_ENTITY_TYPE_OSD == con->src.name.type())
    {
        index = con->src.name.num();
    }

    list<Dispatcher*> &dispatchers = v_dispatchers[index];
    for (auto p : dispatchers) 
    {
        if(p->ms_handle_reset(con))
        {
            ldout(cct,10)<< __func__ << "begin to handle reset vcon: "<< con 
                 << " con is: "<< con->con <<dendl;
            break;
        }
    }
    #if 0
        for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p) 
        {
            if(CEPH_ENTITY_TYPE_OSD == q->second.name.type())
            {
                if(q->second.name.num() == (*p)->osd_id)
                {
                    ldout(cct,10) <<"ergodic entity_inst_t src "<< q->second << "handle dispatcher " << *p << " osd id is:"<<(*p)->osd_id << dendl;
                    ldout(cct,10)<< __func__ << "begin to handle reset vcon: "<< q->first 
                    	<< " con is: "<< q->first->con<<dendl;
                    
                    if((*p)->ms_handle_reset(q->first.get()))
                    {
                    	break;
                    }
                }
            }
            else
            {
 
                ldout(cct,10)<< __func__ << "begin to handle reset vcon: "<< q->first
                	<< " con is: "<< q->first->con<<dendl;
                if((*p)->ms_handle_reset(q->first.get()))
                {
                    break;
                }
            }
        }
        #endif

    lock.get_write();
    delete_con_from_sharecon(con);
    lock.put_write();
	
    dispatch_lock.put_read();

}


void virtual_messenger::ms_handle_reset_vcon(virtual_connection* vcon)
{
    entity_inst_t src = vcon->src;

    ldout(cct,1)<< __func__ << "begin to handle reset vcon: "<<vcon << " con is "<< vcon->con<<dendl;
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == src.name.type())
    {
        index = src.name.num();
    }
   
    dispatch_lock.get_read();
    list<Dispatcher*> &dispatchers = v_dispatchers[index];
    for (auto p : dispatchers) 
    {
        ldout(cct,10)<< "handle vcon reset src: "<< vcon->src << " dst: "<< vcon->dst<<dendl;
        if(p->ms_handle_reset(vcon))
            break;
    }
    #if 0
    for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p) 
    {
        if(CEPH_ENTITY_TYPE_OSD == src.name.type())
        {
            if(src.name.num() == (*p)->osd_id)
            {
                ldout(cct,10)<< "handle vcon reset src: "<< vcon->src << " dst: "<< vcon->dst<<dendl;
                if((*p)->ms_handle_reset(vcon))
                    break;
            }
        }
	    else
  	    {
  	        ldout(cct,10)<< "handle vcon reset src: "<< vcon->src << " dst: "<< vcon->dst<<dendl;
  	        if((*p)->ms_handle_reset(vcon))
                break;
  	    }
    }
    #endif

    dispatch_lock.put_read();
    
}

void virtual_messenger::ms_handle_remote_reset_vcon(virtual_connection* vcon)
{
    entity_inst_t src = vcon->src;
    //entity_inst_t dst = vcon->dst;
    ldout(cct,1)<< __func__ << "begin to handle remote reset vcon: "<< vcon << " con is "<< vcon->con<<dendl;
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == src.name.type())
    {
        index = src.name.num();
    }

    dispatch_lock.get_read();
    list<Dispatcher*> &dispatchers = v_dispatchers[index];
    for (auto p : dispatchers) 
    {
        ldout(cct,10)<< "handle vcon reset src: "<< vcon->src << " dst: "<< vcon->dst<<dendl;
        if(p->ms_handle_reset(vcon))
            break;
    }
    #if 0
    for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p) 
    {
        if(CEPH_ENTITY_TYPE_OSD == src.name.type())
        {
            if(src.name.num() == (*p)->osd_id)
            {
                ldout(cct,10)<< "handle vcon reset src: "<< vcon->src << " dst: "<< vcon->dst<<dendl;
                if((*p)->ms_handle_reset(vcon))
                    break;
            }
        }
        else
        {
            ldout(cct,10)<< "handle vcon reset src: "<< vcon->src << " dst: "<< vcon->dst<<dendl;
            if((*p)->ms_handle_reset(vcon))
                break;
        }
    }
    #endif

    dispatch_lock.put_read();
    
}


bool virtual_messenger::ms_handle_reset(Connection *con)
{
    if(is_stop())
    {
        return true;
    }
    ldout(cct,1) << __func__<<" begin to handle connection reset:" << con<< dendl;

    lock.get_read();
#if 0    
    for(p = share_con.begin(); p!= share_con.end(); p++)
    {
        for(q = p->second.begin(); q!= p->second.end(); q++)
        {
            if(q->second->con.get() == con)
            {
                reset_con.insert(make_pair(q->second, p->first));
                outfile << this  << "  addr: " << con->get_peer_addr().addr  << '/' << con->get_peer_addr().nonce <<  "  vcon "   << q->second << "  src_id:"<< q->second->src_id << "  con "   << con<< "  \n";
                ldout(cct, 10) << "reset!! " << "vcon " << q->second << " con " <<q->second->con  << dendl;
            }
        }
    }
#endif
    CON_TO_VCON_MAP::iterator it = con_to_vcon_map.find(con->get_peer_addr());
    if (it != con_to_vcon_map.end())
    {
        std::set<uint64_t> findSet = it->second;
        lock.put_read();
        for(std::set<uint64_t>::iterator tor = findSet.begin(); tor!= findSet.end(); ++tor)
        {
            SEQ_MAP::iterator iter = seqMap.find(*tor);
            if(iter != seqMap.end())
            {
                if(iter->second->con.get() == con)
                {
                    in_vcon_que(iter->second);
                    lock.get_write();
                    delete_con_from_sharecon(iter->second.get());
                    lock.put_write();
                    //handle_reset_dispatcher(iter->second.get());
                    //ldout(cct, 10) << this << "  addr: " << con->get_peer_addr() << " vcon " << iter->second <<  "  src_id:  " << *tor << " con "  <<iter->second->con << dendl;
                }
        	}
            else
            {
                ldout(cct, 10) <<this <<  "  src_id:  " << *tor << dendl;
            }
        }
    }
    else
    {
        lock.put_read();
    }

    
    return true;
}


void virtual_messenger::ms_handle_remote_reset(Connection *con)
{
    if(is_stop())
    {
        return;
    }
    ldout(cct,1) << "begin to handle connection remote reset: " << con<< dendl;

    lock.get_read();
#if 0
    for(p = share_con.begin(); p!= share_con.end(); p++)
    {
        for(q = p->second.begin(); q!= p->second.end(); q++)
        {
            if(q->second->con == con)
            {
                reset_con.insert(make_pair(q->second, q->second->src));
            }
        }
    }
#endif
    CON_TO_VCON_MAP::iterator it = con_to_vcon_map.find(con->get_peer_addr());
    if (it != con_to_vcon_map.end())
    {
        std::set<uint64_t> findSet = it->second;
        lock.put_read();
        for(std::set<uint64_t>::iterator tor = findSet.begin(); tor!= findSet.end(); ++tor)
        {
            SEQ_MAP::iterator iter = seqMap.find(*tor);
            if(iter != seqMap.end())
            {
                if(iter->second->con.get() == con)
                {
                    in_vcon_que(iter->second);
                    lock.get_write();
                    delete_con_from_sharecon(iter->second.get());
                    lock.put_write();
                    //handle_remote_reset_dispatcher(iter->second.get());
                    //ldout(cct, 10) << this << "  addr: " << con->get_peer_addr() << " vcon " << iter->second <<  "  src_id:  " << *tor << " con "  <<iter->second->con << dendl;
                }
            }
            else
            {
                ldout(cct, 10) <<this <<  "  src_id:  " << *tor << dendl;
            }
        }
    }
    else
    {
        lock.put_read();
    }

}

bool virtual_messenger::ms_get_authorizer_vcon(int peer_type, AuthAuthorizer **a, bool force_new, int type, int osd_id)
{
    int index = -1;
    if(CEPH_ENTITY_TYPE_OSD == type)
    {
        index = osd_id;
    }
    dispatch_lock.get_read();
    list<Dispatcher*> &dispatchers = v_dispatchers[index];
    for (auto p : dispatchers) 
    {
        if (p->ms_get_authorizer(peer_type, a, force_new))
        {
            dispatch_lock.put_read();
            return true;
        }
    }
    #if 0
    for (list<Dispatcher*>::iterator p = v_dispatchers.begin();p != v_dispatchers.end();++p)
    {
        if(CEPH_ENTITY_TYPE_OSD == type)
        {
            if(osd_id == (*p)->osd_id)
            {
                if ((*p)->ms_get_authorizer(peer_type, a, force_new))
                {
                    dispatch_lock.put_read();
                    return true;
                }

            }
        }
        else
        {
            if ((*p)->ms_get_authorizer(peer_type, a, force_new))
            {
                dispatch_lock.put_read();
                return true;
            }
        }
    }
    #endif

    dispatch_lock.put_read();
    return true;
}


bool virtual_messenger::ms_get_authorizer(int peer_type, AuthAuthorizer **a, bool force_new) 
{
    if(is_stop())
    {
        return true;
    }
    return false;
}

bool virtual_messenger::ms_verify_authorizer(Connection *con, int peer_type,
        int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
        bool& isvalid, CryptoKey& session_key)
{
    if(is_stop())
    {
        return true;
    }
    isvalid = true;
    return true;
}

int virtual_messenger::get_vcon_num_from_map(virtual_connection* vcon)
{
    int con_num = 0;
    NUM_MAP::iterator p;
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    for(p = share_con.begin(); p != share_con.end(); p++)
    {
        for(q = p->second.begin(); q != p->second.end(); q++)
        {
            if(q->second->con == vcon->con)
            {
                con_num ++;
            }
        }
    }
    ldout(cct,10) << " vcon "<< vcon << "  in map num is"<< con_num << dendl;
    return con_num;
}

bool virtual_messenger::is_loacl_con(ConnectionRef con)
{
    string type = cct->_conf->ms_type;
    if("simple" == type)
    {
        return ((static_cast<SimpleMessenger*>(messgr))->local_connection == con);
    }
    else if("async" == type)
    {
        return ((static_cast<AsyncMessenger*>(messgr))->local_connection == con);
    }
    else
    {
        return false;
    }
}

#if 0
void virtual_messenger::mark_down(virtual_connection* vcon)
{
    ldout(cct,1) << "mark_down  vcon "<< vcon << " con is "<< vcon->con << dendl;
    lock.Lock();
    
    int con_num = 0;

    if(CEPH_ENTITY_TYPE_OSD == vcon->src.name.type()  || CEPH_ENTITY_TYPE_OSD == vcon->dst.name.type())
    {
        con_num = get_vcon_num_from_map(vcon);
        lock.Unlock();
        if(1 == con_num)
        {
               if(vcon->con)
               {
                   vcon->con_state = VCON_MARK_DOWNING;
                   ldout(cct,10) << "the last vcon has delete from map, handle con markdown, vcon src: "<< vcon->src << " dst is: "<<vcon->dst << dendl;
                   send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon, vcon->src, vcon->dst);
#if 0
                   if(is_loacl_con(vcon->con))
                   {
                     send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon, vcon->src, vcon->dst);
                   }
                   else
                   {
                          conn_mark_down(vcon->con);
                   }
#endif
               }
        }
        else
        {
            if(vcon->con)
            {
                vcon->con_state = VCON_MARK_DOWNING;
                ldout(cct,10) << "begin to send remove con msg  src is : "<< vcon->dst << " dst is: "<<vcon->src << dendl;
                send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon, vcon->src, vcon->dst);
            }
        }
    }
    else
    {
        lock.Unlock();
	    if(vcon->con)
            conn_mark_down(vcon->con);
    }	

    lock.Lock();
    //messgr->delete_con_from_sharecon(vcon);
    lock.Unlock();
}

void virtual_messenger::mark_down_impl(const entity_addr_t& dst_addr)
{
    ldout(cct,1) << "mark_down  dst addr "<< dst_addr << dendl;
    CON_MAP::iterator p;
    map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    list<Vri_ConnectionRef> mark_down_list;
    lock.Lock();
    for (p = share_con.begin(); p != share_con.end(); p++)
    {
        for(q = p->second.begin(); q != p->second.end();)
        {
            if(q->first.addr == dst_addr)
            {
                 Vri_ConnectionRef vcon = q->second;
                 vcon->clear_delay_list_msg();
                  q = p->second.erase(q);
                  if(vcon->con)
                  {
                      ldout(cct,10) << "mark down vcon:" << vcon << " con in vcon is " << vcon->con << dendl;
                      mark_down_list.push_back(vcon);
                  }
                 // lock.Unlock();
                  break;
             }
             else
             {
                 q++;
             }
        }
    }
    lock.Unlock();

#if 1
    list<Vri_ConnectionRef>::iterator iter = mark_down_list.begin();
    for(;iter != mark_down_list.end(); iter++)
    {
         Vri_ConnectionRef vcon = *iter; 
         ms_handle_reset_vcon(vcon.get());
    }
#endif

    msgr_mark_down(messgr, dst_addr);
}

void virtual_messenger::mark_down(const entity_addr_t& dst_addr, int src_osd, int dest_osd)
{
    ldout(cct,0) << "mark_down  dst addr "<< dst_addr << " src:" << src_osd << " dest:" << dest_osd << dendl;
    CON_MAP::iterator p;
    map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    list<Vri_ConnectionRef> mark_down_list;
    list<Vri_ConnectionRef> remove_vcon_list;
    int con_num = 0;
    Vri_ConnectionRef vcon = NULL;
    lock.Lock();
    for(p = share_con.begin(); p != share_con.end(); p++)
    {
        if (src_osd != p->first.name.num())
        {
            continue;
        }
        for(q = p->second.begin(); q != p->second.end();)
        {
            if(q->first.addr == dst_addr && dest_osd == q->first.name.num())
            {
                vcon = q->second;
                if(VCON_SERVER == vcon->vcon_dir)
                    break;
                con_num = get_vcon_num_from_map(vcon.get());
                if(1 == con_num)
                {
                    if(vcon->con)
                    {
                        ldout(cct,10) << "vcon->con->mark_down vcon is:"<< vcon << " con is " << vcon->con<<dendl;
                        ldout(cct,10) << "the last vcon will delete from map, handle con markdown, vcon src: "<< vcon->src << " dst is: "<<vcon->dst << dendl;
                        if(!is_loacl_con(vcon->con))
                        {
                            //mark_down_list.push_back(vcon);
                            remove_vcon_list.push_back(vcon);
                        }
                        else
                        {
                            remove_vcon_list.push_back(vcon);
                        }
                    }
                }
                else
                {
                    if(vcon->con)
                    {
                         remove_vcon_list.push_back(vcon);
                         ldout(cct,10) << "need send remove con msg  src is : "<< vcon->dst << " dst is: "<<vcon->src << dendl;
                    }
                }
                vcon->clear_delay_list_msg();
                //q = p->second.erase(q);
                q++;	
                break;
            }
            else
            {
                q++;
            }
        }
    }
    lock.Unlock();

    list<Vri_ConnectionRef>::iterator iter = mark_down_list.begin();
    for(;iter != mark_down_list.end(); iter++)
    {
        Vri_ConnectionRef vcon = *iter;  
        ms_handle_reset_vcon(vcon.get());
        conn_mark_down(vcon->con);
    }

    list<Vri_ConnectionRef>::iterator piter = remove_vcon_list.begin();
    for(;piter != remove_vcon_list.end(); piter++)
    {
        Vri_ConnectionRef vcon = *piter;      
        ldout(cct,10) << "send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
        if(vcon->con)
        {
            vcon->con_state = VCON_MARK_DOWNING;
            send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon.get(), vcon->src, vcon->dst);
            ms_handle_reset_vcon(vcon.get());
        }
    }
}


void virtual_messenger::mark_down_all()
{
    ldout(cct,1) << "mark_down  all "<< dendl;
    CON_MAP::iterator p;
    map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    list<Vri_ConnectionRef> reset_vcon_list;

    lock.Lock();
    for(p = share_con.begin(); p != share_con.end(); p++)
    {
        for(q = p->second.begin(); q!= p->second.end();)
        {
            Vri_ConnectionRef vcon = q->second;
            ldout(cct,10)<< "begin to mark down vcon " << vcon <<" con" <<vcon->con <<dendl;
            vcon->clear_delay_list_msg();
            reset_vcon_list.push_back(vcon);
            //ms_handle_reset_vcon(vcon);
            q = p->second.erase(q);			
        }
    }
    lock.Unlock();

    list<Vri_ConnectionRef>::iterator iter = reset_vcon_list.begin();
    for(;iter != reset_vcon_list.end(); iter++)
    {
        Vri_ConnectionRef vcon = *iter;  
        ms_handle_reset_vcon(vcon.get());
    }
    messgr->mark_down_all();
}

void virtual_messenger::mark_down_all(int osd_id)
{
    ldout(cct,1) << "mark_down  all osd id is "<< osd_id << dendl;
    CON_MAP::iterator p;
    map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    int con_num = 0;
    list<Vri_ConnectionRef> remove_vcon_list;
    list<Vri_ConnectionRef> mark_down_list;
	
    lock.Lock();
    for(p = share_con.begin(); p != share_con.end();p++)
    {
        if(p->first.name.num() != osd_id)
        {
            continue;
        }
        for(q = p->second.begin(); q != p->second.end();)
        {
            Vri_ConnectionRef vcon = q->second;
            con_num = get_vcon_num_from_map(vcon.get());
            if(1 == con_num)
            {
                if(vcon->con)
                {
                    ldout(cct,10) << "vcon->con->mark_down vcon is:"<< vcon << " con is " << vcon->con<<dendl;
                    ldout(cct,10) << "the last vcon will delete from map, handle con markdown, vcon src: "<< vcon->src << " dst is: "<<vcon->dst << dendl;	
                    if(!is_loacl_con(vcon->con))
                    {
                        remove_vcon_list.push_back(vcon);
                    }
                    else
                    {
                        remove_vcon_list.push_back(vcon);
                    }
                }
            }
            else
            {
                if(vcon->con)
                {
                    ldout(cct,10) << "need send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
                    remove_vcon_list.push_back(vcon);
                }
            }
            vcon->clear_delay_list_msg();
            q++;
        }
    }
    lock.Unlock();

    list<Vri_ConnectionRef>::iterator iter = mark_down_list.begin();
    for(;iter != mark_down_list.end(); iter++)
    {
        Vri_ConnectionRef vcon = *iter; 
        ms_handle_reset_vcon(vcon.get());
	    conn_mark_down(vcon->con);
    }
	
    list<Vri_ConnectionRef>::iterator piter = remove_vcon_list.begin();
    for(;piter != remove_vcon_list.end(); piter++)
    {
        Vri_ConnectionRef vcon = *piter;      
        ldout(cct,10) << "send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
        if(vcon->con)
        {
            vcon->con_state = VCON_MARK_DOWNING;
            send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon.get(), vcon->src, vcon->dst);
            ms_handle_reset_vcon(vcon.get());
        }
    }
}


void virtual_messenger::mark_down_all_delete_dispatch(int osd_id)
{
    ldout(cct,1) << "mark_down  all osd id is "<< osd_id << dendl;
    CON_MAP::iterator p;
    map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    int con_num = 0;
    list<Vri_ConnectionRef> remove_vcon_list;
    list<Vri_ConnectionRef> mark_down_list;
	
    lock.Lock();
    for(p = share_con.begin(); p != share_con.end();p++)
    {
        if(p->first.name.num() != osd_id)
        {
            continue;
        }
        for(q = p->second.begin(); q != p->second.end();)
        {
            Vri_ConnectionRef vcon = q->second;
			vcon->con_state = VCON_OSD_STOP;
            con_num = get_vcon_num_from_map(vcon.get());
            if(1 == con_num)
            {
                if(vcon->con)
                {
                    ldout(cct,10) << "vcon->con->mark_down vcon is:"<< vcon << " con is " << vcon->con<<dendl;
                    ldout(cct,10) << "the last vcon will delete from map, handle con markdown, vcon src: "<< vcon->src << " dst is: "<<vcon->dst << dendl;	
                    if(!is_loacl_con(vcon->con))
                    {
                        mark_down_list.push_back(vcon);
                    }
                    else
                    {
                        remove_vcon_list.push_back(vcon);
                    }
                }
            }
            else
            {
                if(vcon->con)
                {
                    ldout(cct,10) << "need send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
                    remove_vcon_list.push_back(vcon);
                }
            }
            vcon->clear_delay_list_msg();
            q = p->second.erase(q);	
        }
    }
    lock.Unlock();

    list<Vri_ConnectionRef>::iterator iter = mark_down_list.begin();
    for(;iter != mark_down_list.end(); iter++)
    {
        Vri_ConnectionRef vcon = *iter; 
        ms_handle_reset_vcon(vcon.get());
        conn_mark_down(vcon->con);
    }
	
    list<Vri_ConnectionRef>::iterator piter = remove_vcon_list.begin();
    for(;piter != remove_vcon_list.end(); piter++)
    {
        Vri_ConnectionRef vcon = *piter;      
        ldout(cct,10) << "send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
        if(vcon->con)
        {
            send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon.get(), vcon->src, vcon->dst);
            ms_handle_reset_vcon(vcon.get());
        }
    }
}
#endif

void virtual_messenger::mark_down(virtual_connection* vcon)
{
    ldout(cct,1) << "mark_down  vcon "<< vcon << " con is "<< vcon->con << dendl;

    if(CEPH_ENTITY_TYPE_OSD == vcon->src.name.type()  || CEPH_ENTITY_TYPE_OSD == vcon->dst.name.type())
    {      
        if(vcon->con && VCON_MARK_DOWNING != vcon->con_state)
        {
            vcon->con_state = VCON_MARK_DOWNING;
            vcon->clear_delay_list_msg();
            ldout(cct,10) << "begin to send remove con msg  src is : "<< vcon->dst << " dst is: "<<vcon->src << dendl;
            send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon, vcon->src, vcon->dst);
        }
    }
    else
    {
        if(vcon->con)
            conn_mark_down(vcon->con);
        lock.get_write();
        delete_con_from_sharecon(vcon);
        lock.put_write();
    }
}

void virtual_messenger::mark_down_impl(const entity_addr_t& dst_addr)
{	
    ldout(cct,1) << "mark_down  dst addr: "<< dst_addr << dendl;
    lock.get_write();
    list<Vri_ConnectionRef> mark_down_list;
    CON_TO_VCON_MAP::iterator it = con_to_vcon_map.find(dst_addr);
    if (it != con_to_vcon_map.end())
    {
        std::set<uint64_t> findSet = it->second;
        for(std::set<uint64_t>::iterator tor = findSet.begin(); tor!= findSet.end(); ++tor)
        {
            SEQ_MAP::iterator iter = seqMap.find(*tor);
            if(iter != seqMap.end())
            {
                virtual_connection* vcon = iter->second.get();
                if(vcon->con)
                {
                    ldout(cct,10) << "mark down vcon:" << vcon << " con in vcon is " << vcon->con << dendl;
                    mark_down_list.push_back(iter->second);
                 }
                 delete_con_from_sharecon_lite(vcon);
                 seqMap.erase(iter);
                 //ldout(cct, 10) << this << "  addr: " << vcon->get_peer_addr() << " vcon " << vcon <<  "  src_id:  " << *tor << " con "  <<vcon->con << dendl;
            }
        }
        con_to_vcon_map.erase(it);
    }
    lock.put_write();
	
    list<Vri_ConnectionRef>::iterator iter = mark_down_list.begin();
    for(;iter != mark_down_list.end(); iter++)
    {
         Vri_ConnectionRef vcon = *iter; 
         //ms_handle_reset_vcon(vcon.get());
         in_vcon_que(vcon);
    }

    msgr_mark_down(messgr, dst_addr);

}

void virtual_messenger::mark_down(const entity_addr_t& dst_addr, entity_inst_t src_osd, entity_inst_t dest_osd)
{
    ldout(cct,1) << "mark_down  dst addr "<< dst_addr << " src:" << src_osd << " dest:" << dest_osd << dendl;
    Vri_ConnectionRef vcon = NULL;

    vcon = find_virtual_connection(src_osd, dest_osd);

    if(vcon && VCON_MARK_DOWNING != vcon->con_state)
    {
        ldout(cct,10) << "send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
        vcon->con_state = VCON_MARK_DOWNING;
        vcon->clear_delay_list_msg();
        send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon.get(), vcon->src, vcon->dst);
        //ms_handle_reset_vcon(vcon.get());
        in_vcon_que(vcon);
    }
	
	return;
}


void virtual_messenger::mark_down_all()
{
    ldout(cct,1) << "mark_down  all "<< dendl;
    NUM_MAP::iterator p;
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    list<Vri_ConnectionRef> reset_vcon_list;

    lock.get_write();
    for(p = share_con.begin(); p != share_con.end(); p++)
    {
        for(q = p->second.begin(); q!= p->second.end();)
        {
            Vri_ConnectionRef vcon = q->second;
            ldout(cct,10)<< "begin to mark down vcon " << vcon <<" con" <<vcon->con <<dendl;
            vcon->clear_delay_list_msg();
            if(vcon->con)
            {
            	reset_vcon_list.push_back(vcon);
            	delete_vcon_from_map(vcon);
            }
            q = p->second.erase(q);	
            delete_vcon_from_seqMap(vcon);        
        }
    }
    lock.put_write();

    list<Vri_ConnectionRef>::iterator iter = reset_vcon_list.begin();
    for(;iter != reset_vcon_list.end(); iter++)
    {
        Vri_ConnectionRef vcon = *iter;  
        //ms_handle_reset_vcon(vcon.get());
        in_vcon_que(vcon);
    }
    messgr->mark_down_all();
}

void virtual_messenger::mark_down_all(int osd_id)
{
    ldout(cct,1) << "mark_down  all osd id is "<< osd_id << dendl;
    NUM_MAP::iterator p;
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    list<Vri_ConnectionRef> remove_vcon_list;
	
    lock.get_read();
    for(p = share_con.begin(); p != share_con.end();p++)
    {
        if(p->first != osd_id)
        {
            continue;
        }
        for(q = p->second.begin(); q != p->second.end();)
        {
            Vri_ConnectionRef vcon = q->second;
            
            if(vcon->con)
            {
                ldout(cct,10) << "need send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
                remove_vcon_list.push_back(vcon);
            }
            vcon->clear_delay_list_msg();
            q++;
        }
    }
    lock.put_read();
	
    list<Vri_ConnectionRef>::iterator piter = remove_vcon_list.begin();
    for(;piter != remove_vcon_list.end(); piter++)
    {
        Vri_ConnectionRef vcon = *piter;      
        if(vcon->con && VCON_MARK_DOWNING != vcon->con_state)
        {
            vcon->con_state = VCON_MARK_DOWNING;
            ldout(cct,10) << "send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
            send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon.get(), vcon->src, vcon->dst);
            //ms_handle_reset_vcon(vcon.get());
            in_vcon_que(vcon);
        }
    }
}


void virtual_messenger::mark_down_all_delete_dispatch(int osd_id)
{
    ldout(cct,1) << "mark_down  all osd id is "<< osd_id << dendl;
    NUM_MAP::iterator p;
    ceph::unordered_map<entity_inst_t, Vri_ConnectionRef>::iterator q;
    list<Vri_ConnectionRef> remove_vcon_list;
	
    lock.get_write();
    for(p = share_con.begin(); p != share_con.end();p++)
    {
        if(p->first != osd_id)
        {
            continue;
        }
        for(q = p->second.begin(); q != p->second.end();)
        {
            Vri_ConnectionRef vcon = q->second;
            vcon->con_state = VCON_OSD_STOP;
            if(vcon->con)
            {
                ldout(cct,10) << "need send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
                remove_vcon_list.push_back(vcon);
                delete_vcon_from_map(vcon);
            }
            vcon->clear_delay_list_msg();
            q = p->second.erase(q);	
            delete_vcon_from_seqMap(vcon);
        }
    }
    lock.put_write();
	
    list<Vri_ConnectionRef>::iterator piter = remove_vcon_list.begin();
    for(;piter != remove_vcon_list.end(); piter++)
    {
        Vri_ConnectionRef vcon = *piter;      
        ldout(cct,10) << "send remove con msg  src is : "<< vcon->dst << " dst is: "<< vcon->src << dendl;
        if(vcon->con)
        {
            send_remove_vcon_message(MSG_VCON_NO_EXISTENT, vcon.get(), vcon->src, vcon->dst);
            //ms_handle_reset_vcon(vcon.get());
            in_vcon_que(vcon);
        }
    }
}


Vri_ConnectionRef virtual_messenger::find_virtual_connection(const entity_inst_t  &src, const entity_inst_t  &dst)
{	 
    lock.get_read();
    Vri_ConnectionRef find_con = NULL;
    if(!src.addr.is_ip() || src.addr.is_blank_ip())
    {
        find_con = find_vcon_from_sharecon(src, dst,con_bak);
    }
    else
    {
        find_con = find_vcon_from_sharecon(src, dst,share_con);
    }

    ldout(cct, 10)<< "find virtual_connection:"<< find_con << "  from  share_con return it" <<  dendl;
    lock.put_read();
    return find_con;
}

int virtual_messenger::send_message(Message *m, const entity_inst_t& dst)
{
    entity_inst_t src = get_myinst();
    Vri_ConnectionRef find_con = NULL;
    virtual_connection *vcon = NULL;
    ConnectionRef con = NULL;
    if(get_policy(dst.name.type()).server)
    {
        find_con = find_virtual_connection(src,dst);
        if(!find_con)
        {
            ldout(cct,0) <<"drop msg"<< " from: "<< src << " to: "<< dst << hex << " msg id: "<< m->get_msg_seq()<<dendl;
            m->put();
            return false;
        }
    }

    con = get_connection(src, dst);
    vcon = static_cast<virtual_connection *>(con.get());
    return vcon->send_message(m);

}

int virtual_messenger::send_message(Message *m, const entity_inst_t& src, const entity_inst_t& dst)
{
    virtual_connection *vcon = NULL;
    Vri_ConnectionRef find_con = NULL;
    ConnectionRef con = NULL;
    if(get_policy(dst.name.type()).server)
    {
        find_con = find_virtual_connection(src,dst);
        if(!find_con)
        {
            ldout(cct,0) <<"drop msg"<< " from: "<< src << " to: "<< dst << hex << " msg id: "<< m->get_msg_seq()<<dendl;
            m->put();
            return false;
        }
    }

    con = get_connection(src, dst);
    vcon = static_cast<virtual_connection *>(con.get());
    return vcon->send_message(m);
}


virtual_messenger::~virtual_messenger()
{
    vmessenger_state = V_MESSENGER_STOP;
    stop_async_reset = true;
    while(true)
    {
        if(dispatch_lock.is_locked()  || lock.is_locked())
        {
           usleep(1000); 
        } 
        break;
    }
    clear_reset_vcon_que();
    delete messgr;
}

int virtual_messenger::shutdown()
{
    vmessenger_state = V_MESSENGER_STOP;
    stop_async_reset = true;
    while(true)
    {
        if(dispatch_lock.is_locked()  || lock.is_locked())
        {
            usleep(1000);
        }
        break;
    }
    clear_reset_vcon_que();
    messgr->shutdown();
    return 0;
}

void virtual_messenger::show_status(ceph::Formatter *f)
{
    f->open_object_section("virtual");
    f->dump_string("messenger name ", messenger_name);
    f->dump_int("fast preprocess handle max", dfx_status->fast_preprocess_handle_max);
    f->dump_int("fast preprocess dispatcher handle max", dfx_status->fast_preprocess_dispatcher_handle_max);
    f->dump_int("fast dispatch handle max ", dfx_status->fast_dispatch_handle_max);
    f->dump_int("fast dispatch dispatcher handle max", dfx_status->fast_dispatch_dispatcher_handle_max);
    f->dump_int("ms dispatch handle max", dfx_status->ms_dispatch_handle_max);
    f->dump_int("ms dispatch dispatcher handle max", dfx_status->ms_dispatch_dispatcher_handle_max);
    
    if(dfx_status->fast_preprocess_dispatcher_handle_cnt.read() > 0)
    {
        f->dump_float("fast preprocess handle avg", dfx_status->fast_preprocess_handle_sum.read()/dfx_status->fast_preprocess_handle_cnt.read());
        f->dump_float("fast preprocess dispatch handle avg", dfx_status->fast_dispatch_dispatcher_handle_sum.read()/dfx_status->fast_preprocess_dispatcher_handle_cnt.read());
    }
    if(dfx_status->fast_dispatch_dispatcher_handle_cnt.read() > 0)
    {
         f->dump_float("fast dispatch handle avg", dfx_status->fast_dispatch_handle_sum.read()/dfx_status->fast_dispatch_handle_cnt.read());
         f->dump_float("fast dispatch dispatcher handle avg", dfx_status->fast_dispatch_dispatcher_handle_sum.read()/dfx_status->fast_dispatch_dispatcher_handle_cnt.read());
    }
    if(dfx_status->ms_dispatch_dispatcher_handle_cnt.read() > 0)
    {
         f->dump_float("ms dispatch handle avg", dfx_status->ms_dispatch_handle_sum.read()/dfx_status->ms_dispatch_handle_cnt.read());
         f->dump_float("ms dispatch dispatcher handle avg", dfx_status->ms_dispatch_dispatcher_handle_sum.read()/dfx_status->ms_dispatch_dispatcher_handle_cnt.read());
    }

    f->close_section();
}

void virtual_messenger::show_msg_status(ceph::Formatter *f)
{
    dfx_status->type_lock.Lock();
    for(list<dispatch_timeout_detail *>::iterator p = dfx_status->msg_type_list.begin(); p!= dfx_status->msg_type_list.end(); p++)
    {
        dispatch_timeout_detail *tmp = *p;
        f->open_object_section("virtual msg");
        f->dump_string("messenger name ", messenger_name);
        f->dump_int("msg handle time (ms) ", tmp->handle_msg_time);
        f->dump_string("timeout msg detial: ", tmp->msg_detai.str());
	
        stringstream m_time;
        m_time<< tmp->recv_stamp;
        f->dump_string("msg begin recv: ", m_time.str());
        
        m_time.str("");
        m_time<<tmp->throttle_stamp;
        f->dump_string("msg throttle: ", m_time.str());
        
        m_time.str("");
        m_time<< tmp->recv_complete_stamp;
        f->dump_string("msg recv end: ", m_time.str());
        
        m_time.str("");
        m_time<< tmp->preprocess_end_stamp;
        f->dump_string("msg preprocess end: ", m_time.str());
        
        m_time.str("");
        m_time<< tmp->fast_dispatcher_begin_stamp;
        f->dump_string("msg fast dispatcher begin: ", m_time.str());
        
        m_time.str("");
        m_time<< tmp->fast_dispatch_end_stamp;
        f->dump_string("msg fast dispatch end: ", m_time.str());
        
        m_time.str("");
        m_time<< tmp->dispatcher_begin_stamp;
        f->dump_string("msg dispatcher begin: ", m_time.str());
        
        m_time.str("");
        m_time<< tmp->dispatch_end_stamp;
        f->dump_string("msg dispatch end: ", m_time.str());
        
        f->close_section();
    }
    dfx_status->type_lock.Unlock();
}

void virtual_messenger::clear_status(ceph::Formatter *f)
{
    dfx_status->fast_preprocess_handle_max = 0;
    dfx_status->fast_preprocess_dispatcher_handle_max = 0;
    dfx_status->fast_dispatch_handle_max = 0;
    dfx_status->fast_dispatch_dispatcher_handle_max = 0;
    dfx_status->ms_dispatch_handle_max = 0;
    dfx_status->ms_dispatch_dispatcher_handle_max = 0;
    dfx_status->fast_preprocess_handle_sum.set(0);
    dfx_status->fast_preprocess_handle_cnt.set(0);
    dfx_status->fast_preprocess_dispatcher_handle_sum.set(0);
    dfx_status->fast_preprocess_dispatcher_handle_cnt.set(0);
    dfx_status->fast_dispatch_handle_sum.set(0);
    dfx_status->fast_dispatch_handle_cnt.set(0);
    dfx_status->fast_dispatch_dispatcher_handle_sum.set(0);
    dfx_status->fast_dispatch_dispatcher_handle_cnt.set(0);
    dfx_status->ms_dispatch_handle_sum.set(0);
    dfx_status->ms_dispatch_handle_cnt.set(0);
    dfx_status->ms_dispatch_dispatcher_handle_sum.set(0);
    dfx_status->ms_dispatch_dispatcher_handle_cnt.set(0);
	
    dfx_status->type_lock.Lock();
    for(list<dispatch_timeout_detail *>::iterator p = dfx_status->msg_type_list.begin(); p!= dfx_status->msg_type_list.end(); p++)
    {
        delete *p;
    }
    dfx_status->msg_type_list.clear();
    dfx_status->type_lock.Unlock();

     f->open_object_section("virtual");
     f->dump_string("clear virtual status", " is OK");
     f->close_section();
}


dfx_vcon::~dfx_vcon()
{
    type_lock.Lock();
    for(list<dispatch_timeout_detail *>::iterator p = msg_type_list.begin(); p!= msg_type_list.end(); p++)
    {
        delete *p;
    }
    msg_type_list.clear();
    type_lock.Unlock();

}


void virtual_messenger::in_vcon_que(Vri_ConnectionRef vcon)
{
    ldout(cct,10)<< " in_vcon_que src: "<< vcon->src << " dst: "<< vcon->dst<<" vcon: "<< vcon<<dendl;
    async_reset_lock.Lock();
    reset_vcon_que.push(vcon);
    async_reset_cond.Signal();
    async_reset_lock.Unlock();
    return;
}

void virtual_messenger::run_delivery()
{
    async_reset_lock.Lock();
    while (true) {
        while(!reset_vcon_que.empty())
        {
            Vri_ConnectionRef vcon = reset_vcon_que.front();
            ldout(cct,10)<< " run_delivery src: "<< vcon->src << " dst: "<< vcon->dst<< " vcon: "<<vcon <<dendl;
            reset_vcon_que.pop();
            async_reset_lock.Unlock();
            ms_handle_reset_vcon(vcon.get());
            async_reset_lock.Lock();
        }

        if (stop_async_reset)
            break;
        async_reset_cond.Wait(async_reset_lock);
    }
    async_reset_lock.Unlock();
}

void virtual_messenger::vcon_start()
{
    assert(!stop_async_reset);
    assert(!async_reset_vcon_thread.is_started());
    //ringstream tmp;
    //p<<messenger_name<<": "<<"reset_vcon";
    async_reset_vcon_thread.create("reset_vcon");
}

void virtual_messenger::clear_reset_vcon_que()
{
    Mutex::Locker l(async_reset_lock);
    while(0 != reset_vcon_que.size())
    {
         reset_vcon_que.pop();
    }

    return;
}

uint64_t virtual_messenger::get_available_seqId(Vri_ConnectionRef vcon)
{
    uint64_t  src_id = vcon_seq.inc();
    seqMap[src_id] = vcon;
    return src_id;
}

void  virtual_messenger::insert_to_con_vcon_map(Vri_ConnectionRef  vcon)
{
    if(!vcon->con)
    {
        return;
    }
    if(!vcon->src.addr.is_ip() || vcon->src.addr.is_blank_ip())
    {
        return;
    }
    uint64_t seqId = vcon->src_id;
    CON_TO_VCON_MAP::iterator p = con_to_vcon_map.find(vcon->con->get_peer_addr());
    if (p == con_to_vcon_map.end())
    {
        std::set<uint64_t>  vconSet;
        vconSet.insert(seqId);
        con_to_vcon_map[vcon->con->get_peer_addr()] = vconSet;
        return;
    }
    std::set<uint64_t> &findSet = p->second;
    findSet.insert(seqId);
}

void  virtual_messenger::delete_vcon_from_map(Vri_ConnectionRef vcon)//(const entity_addr_t& addr, uint64_t seqId)
{
    CON_TO_VCON_MAP::iterator p = con_to_vcon_map.find(vcon->con->get_peer_addr());
    if (p == con_to_vcon_map.end())
    {
    	ldout(cct,1) << __func__<<" not find !" << dendl;
        return;
    }
    std::set<uint64_t> &findSet = p->second;
    std::set<uint64_t>::iterator q = findSet.find(vcon->src_id);
    if(q != findSet.end())
    {
        findSet.erase(q);
    }
}

void virtual_messenger::delete_vcon_from_seqMap(Vri_ConnectionRef vcon)
{
	SEQ_MAP::iterator it = seqMap.find(vcon->src_id);
    if(it != seqMap.end())
    {
    	seqMap.erase(it);
    }
}

