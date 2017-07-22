#include <errno.h>
#include <iostream>
#include <fstream>
#include "virtual_connection.h"
#include "virtual_messenger.h"

using namespace std;

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, virtual_connection *con) {
  return *_dout << "-( " << con->src << "  "<<con->dst<<" )- ";
}

void virtual_connection::mark_down_impl()
{
    virtual_messenger *messgr = static_cast<virtual_messenger *>(msgr);
    messgr->mark_down(this);
}


int virtual_connection::send_delay_list_msg()
{
    //virtual_messenger *messgr = static_cast<virtual_messenger *>(msgr);
    lock.Lock();
    ldout(cct,10)<< "list size is:" << delay_msg_list.size() <<dendl;
   // list<MessageRef>::iterator p = delay_msg_list.begin();
    while(0 != delay_msg_list.size())
    {
         MessageRef msg = delay_msg_list.front();
         delay_msg_list.pop();
         lock.Unlock();
         msg->set_header_src(get_ceph_inst(src));
         msg->set_header_dst(get_ceph_inst(dst));
         msg->set_vcon_seq(src_id);
         //msg->set_msg_seq(messgr->create_msg_id(this));
         ldout(cct,30)<< "send msg id: "<< hex << msg->get_msg_seq() << dendl;
         ldout(cct,1)<< " send msg from "<< src <<" to " << dst << " msg type: " << msg->get_type_name() << 
		 	" vcon seq: "<< src_id<< " msg id is "<< hex << msg->get_msg_seq() <<dendl;        
         con->send_message(msg.get());
         lock.Lock();
    }
    //delay_msg_list.clear();
    lock.Unlock();
    return 0;
}

void virtual_connection::clear_delay_list_msg()
{
    lock.Lock();
    //list<MessageRef>::iterator p = delay_msg_list.begin();
    while(0 != delay_msg_list.size())
    {
         MessageRef msg = delay_msg_list.front();
         delay_msg_list.pop();
         ldout(cct,0)<< "drop msg clear_delay_list_msg "<< src <<" to " << dst << " msg type: " << msg->get_type_name() << 
		 	" vcon seq: "<< src_id<< " msg id is "<< hex << msg->get_msg_seq() <<dendl; 
    }
    ldout(cct,10)<< "clear list msg list size is:" << delay_msg_list.size() <<dendl;
   // delay_msg_list.clear();
    lock.Unlock();
    return;
}

int virtual_connection::send_message(Message *m)
{
    virtual_messenger *messgr = static_cast<virtual_messenger *>(msgr);
    if(VCON_CONNECTING == con_state)
    {
        MessageRef msg = m;
        m->set_msg_seq(messgr->create_msg_id(this));
        ldout(cct,1) << "send_delay_list_msg push msg: "<< m << " type: "<< m->get_type_name() << 
			" msg id: "<< hex <<m->get_msg_seq() <<dendl;
        lock.Lock();		
        delay_msg_list.push(msg);
        lock.Unlock();
        if(VCON_CONNECTED == con_state)
        {
            send_delay_list_msg();
        }
    }
    else if(VCON_CONNECTED == con_state)
    { 
        if(0 != delay_msg_list.size())
        {
            send_delay_list_msg();
        }
        m->set_header_src(get_ceph_inst(src));
        m->set_header_dst(get_ceph_inst(dst));
        m->set_vcon_seq(src_id);
        m->set_msg_seq(messgr->create_msg_id(this));
        ldout(cct,30)<< "send msg id: "<< hex << m->get_msg_seq() << dendl;
        ldout(cct,1)<< " send msg from "<< src <<" to " << dst << " msg type: " << m->get_type_name() << 
            " vcon seq: "<< src_id<< " msg id is "<< hex << m->get_msg_seq() <<dendl;   
        return con->send_message(m);
    }
    else if(VCON_MARK_DOWNING == con_state)
    {
#if 0
        MessageRef msg = m;
        m->set_msg_seq(messgr->create_msg_id(this));
        ldout(cct,1) << "send_delay_list_msg push msg: "<< m << " type: "<< m->get_type_name() << 
			" msg id: "<< hex <<m->get_msg_seq() <<dendl;
        lock.Lock();
        delay_msg_list.push_back(msg);
        lock.Unlock();
	 con_state = VCON_CONNECTING;
	 messgr->send_vconnect_message(MSG_CONNECT, this, src, dst); 
#endif
	 ConnectionRef con = NULL;
	 con = messgr->get_connection(src,dst);
	 con->send_message(m);
    }
    return 0;
}
