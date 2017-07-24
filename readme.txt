
class virtual_messenger in file src/msg/virtual_messenger.cc src/msg/virtual_messenger.h
class virtual_connection in file src/msg/virtual_connection.cc src/msg/virtual_connection.h
class OSDManager in file src/osd/OSDManager.cc src/osd/OSDManager.h
class OSDInstance in file src/osd/OSDInstance.cc src/osd/OSDInstance.h
class OSDPseudo in file src/osd/OSDPseudo.cc src/osd/OSDPseudo.h


Class virtual_messenger,class virtual_connection,Class OSDManager,class OSDInstance is the Core of VMsg.
Class OSDPseudo is the code of virtual osd.
The virtual_messenger is public class of Messenger and Dispatcher.The pointer of Messenger who named messgr is pointer to real Messenger.
The real Messenger is SimpleMessenger of AsyncMessenger.Con_bak is the member of virtual_messenger.The con_bak's type is ceph::unordered_map<entity_inst_t , ceph::unordered_map<entity_inst_t , Vri_ConnectionRef> >.
We can find the vconn from con_bak.Because vconn.first is source addr,vconn.second is a hash map who is <dest addr,vconn>.
If osd send message,osd should get conn from virtual_message by get_connection(src,dst).
if real conn recv one msg,the real conn can call the dispatcher who is the virtual_messager.

When the ceph-osd start without id,then the osd is Management mode.In this mode,the Class OSDManager can new one AdminSocket who named osd.admin.The AdminSocket can recv command,such as start,stop,staus...
The Start command can new OSD.This OSD can manager one disk.
The Stop command can stop one OSD's thread,then delete the OSD.
The Status command can get the run status of one OSD.
When the ceph-osd start with id,the osd is virtual mode.In this mode,the process can't Manager everything.The ceph-osd may send Command to ManagerOSD by the AdminSocket who named osd.admin.
The process of ManagerOSD can manage ervery disk of one host.

If any error occurred such as disk read error,disk write error,assert...,the process of ManagerOSD must delete one OSD.

Signed-off-by: Ying He <heyingbj@inspur.com>