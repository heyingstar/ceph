libmsg_la_SOURCES = \
	msg/Message.cc \
	msg/Messenger.cc \
	msg/msg_types.cc \
	msg/DispatchQueue.cc \
	msg/virtual_messenger.cc \
	msg/virtual_connection.cc

noinst_HEADERS += \
	msg/Connection.h \
	msg/Dispatcher.h \
	msg/DispatchQueue.h \
	msg/Message.h \
	msg/Messenger.h \
	msg/SimplePolicyMessenger.h \
	msg/virtual_messenger.h \
	msg/virtual_connection.h \
	msg/msg_types.h

libmsg_la_SOURCES += \
	msg/simple/Accepter.cc \
	msg/simple/Pipe.cc \
	msg/simple/PipeConnection.cc \
	msg/simple/SimpleMessenger.cc \
	msg/async/Stack.cc \
	msg/async/AsyncConnection.cc \
	msg/async/AsyncMessenger.cc \
	msg/async/Event.cc \
	msg/async/PosixStack.cc \
	msg/async/net_handler.cc \
	msg/async/EventSelect.cc

if LINUX
libmsg_la_SOURCES += msg/async/EventEpoll.cc
endif

if DARWIN
libmsg_la_SOURCES += msg/async/EventKqueue.cc
endif

if FREEBSD
libmsg_la_SOURCES += msg/async/EventKqueue.cc
endif


noinst_HEADERS += \
	msg/simple/Accepter.h \
	msg/simple/Pipe.h \
	msg/simple/PipeConnection.h \
	msg/simple/SimpleMessenger.h \
	msg/async/Stack.h \
	msg/async/AsyncConnection.h \
	msg/async/AsyncMessenger.h \
	msg/async/Event.h \
	msg/async/PosixStack.h \
	msg/async/EventEpoll.h \
	msg/async/EventSelect.h \
	msg/async/net_handler.h

if LINUX
libmsg_la_SOURCES += msg/async/EventEpoll.h
endif

if DARWIN
libmsg_la_SOURCES += msg/async/EventKqueue.h
endif

if FREEBSD
libmsg_la_SOURCES += msg/async/EventKqueue.h
endif

if ENABLE_RDMA
libmsg_la_SOURCES += \
	msg/async/rdma/Device.cc \
	msg/async/rdma/Infiniband.cc \
	msg/async/rdma/RDMAConnTCP.cc \
	msg/async/rdma/RDMAConnectedSocketImpl.cc \
	msg/async/rdma/RDMAServerSocketImpl.cc \
	msg/async/rdma/RDMAStack.cc

noinst_HEADERS += \
	msg/async/rdma/Device.h \
	msg/async/rdma/Infiniband.h \
	msg/async/rdma/RDMAConnectedSocketImpl.h \
	msg/async/rdma/RDMAConnTCP.h \
	msg/async/rdma/RDMAStack.h
endif

if ENABLE_XIO
libmsg_la_SOURCES += \
	msg/xio/QueueStrategy.cc \
	msg/xio/XioConnection.cc \
	msg/xio/XioMessenger.cc \
	msg/xio/XioMsg.cc \
	msg/xio/XioPortal.cc \
	msg/xio/XioPool.cc

noinst_HEADERS += \
	msg/xio/DispatchStrategy.h \
	msg/xio/FastStrategy.h \
	msg/xio/QueueStrategy.h \
	msg/xio/XioConnection.h \
	msg/xio/XioInSeq.h \
	msg/xio/XioMessenger.h \
	msg/xio/XioMsg.h \
	msg/xio/XioPool.h \
	msg/xio/XioPortal.h \
	msg/xio/XioSubmit.h
endif

noinst_LTLIBRARIES += libmsg.la
