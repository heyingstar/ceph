libosd_types_la_SOURCES = \
	osd/PGLog.cc \
	osd/osd_types.cc \
	osd/ECUtil.cc
libosd_types_la_CXXFLAGS = ${AM_CXXFLAGS}
noinst_LTLIBRARIES += libosd_types.la

if ENABLE_SERVER
if WITH_OSD

libosd_a_SOURCES = \
	osd/PG.cc \
	osd/ReplicatedPG.cc \
	osd/ReplicatedBackend.cc \
	osd/ECBackend.cc \
	osd/ECMsgTypes.cc \
	osd/ECTransaction.cc \
	osd/PGBackend.cc \
	osd/HitSet.cc \
	osd/OSD.cc \
	osd/OSDCap.cc \
	osd/Watch.cc \
	osd/ClassHandler.cc \
	osd/OpRequest.cc \
	osd/SnapMapper.cc \
	osd/ScrubStore.cc \
	objclass/class_api.cc \
	osd/ErasureCodePG.cc \
	osd/DirectECProcess.cc \
	osd/NVRAMECProcess.cc \
	osd/OSDMessengers.cc \
	osd/OSDInstance.cc \
	osd/OSDManager.cc \
	osd/OSDPseudo.cc

libosd_a_CXXFLAGS = ${AM_CXXFLAGS}
libosd_a_LIBADD =
noinst_LIBRARIES += libosd.a

noinst_HEADERS += \
	osd/ClassHandler.h \
	osd/HitSet.h \
	osd/OSD.h \
	osd/OSDCap.h \
	osd/OSDMap.h \
	osd/ObjectVersioner.h \
	osd/OpRequest.h \
	osd/SnapMapper.h \
	osd/PG.h \
	osd/PGLog.h \
	osd/ReplicatedPG.h \
	osd/PGBackend.h \
	osd/ReplicatedBackend.h \
	osd/TierAgentState.h \
	osd/ECBackend.h \
	osd/ECUtil.h \
	osd/ECMsgTypes.h \
	osd/ECTransaction.h \
	osd/Watch.h \
	osd/ScrubStore.h \
	osd/osd_types.h \
	osd/ErasureCodePG.h \
	osd/DirectECProcess.h \
	osd/NVRAMECProcess.h \
	osd/OSDMessengers.h \
	osd/OSDInstance.h \
	osd/OSDManager.h \
	osd/OSDPseudo.h

endif # WITH_OSD
endif # ENABLE_SERVER
