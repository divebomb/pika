CXX = g++

ifeq ($(__REL), 1)
	CXXFLAGS = -O2 -g -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith \
	         -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter \
			 -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -W -DDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch \
	            -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE \
				-D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls
endif

OBJECT = pika_port
SRC_DIR = .
THIRD_PATH = ../../third

include ../../make_config.mk
INCLUDE_PATH = -I../.. \
			   -I../../include \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/slash \
			   -I$(THIRD_PATH)/pink \
			   -I$(THIRD_PATH)/nemo/include/ \
			   -I$(THIRD_PATH)/nemo-rocksdb/include/ \
			   -I$(THIRD_PATH)/rocksdb/include/

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/nemo-rocksdb/lib \
		   -L$(THIRD_PATH)/rocksdb/ \
		   -L$(THIRD_PATH)/slash/slash/lib \
		   -L$(THIRD_PATH)/pink/pink/lib \
		   -L$(THIRD_PATH)/glog/.libs/ \
           -L$(THIRD_PATH)/nemo/lib/

LIBS = -Wl,-Bstatic -lnemo -lrocksdb\
			 -Wl,-Bdynamic -lpthread\
			 -lrt \
	         -lglog \
			 -lpink \
			 -lslash \
			 -lnemodb \
			 -lrocksdb
LIBS += $(ROCKSDB_LDFLAGS)

GLOG = $(THIRD_PATH)/glog/.libs/libglog.so.0
PINK = $(THIRD_PATH)/pink/pink/lib/libpink.a
SLASH = $(THIRD_PATH)/slash/slash/lib/libslash.a

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(GLOG) $(PINK) $(SLASH) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH)  $(LFLAGS) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OBJECT)

