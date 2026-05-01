CXX ?= g++
ROCKSDB_ROOT ?= ..

CXXFLAGS ?= -O2 -g -std=c++20
CXXFLAGS += -I$(ROCKSDB_ROOT) -I$(ROCKSDB_ROOT)/include -pthread
CXXFLAGS += -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
LDFLAGS ?=
ROCKSDB_LIB := $(ROCKSDB_ROOT)/librocksdb.a
LDLIBS ?= $(ROCKSDB_LIB) -lrdmacm -libverbs -lpthread -ldl -lz -lbz2 -llz4 -lzstd -lsnappy -luring

SRCS = \
	bloom_evaluator.cc \
	rdma_server.cc \
	server_main.cc \
	thread_pool.cc

OBJS = $(SRCS:.cc=.o)
TARGET = pruneoff_rdma_server

.PHONY: all clean rocksdb

all: $(TARGET)

rocksdb:
	$(MAKE) -C $(ROCKSDB_ROOT) static_lib

$(ROCKSDB_LIB):
	$(MAKE) -C $(ROCKSDB_ROOT) static_lib

$(TARGET): $(OBJS) $(ROCKSDB_LIB)
	$(CXX) $(LDFLAGS) -o $@ $(OBJS) $(LDLIBS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(TARGET)
