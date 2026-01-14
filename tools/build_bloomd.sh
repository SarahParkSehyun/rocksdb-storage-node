#!/bin/bash
# bloomd 빌드 스크립트 (간단한 방법)

set -e

cd "$(dirname "$0")/.."

echo "=== Building bloomd ==="

# 1. Protobuf 파일 생성
echo "1. Generating protobuf files..."
protoc --cpp_out=tools --proto_path=tools tools/bloomcheck.proto
protoc --grpc_out=tools --cpp_out=tools --proto_path=tools \
  --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) tools/bloomcheck.proto

# 2. bloomd.o 컴파일
echo "2. Compiling bloomd.o..."
mkdir -p ./tools
CXX=$(which g++-13 g++-12 g++-11 g++ 2>/dev/null | head -1)
$CXX -std=c++20 -DROCKSDB_PLATFORM_POSIX -DOS_LINUX -I. -I./include -Itools -c tools/bloomd.cc -o ./tools/bloomd.o

# 3. bloomd 링크
echo "3. Linking bloomd..."
# RocksDB 라이브러리가 있으면 사용, 없으면 gRPC/protobuf만 링크
LIB_OPTION=""
if ls librocksdb*.a 2>/dev/null | head -1 | grep -q .; then
    LIB_FILE=$(ls librocksdb*.a 2>/dev/null | head -1)
    LIB_OPTION="$LIB_FILE"
elif ls librocksdb*.so 2>/dev/null | head -1 | grep -q .; then
    LIB_OPTION="-L. -lrocksdb"
else
    echo "Warning: RocksDB library not found. Building without it..."
fi

$CXX -std=c++20 -I. -Itools \
  ./tools/bloomd.o \
  tools/bloomcheck.pb.cc tools/bloomcheck.grpc.pb.cc \
  $LIB_OPTION \
  -Wl,--no-as-needed -lgrpc++ -lgrpc++_reflection -lprotobuf -lgpr -labsl_synchronization -labsl_strings -lz -llz4 -lzstd -lbz2 -lsnappy -luring -pthread \
  -o bloomd

echo "=== Done! bloomd built successfully ==="

