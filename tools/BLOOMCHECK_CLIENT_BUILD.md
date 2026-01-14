# BloomCheck Client 빌드 가이드 (컴퓨트 노드)

컴퓨트 노드에서 `bloomcheck_client`를 빌드하는 방법입니다.

## 필수 의존성 설치

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
    g++-11 g++-12 g++-13 \
    libgrpc++-dev \
    protobuf-compiler \
    protobuf-compiler-grpc \
    libprotobuf-dev \
    make

# g++ 버전 확인 (g++-11 이상 필요)
g++-11 --version  # 또는 g++-12, g++-13
```

## 빌드 방법

### 옵션 1: 스토리지 노드와 동일한 RocksDB 소스에서 빌드 (권장)

스토리지 노드와 동일한 RocksDB 소스 코드가 있다면:

```bash
cd /path/to/rocksdb

# protobuf 파일 생성
protoc --cpp_out=tools --proto_path=tools tools/bloomcheck.proto
protoc --grpc_out=tools --cpp_out=tools --proto_path=tools \
  --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) tools/bloomcheck.proto

# 클라이언트만 빌드 (RocksDB 라이브러리 필요 없음)
g++-11 -std=c++20 -O2 -Wall \
  -I. -Itools \
  tools/bloomcheck_client.cc \
  tools/bloomcheck.pb.cc \
  tools/bloomcheck.grpc.pb.cc \
  -lgrpc++ -lprotobuf -lgpr -labsl_synchronization -pthread \
  -o bloomcheck_client
```

### 옵션 2: 최소 파일만 복사하여 빌드

RocksDB 전체 소스를 복사하지 않고 필요한 파일만 사용:

```bash
# 작업 디렉토리 생성
mkdir -p ~/bloomcheck_client_build
cd ~/bloomcheck_client_build

# 스토리지 노드에서 필요한 파일만 복사
# (scp, rsync 등 사용)
scp user@storage-node:/path/to/rocksdb/tools/bloomcheck.proto .
scp user@storage-node:/path/to/rocksdb/tools/bloomcheck_client.cc .

# protobuf 파일 생성
protoc --cpp_out=. --proto_path=. bloomcheck.proto
protoc --grpc_out=. --cpp_out=. --proto_path=. \
  --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) bloomcheck.proto

# 빌드
g++-11 -std=c++20 -O2 -Wall \
  -I. \
  bloomcheck_client.cc \
  bloomcheck.pb.cc \
  bloomcheck.grpc.pb.cc \
  -lgrpc++ -lprotobuf -lgpr -labsl_synchronization -pthread \
  -o bloomcheck_client
```

### 옵션 3: Makefile 사용 (전체 RocksDB 소스가 있는 경우)

```bash
cd /path/to/rocksdb
make bloomcheck_client
```

## 빌드 확인

```bash
./bloomcheck_client -h
```

## 사용 방법

```bash
# 스토리지 노드의 서버로 요청
./bloomcheck_client -s <storage_node_ip>:50051 -k "your_key" -f /path/to/file.sst

# 헥스 키 사용 (sst_dump 출력)
./bloomcheck_client -s <storage_node_ip>:50051 \
  -x 000000002BAF737B30303030303030303030303030303030303030303030303030303030303030303030303030303030 \
  -f /mnt/kvafa/db_bench/021903.sst
```

## 문제 해결

### gRPC 플러그인을 찾을 수 없음
```bash
which grpc_cpp_plugin
# 없다면
sudo apt-get install protobuf-compiler-grpc
```

### C++20 지원 문제
```bash
# g++-11 이상 필요
g++-11 --version
# 없다면
sudo apt-get install g++-11
```

### 라이브러리 링크 오류
```bash
# 필요한 라이브러리 확인
ldconfig -p | grep grpc
ldconfig -p | grep protobuf
```

