# BloomCheck 빌드 가이드

## 문제 해결: grpc_cpp_plugin을 찾을 수 없음

`grpc_cpp_plugin`을 찾을 수 없다는 에러가 발생하는 경우, 다음을 확인하세요:

### 1. gRPC 설치 확인

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
  libgrpc++-dev \
  libprotobuf-dev \
  protobuf-compiler \
  protobuf-compiler-grpc

# 설치 확인
which grpc_cpp_plugin
protoc --version
```

### 2. 플러그인 경로 수동 지정

플러그인이 설치되어 있지만 PATH에 없는 경우:

```bash
# 플러그인 위치 찾기
find /usr -name grpc_cpp_plugin 2>/dev/null

# Makefile에서 사용
make bloomd GRPC_CPP_PLUGIN=/usr/bin/grpc_cpp_plugin
```

### 3. 소스에서 빌드 (선택사항)

패키지 매니저로 설치가 안 되는 경우, 소스에서 빌드:

```bash
# gRPC 소스 빌드 (시간이 오래 걸릴 수 있음)
git clone --recurse-submodules -b v1.60.0 --depth 1 https://github.com/grpc/grpc
cd grpc
mkdir -p cmake/build
cd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      ../..
make -j$(nproc)
sudo make install
```

### 4. Protobuf 파일 수동 생성

Makefile 없이 직접 생성:

```bash
cd /path/to/rocksdb

# 1. 기본 protobuf 파일 생성
protoc --cpp_out=tools --proto_path=tools tools/bloomcheck.proto

# 2. gRPC 파일 생성 (플러그인 경로 지정)
GRPC_PLUGIN=$(find /usr -name grpc_cpp_plugin 2>/dev/null | head -1)
if [ -n "$GRPC_PLUGIN" ]; then
  protoc --grpc_out=tools --cpp_out=tools --proto_path=tools \
    --plugin=protoc-gen-grpc=$GRPC_PLUGIN tools/bloomcheck.proto
else
  echo "Error: grpc_cpp_plugin not found"
  exit 1
fi
```

### 5. 빌드

```bash
# Protobuf 파일 생성 후 빌드
make bloomd bloomcheck_client
```

## 대안: Protobuf만 사용 (gRPC 없이)

gRPC 설치가 어려운 경우, 프로토타입 단계에서는 protobuf만 사용하는 간단한 버전을 만들 수도 있습니다. 하지만 현재 구현은 gRPC를 필요로 합니다.

## 확인 사항

빌드 전에 다음이 설치되어 있는지 확인:

- [ ] `protoc` (protobuf-compiler)
- [ ] `grpc_cpp_plugin` (protobuf-compiler-grpc)
- [ ] `libgrpc++-dev`
- [ ] `libprotobuf-dev`

```bash
# 한 번에 확인
which protoc && which grpc_cpp_plugin && \
  pkg-config --exists protobuf && pkg-config --exists grpc++ && \
  echo "All dependencies found!" || echo "Some dependencies missing"
```

