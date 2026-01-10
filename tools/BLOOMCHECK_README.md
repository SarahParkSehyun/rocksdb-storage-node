# BloomCheck Service

RocksDB의 Bloom filter만 storage node로 오프로딩하기 위한 gRPC 기반 서비스입니다.

## 개요

이 서비스는 RocksDB SST 파일의 partitioned bloom filter만 체크하여, key가 특정 SST 파일에 존재할 가능성이 있는지 확인합니다. Index나 data 블록은 읽지 않고 filter만 검사하므로 빠르고 효율적입니다.

## 요구사항

- RocksDB 10.11.0
- gRPC 및 protobuf 라이브러리
- C++20 컴파일러

### 의존성 설치 (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y \
  libgrpc++-dev \
  libprotobuf-dev \
  protobuf-compiler \
  protobuf-compiler-grpc
```

## 빌드

### 1. Protobuf 파일 생성

```bash
cd /path/to/rocksdb
protoc --cpp_out=tools --proto_path=tools tools/bloomcheck.proto
protoc --grpc_out=tools --cpp_out=tools --proto_path=tools \
  --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` tools/bloomcheck.proto
```

### 2. 서버 및 클라이언트 빌드

```bash
make bloomd bloomcheck_client
```

또는 개별적으로:

```bash
make bloomd          # 서버만 빌드
make bloomcheck_client  # 클라이언트만 빌드
```

## 사용법

### 서버 실행

```bash
./bloomd [server_address] [cache_size]

# 예시:
./bloomd 0.0.0.0:50051 100
```

- `server_address`: 서버 주소 (기본값: `0.0.0.0:50051`)
- `cache_size`: 캐시할 최대 SST 파일 수 (기본값: 100)

### 클라이언트 사용

#### 단일 key 체크

```bash
./bloomcheck_client -s localhost:50051 -k "user_key" -f "/path/to/sst/file.sst"
```

#### 성능 테스트

```bash
./bloomcheck_client -s localhost:50051 -t
```

## API

### 요청 형식

```protobuf
message KeyRequest {
  bytes user_key = 1;           // User key (바이너리 가능)
  repeated string sst_paths = 2; // Candidate SST 파일 경로들
}

message BatchRequest {
  repeated KeyRequest reqs = 1;
}
```

### 응답 형식

```protobuf
message KeyResponse {
  repeated string may_match_sst_paths = 1; // may_match=true인 SST 경로들
}

message BatchResponse {
  repeated KeyResponse resps = 1;
}
```

## 동작 원리

1. **SST 파일 열기**: 요청된 SST 파일 경로를 사용하여 `BlockBasedTable`을 엽니다.
2. **Filter 체크**: `whole_key_filtering=true`인 경우 `FilterBlockReader::KeyMayMatch()`를 호출합니다.
3. **결과 반환**: Filter가 "maybe"라고 응답한 SST 파일만 결과에 포함합니다.
4. **False Negative 방지**: 에러 발생 시 안전하게 `true`(may_match)를 반환합니다.

## 주의사항

- **whole_key_filtering**: 현재 구현은 `whole_key_filtering=true`인 SST만 지원합니다. `false`인 경우 항상 `true`를 반환합니다.
- **Timestamp**: User-defined timestamp가 있는 경우 자동으로 strip됩니다.
- **캐싱**: LRU 캐시를 사용하여 최근 사용된 SST 파일의 TableReader를 메모리에 유지합니다.

## 성능

- Filter만 체크하므로 매우 빠릅니다 (index/data 블록 읽기 없음)
- LRU 캐싱으로 반복 요청 시 성능 향상
- 멀티스레드 요청 처리 지원

## 예제

### Python 클라이언트 예제

```python
import grpc
import bloomcheck_pb2
import bloomcheck_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = bloomcheck_pb2_grpc.BloomCheckStub(channel)

request = bloomcheck_pb2.BatchRequest()
key_req = request.reqs.add()
key_req.user_key = b"my_key"
key_req.sst_paths.append("/path/to/sst1.sst")
key_req.sst_paths.append("/path/to/sst2.sst")

response = stub.Check(request)
for resp in response.resps:
    print("May match SSTs:", resp.may_match_sst_paths)
```

## 문제 해결

### 빌드 에러: gRPC를 찾을 수 없음

```bash
# pkg-config로 경로 확인
pkg-config --cflags --libs grpc++
pkg-config --cflags --libs protobuf

# 필요시 경로 수동 지정
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH
```

### 런타임 에러: SST 파일을 열 수 없음

- SST 파일 경로가 올바른지 확인
- 파일 읽기 권한 확인
- SST 파일이 손상되지 않았는지 확인

## 라이선스

RocksDB와 동일한 라이선스를 따릅니다 (GPLv2 및 Apache 2.0).

