#!/bin/bash
# BloomCheck Client만 빌드하는 스크립트 (컴퓨트 노드용)
# RocksDB 전체 라이브러리 없이 클라이언트만 빌드

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS_DIR="$SCRIPT_DIR"
OUTPUT_NAME="bloomcheck_client"

# Find g++ with C++20 support
CXX_CANDIDATES=(g++-13 g++-12 g++-11 g++)
CXX=""
for candidate in "${CXX_CANDIDATES[@]}"; do
    if command -v "$candidate" &> /dev/null; then
        CXX_VERSION=$("$candidate" -dumpversion | cut -d. -f1)
        if [ "$CXX_VERSION" -ge 11 ]; then
            CXX="$candidate"
            break
        fi
    fi
done

if [ -z "$CXX" ]; then
    echo "Error: g++-11 or higher is required for C++20 support" >&2
    echo "Install with: sudo apt-get install g++-11" >&2
    exit 1
fi

echo "Using compiler: $CXX"
$CXX --version

# Check for grpc_cpp_plugin
GRPC_PLUGIN=$(which grpc_cpp_plugin 2>/dev/null || echo "")
if [ -z "$GRPC_PLUGIN" ]; then
    echo "Error: grpc_cpp_plugin not found" >&2
    echo "Install with: sudo apt-get install protobuf-compiler-grpc" >&2
    exit 1
fi

echo "Using grpc_cpp_plugin: $GRPC_PLUGIN"

# Generate protobuf files
echo "Generating protobuf files..."
protoc --cpp_out="$TOOLS_DIR" --proto_path="$TOOLS_DIR" "$TOOLS_DIR/bloomcheck.proto"
protoc --grpc_out="$TOOLS_DIR" --cpp_out="$TOOLS_DIR" --proto_path="$TOOLS_DIR" \
    --plugin=protoc-gen-grpc="$GRPC_PLUGIN" "$TOOLS_DIR/bloomcheck.proto"

# Build client
echo "Building $OUTPUT_NAME..."
$CXX -std=c++20 -O2 -Wall -Wextra \
    -I"$TOOLS_DIR/.." -I"$TOOLS_DIR" \
    "$TOOLS_DIR/bloomcheck_client.cc" \
    "$TOOLS_DIR/bloomcheck.pb.cc" \
    "$TOOLS_DIR/bloomcheck.grpc.pb.cc" \
    -lgrpc++ -lprotobuf -lgpr -labsl_synchronization -pthread \
    -o "$OUTPUT_NAME"

if [ $? -eq 0 ]; then
    echo "Success! Built $OUTPUT_NAME"
    file "$OUTPUT_NAME"
    ls -lh "$OUTPUT_NAME"
else
    echo "Build failed!" >&2
    exit 1
fi

