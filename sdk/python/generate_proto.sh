#!/usr/bin/env bash
# Генерирует Python-стабы из solenix.proto.
# Запускать из директории sdk/python/
set -e

PROTO_PATH="../../api/proto"
OUT_DIR="solenix/proto"

mkdir -p "$OUT_DIR"
touch "$OUT_DIR/__init__.py"

python -m grpc_tools.protoc \
  -I "$PROTO_PATH" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_PATH/solenix.proto"

# grpc_tools генерирует абсолютный import, исправляем на относительный
sed -i '' 's/^import solenix_pb2/from . import solenix_pb2/' "$OUT_DIR/solenix_pb2_grpc.py" 2>/dev/null || \
sed -i    's/^import solenix_pb2/from . import solenix_pb2/' "$OUT_DIR/solenix_pb2_grpc.py"

echo "✓ proto stubs generated in $OUT_DIR"
