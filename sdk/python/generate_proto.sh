#!/usr/bin/env bash
# Генерирует Python-стабы из pulse.proto.
# Запускать из директории sdk/python/
set -e

PROTO_PATH="../../api/proto"
OUT_DIR="pulse/proto"

mkdir -p "$OUT_DIR"
touch "$OUT_DIR/__init__.py"

python -m grpc_tools.protoc \
  -I "$PROTO_PATH" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_PATH/pulse.proto"

# grpc_tools генерирует абсолютный import, исправляем на относительный
sed -i '' 's/^import pulse_pb2/from . import pulse_pb2/' "$OUT_DIR/pulse_pb2_grpc.py" 2>/dev/null || \
sed -i    's/^import pulse_pb2/from . import pulse_pb2/' "$OUT_DIR/pulse_pb2_grpc.py"

echo "✓ proto stubs generated in $OUT_DIR"
