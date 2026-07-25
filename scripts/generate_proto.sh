#!/usr/bin/env bash
# Regenerates Python gRPC stubs from proto/inventory.proto into
# services/shared/generated/. Run this after editing the .proto file.
#
# Both the api and inventory services import the generated stubs from
# services/shared/generated, so the message/stub classes only need to
# be generated once and live in one place both services can import.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$ROOT_DIR/services/shared/generated"
mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
  -I "$ROOT_DIR/proto" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  --pyi_out="$OUT_DIR" \
  "$ROOT_DIR/proto/inventory.proto"

# grpc_tools.protoc emits `import inventory_pb2 as inventory__pb2`, which
# assumes the two generated files sit in a flat, non-package directory.
# Since we want them importable as services.shared.generated.*, rewrite
# that to a package-relative import (a well-known grpc_tools gotcha).
sed -i.bak 's/^import inventory_pb2 as inventory__pb2$/from . import inventory_pb2 as inventory__pb2/' \
  "$OUT_DIR/inventory_pb2_grpc.py"
rm -f "$OUT_DIR/inventory_pb2_grpc.py.bak"

touch "$OUT_DIR/__init__.py"
echo "Generated gRPC stubs in $OUT_DIR"
