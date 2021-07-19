#!/bin/bash

set -euo pipefail

DIR="$(cd "$(dirname "${0}")/../.." && pwd)"
cd "${DIR}"

if echo "${GOPATH}" | grep : >/dev/null; then
  echo "error: GOPATH can only contain one directory but is ${GOPATH}" >&2
  exit 1
fi

# Run stringer
#
# https://github.com/golang/go/issues/10249
#
# $1: type
# $2: go package
generate_stringer() {
  go install "${2}"
  stringer "-type=${1}" "${2}"
}

GOGO_PROTO_DIR="/Users/utkarsh.s/Utkarsh/pkg/mod/github.com/gogo/protobuf@v1.3.1"

protoc_all() {
  protoc_go_grpc $@
  protoc_yarpc_go $@
}

protoc_yarpc_go() {
  protoc_with_imports "yarpc-go" "" $@
}

protoc_go() {
  protoc_with_imports "gogoslick" "" $@
}

protoc_go_grpc() {
  protoc_with_imports "gogoslick" "plugins=grpc," $@
}

protoc_with_imports() {
  protoc \
    -I "$GOGO_PROTO_DIR/protobuf" \
    -I . \
    -I "/Users/utkarsh.s/Utkarsh/git/gronosq/server/pb" \
    "--${1}_out=${2}Mgoogle/protobuf/descriptor.proto=github.com/gogo/protobuf/protoc-gen-gogo/descriptor,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgogoproto/gogo.proto=github.com/gogo/protobuf/gogoproto,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types:." \
    "${@:3}"
}

protoc_all /Users/utkarsh.s/Utkarsh/git/gronosq/server/pb/server.proto
