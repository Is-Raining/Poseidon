#!/usr/bin/env bash

#set -x
set -e

cd $(dirname $0) || exit 1

protoc -I . --doc_out=../../docs --doc_opt=markdown,api.md \
  ./agent/*.proto \
  ./master/*.proto \
  ./adapter/**/*.proto \
  ./types/*.proto \
  ./code/*.proto

find . -name "*.proto" ! -path google ! -path validate -print0 | while read -r -d $'\0' proto_file; do
  proto_base_name="$(basename "${proto_file}" .proto)"
  proto_dir="$(dirname "${proto_file}")"

  protoc -I . \
    --go_out paths=source_relative:. \
    --go-grpc_out paths=source_relative:. \
    --grpc-gateway_out paths=source_relative:. --grpc-gateway_opt logtostderr=true --grpc-gateway_opt generate_unbound_methods=true \
    ${proto_file}
  printf "\r\033[K%s compilied \n" "${proto_file}"

done

printf "\r\033[Kproto-gen done...\n"
