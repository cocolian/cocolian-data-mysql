#!/usr/bin/env bash
#!/bin/bash

PROTO_RES_HOME="$PWD"
echo "res home ==> $PROTO_RES_HOME"

PROTO_OUT_HOME="$PROTO_RES_HOME/../gen"
echo "proto out home ==> $PROTO_OUT_HOME"

echo "start generator proto to java files .."
protoc \
    --proto_path=$PROTO_RES_HOME \
    --java_out=$PROTO_OUT_HOME \
    *.proto

echo "end generator files .."
