#!/bin/bash
#
# creates the python classes for our .proto
#

project_base="/home/komalydedhia/275/project2/core-netty-4.4_new/python"

rm ${project_base}/src/comm_pb2.py

protoc -I=${project_base}/resources --python_out=./src ../resources/comm.proto 
