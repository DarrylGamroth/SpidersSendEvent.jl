#!/bin/bash

# Set the environment variables
export JULIA_PROJECT=@.
export STATUS_URI="aeron:udp?endpoint=localhost:40123"
export STATUS_STREAM_ID=1
export CONTROL_URI="aeron:udp?endpoint=0.0.0.0:40123"
export CONTROL_STREAM_ID=2
export CONTROL_STREAM_FILTER="Camera"
export PUB_DATA_URI_1="aeron:udp?endpoint=132.246.192.209:40123"
export PUB_DATA_STREAM_1=3
export BLOCK_ID=1023

# Run the Julia script
julia -e "using SpidersSendEvent; SpidersSendEvent.main(ARGS)" -- "$@"
