#!/bin/bash

# Set the environment variables
export JULIA_PROJECT=@.
export STREAM_URI="aeron:udp?endpoint=0.0.0.0:40123"
export STREAM_ID=2
export BLOCK_ID=1023

# Run the Julia script
julia -e "using SpidersSendMessage" -- "$@"
