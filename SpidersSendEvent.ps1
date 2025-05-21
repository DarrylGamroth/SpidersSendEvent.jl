# Set the environment variables
$env:JULIA_PROJECT = "@."
$env:STREAM_URI = "aeron:udp?endpoint=0.0.0.0:40123"
$env:STREAM_ID = 2
$env:BLOCK_ID = 1023

# Run the Julia script
& "julia" -e "using SpidersSendEvent" -- $args