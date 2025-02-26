# Set the environment variables
$env:JULIA_PROJECT = "@."
$env:STATUS_URI = "aeron:udp?endpoint=localhost:40123"
$env:STATUS_STREAM_ID = 1
$env:CONTROL_URI = "aeron:udp?endpoint=0.0.0.0:40123"
$env:CONTROL_STREAM_ID = 2
$env:CONTROL_STREAM_FILTER = "Camera"
$env:BLOCK_NAME = "Camera"
$env:BLOCK_ID = 1023

# Run the Julia script
& "julia" -e "using SpidersSendEvent; SpidersSendEvent.main(ARGS)" -- $args