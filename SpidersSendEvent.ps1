# Set the environment variables
$env:JULIA_PROJECT = "@."
$env:STATUS_URI = "aeron:udp?endpoint=localhost:40123"
$env:STATUS_STREAM_ID = 1
$env:CONTROL_URI = "aeron:udp?endpoint=0.0.0.0:40123"
$env:CONTROL_STREAM_ID = 2
$env:CONTROL_STREAM_FILTER = "Camera"
$env:PUB_DATA_URI_1 = "aeron:udp?endpoint=132.246.192.209:40123"
$env:PUB_DATA_STREAM_1 = 3
$env:BLOCK_NAME = "Camera"
$env:BLOCK_ID = 367

# Run the Julia script
& "julia" -e "using SpidersSendEvent; SpidersSendEvent.main(ARGS)" -- $args