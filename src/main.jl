#!/usr/bin/env julia

include("SpidersSendEvent.jl")

import .SpidersSendEvent

function (@main)(ARGS)
    SpidersSendEvent.main(ARGS)
end