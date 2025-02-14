#! /usr/bin/env julia
ENV["STATUS_URI"] = "aeron:udp?endpoint=localhost:40123"
ENV["STATUS_STREAM_ID"] = "1"

ENV["CONTROL_URI"] = "aeron:udp?endpoint=localhost:40123"
ENV["CONTROL_STREAM_ID"] = "2"

ENV["BLOCK_ID"] = "1023"

module SpidersSendEvent

include("uvclockgetttime.jl")
using .Clocks

using Aeron
using ArgParse
using Downloads
using EnumX
using FITSIO
using URIs
using SnowflakeId
using SpidersMessageCodecs

export main

const CONNECTION_TIMEOUT_NS = 1_000_000_000

const BLOCK_ID = parse(Int, get(ENV, "BLOCK_ID", "1023"))
const id_gen = SnowflakeIdGenerator(BLOCK_ID)

function is_valid_uri(url::AbstractString)
    try
        u = URI(url)
        return isvalid(u) && u.scheme in ["file", "http", "https", "ftp", "ftps"]
    catch e
        return false
    end
end

function offer(p, buf, max_attempts=10)
    attempts = max_attempts
    while attempts > 0
        result = Aeron.offer(p, buf)
        if result > 0
            return
        elseif result in (Aeron.PUBLICATION_BACK_PRESSURED, Aeron.PUBLICATION_ADMIN_ACTION)
            continue
        elseif result == Aeron.PUBLICATION_ERROR
            @error "Publication error"
            Aeron.throwerror()
            return
        end
        attempts -= 1
    end
    @error "Offer failed"
end

function try_claim(p, length, max_attempts=10)
    attempts = max_attempts
    while attempts > 0
        claim, result = Aeron.try_claim(p, length)
        if result > 0
            return claim
        elseif result in (Aeron.PUBLICATION_BACK_PRESSURED, Aeron.PUBLICATION_ADMIN_ACTION)
            continue
        elseif result == Aeron.PUBLICATION_ERROR
            Aeron.throwerror()
            return
        end
        attempts -= 1
    end
    @error "Try claim failed"
end

function encode(tag::AbstractString, event::Pair{T,S}) where {T<:AbstractString,S}
    timestamp = clock_gettime(uv_clock_id.REALTIME)
    if Sbe.is_sbe_message(S)
        len = Sbe.sbe_decoded_length(event.second)
    else
        len = sizeof(S)
    end
    buf = zeros(UInt8, 128 + len)
    encoder = Event.EventMessageEncoder(buf, Event.MessageHeader(buf))
    header = Event.header(encoder)
    Event.timestampNs!(header, timestamp)
    Event.correlationId!(header, next_id(id_gen))
    Event.tag!(String, header, tag)

    encoder(event...)
    convert(AbstractVector{UInt8}, encoder)
end

function encode(tag::AbstractString, event::Pair{T,S}) where {T<:AbstractString,S<:URI}
    timestamp = clock_gettime(uv_clock_id.REALTIME)
    buf = zeros(UInt8, 128)
    encoder = Tensor.TensorMessageEncoder(buf, Tensor.MessageHeader(buf))
    header = Tensor.header(encoder)
    Tensor.timestampNs!(header, timestamp)
    Tensor.correlationId!(header, next_id(id_gen))
    Tensor.tag!(String, header, tag)

    uri = event.second

    if endswith(uri.uri, r"\.fits?(.gz)?")
        data = FITS(uri.uri, "r") do hdus
            read(hdus[1])
        end
    elseif uri.scheme == "file"
        data = open(uri.path, "r") do f
            read(f)
        end
    elseif uri.scheme in ["http", "https", "ftp"]
        io = IOBuffer()
        Downloads.download(uri.uri, io)
        data = take!(io)
    end

    resize!(buf, 128 + ndims(data) * sizeof(Int32) + sizeof(data))

    encoder(data)
    encode(tag, event.first => encoder)
end

function parse_key_values(key_values)
    if !isempty(key_values)
        kwargs = map(key_values) do argstr
            args = split(argstr, "=")
            if length(args) == 2
                key, value = args
                if occursin(r"^'.*'$", value) || occursin(r"^\".*\"$", value)
                    return key => value[2:end-1]
                elseif occursin(r"^(true|false)$", value)
                    return key => parse(Bool, value)
                elseif occursin(r"^[+-]?\d+$", value)
                    return key => parse(Int64, value)
                elseif occursin(r"^[+-]?\d+[bB]$", value)
                    return key => parse(Int8, value[1:end-1])
                elseif occursin(r"^[+-]?\d+[hH]$", value)
                    return key => parse(Int16, value[1:end-1])
                elseif occursin(r"^[+-]?\d+[lL]$", value)
                    return key => parse(Int32, value[1:end-1])
                elseif occursin(r"^[+-]?\d+[lL][lL]$", value)
                    return key => parse(Int64, value[1:end-2])
                elseif occursin(r"^[+-]?\d+[uU]$", value)
                    return key => parse(UInt64, value[1:end-1])
                elseif occursin(r"^[+-]?\d+[uU][bB]$", value)
                    return key => parse(UInt8, value[1:end-2])
                elseif occursin(r"^[+-]?\d+[uU][hH]$", value)
                    return key => parse(UInt16, value[1:end-2])
                elseif occursin(r"^[+-]?\d+[uU][lL]$", value)
                    return key => parse(UInt32, value[1:end-2])
                elseif occursin(r"^[+-]?\d+[uU][lL][lL]$", value)
                    return key => parse(UInt64, value[1:end-3])
                elseif occursin(r"^0[xX][0-9a-fA-F]+$", value)
                    return key => parse(UInt64, value)
                elseif occursin(r"^[+-]?(\d+([.]\d*)?([eE][+-]?\d+)?|[.]\d+([eE][+-]?\d+)?)$", value)
                    return key => parse(Float64, value)
                elseif is_valid_uri(value)
                    return key => parse(URI, value)
                else
                    return key => value
                end
            elseif length(args) == 1
                key = args[1]
                return key => nothing
            else
                error("multiple '=' delimiters encountered in single argument")
            end
        end
    end
end

function main(ARGS)
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--aerondir"
        help = "Path to Aeron Media Driver directory"
        arg_type = String
        required = false
        "--uri"
        help = "Destination URI for publications"
        arg_type = String
        required = !haskey(ENV, "CONTROL_URI")
        "--stream"
        help = "Stream ID for publications"
        arg_type = Int
        required = !haskey(ENV, "CONTROL_STREAM_ID")
        "--status-uri"
        help = "SPIDERS Status URI"
        arg_type = String
        required = !haskey(ENV, "STATUS_URI")
        "--status-stream"
        help = "SPIDERS Status Stream ID"
        arg_type = Int
        required = !haskey(ENV, "STATUS_STREAM_ID")
        "--tag"
        help = "SPIDERS Event Tag"
        arg_type = String
        required = true
        "key-values"
        help = "Key-Value pairs to send with the event"
        nargs = '+'
        arg_type = String
    end
    parsed_args = parse_args(ARGS, s)
    if isnothing(parsed_args)
        return -1
    end

    aerondir = get(ENV, "AERON_DIR", parsed_args["aerondir"])
    uri = get(ENV, "CONTROL_URI", parsed_args["uri"])
    stream = parse(Int, get(ENV, "CONTROL_STREAM_ID", string(parsed_args["stream"])))
    status_uri = get(ENV, "STATUS_URI", parsed_args["status-uri"])
    status_stream = parse(Int, get(ENV, "STATUS_STREAM_ID", string(parsed_args["status-stream"])))
    tag = parsed_args["tag"]
    key_values = parsed_args["key-values"]

    kwargs = parse_key_values(key_values)

    # messages = ntuple(i -> encode(tag, kwargs[i]), length(kwargs))

    messages = Vector{UInt8}[]
    for arg in kwargs
        push!(messages, encode(tag, arg))
    end

    context = Aeron.Context()
    if aerondir !== nothing
        Aeron.aeron_dir!(context, aerondir)
    end

    client = Aeron.Client(context)

    p = Aeron.add_publication(client, uri, stream)
    # s = Aeron.add_subscription(client, status_uri, status_stream)
    # f = Aeron.FragmentHandler(message_handler, messages)
    # filter = SpidersEventTagFragmentFilter(f, tag)
    # fa = Aeron.FragmentAssembler(filter)

    # Wait for connection
    start_time = time_ns()
    while !Aeron.is_connected(p)
        if (time_ns() - start_time) > CONNECTION_TIMEOUT_NS
            @error "Connection timeout"
            return -1
        end
        sleep(0.1)
    end

    offer(p, messages)

    return 0
end

function message_handler(messages, buffer, header)
    message = Event.EventMessageDecoder(buffer, offset, Event.MessageHeader(buffer))
    spiders_header = Event.header(message)
    correlation_id = Event.correlationId(spiders_header)
    println("Received message: $message\n")

    nothing
end

end

using .SpidersSendEvent
const main = SpidersSendEvent.main
@isdefined(var"@main") ? (@main) : exit(main(ARGS))

