module SpidersSendEvent

using Aeron
using ArgParse
using Clocks
using Downloads
using EnumX
using FITSIO
using URIs
using SnowflakeId
using SpidersMessageCodecs

export main

const CONNECTION_TIMEOUT_NS = 1_000_000_000

const BLOCK_ID = parse(Int, get(ENV, "BLOCK_ID", "1023"))
const clock = CachedEpochClock()
const id_gen = SnowflakeIdGenerator(BLOCK_ID, clock)

function is_valid_uri(url::AbstractString)
    try
        u = URI(url)
        return isvalid(u) && u.scheme in ["file", "http", "https", "ftp", "ftps"]
    catch e
        return false
    end
end

function offer(p, bufs, max_attempts=10)
    attempts = max_attempts
    while attempts > 0
        result = Aeron.offer(p, bufs)
        if result > 0
            return
        elseif result in (Aeron.PUBLICATION_BACK_PRESSURED, Aeron.PUBLICATION_ADMIN_ACTION)
            continue
        elseif result == Aeron.PUBLICATION_NOT_CONNECTED
            return
        elseif result == Aeron.PUBLICATION_ERROR
            @error "Publication error"
            Aeron.throwerror()
            return
        end
        attempts -= 1
    end
    @error "Offer failed"
end

function encode_event(tag::AbstractString, @nospecialize event::Pair{K,V}) where {K<:AbstractString,V}
    data = event.second
    buf = zeros(UInt8, 128 + sizeof(data))
    encoder = SpidersMessageCodecs.EventMessageEncoder(buf)
    header = SpidersMessageCodecs.header(encoder)
    SpidersMessageCodecs.timestampNs!(header, time_nanos(clock))
    SpidersMessageCodecs.correlationId!(header, next_id(id_gen))
    SpidersMessageCodecs.tag!(header, tag)
    SpidersMessageCodecs.key!(encoder, event.first)
    SpidersMessageCodecs.encode(encoder, data)

    convert(AbstractArray{UInt8}, encoder)
end

function encode_event(tag::AbstractString, @nospecialize event::Pair{K,V}) where {K<:AbstractString,V<:AbstractArray}
    data = event.second
    buf = zeros(UInt8, 128 + ndims(data) * sizeof(Int32) + sizeof(data))
    encoder = SpidersMessageCodecs.TensorMessageEncoder(buf)
    header = SpidersMessageCodecs.header(encoder)
    SpidersMessageCodecs.timestampNs!(header, time_nanos(clock))
    SpidersMessageCodecs.correlationId!(header, next_id(id_gen))
    SpidersMessageCodecs.tag!(header, tag)
    SpidersMessageCodecs.encode(encoder, data)

    encode_event(tag, event.first => encoder)
end

function encode_event(tag::AbstractString, @nospecialize event::Pair{K,V}) where {K<:AbstractString,V<:URI}
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
    else
        error("Unsupported URI scheme: $(uri.scheme)")
    end

    encode_event(tag, event.first => data)
end

function encode_tensor(tag::AbstractString, @nospecialize event::Pair{K,V}) where {K<:AbstractString,V<:URI}
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
    else
        error("Unsupported URI scheme: $(uri.scheme)")
    end

    buf = zeros(UInt8, 128 + ndims(data) * sizeof(Int32) + sizeof(data))
    encoder = SpidersMessageCodecs.TensorMessageEncoder(buf)
    header = SpidersMessageCodecs.header(encoder)
    SpidersMessageCodecs.timestampNs!(header, time_nanos(clock))
    SpidersMessageCodecs.correlationId!(header, next_id(id_gen))
    SpidersMessageCodecs.tag!(header, tag)
    SpidersMessageCodecs.encode(encoder, data)

    convert(AbstractArray{UInt8}, encoder)
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
        return kwargs
    end
    return nothing
end

function (@main)(ARGS)
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--aerondir"
        help = "Path to Aeron Media Driver directory"
        arg_type = String
        required = false
        "--uri"
        help = "Destination URI for publications"
        arg_type = String
        required = !haskey(ENV, "STREAM_URI")
        "--stream"
        help = "Stream ID for publications"
        arg_type = Int
        required = !haskey(ENV, "STREAM_ID")
        "--tag"
        help = "SPIDERS Message Tag"
        arg_type = String
        required = true
        "--tensor"
        help = "Tensor data to send"
        action = :store_true
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
    uri = get(ENV, "STREAM_URI", parsed_args["uri"])
    stream = parse(Int, get(ENV, "STREAM_ID", string(parsed_args["stream"])))
    tag = parsed_args["tag"]
    key_values = parsed_args["key-values"]
    tensor = get(parsed_args, "tensor", false)

    # Fetch the current time
    fetch!(clock, EpochClock())

    kwargs = parse_key_values(key_values)
    if isnothing(kwargs)
        return 0
    end
    messages = Vector{UInt8}[]

    if tensor
        for arg in kwargs
            push!(messages, encode_tensor(tag, arg))
        end
    else
        for arg in kwargs
            push!(messages, encode_event(tag, arg))
        end
    end

    Aeron.Context() do context
        if aerondir !== nothing
            Aeron.aeron_dir!(context, aerondir)
        end

        Aeron.Client(context) do client

            p = Aeron.add_publication(client, uri, stream)

            # Wait for connection
            try
                start_time = time_ns()
                while !Aeron.is_connected(p)
                    if (time_ns() - start_time) > CONNECTION_TIMEOUT_NS
                        @error "Connection timeout"
                        return -1
                    end
                    sleep(0.1)
                end

                offer(p, messages)
            finally
                close(p)
            end
        end
    end

    return 0
end

precompile(parse_key_values, (Vector{String},))
precompile(main, (Vector{String},))
precompile(encode_event, (String, Pair{SubString{String},String}))
precompile(encode_event, (String, Pair{SubString{String},Int}))
precompile(encode_event, (String, Pair{SubString{String},Float64}))
precompile(encode_event, (String, Pair{SubString{String},URI}))
precompile(encode_event, (String, Pair{SubString{String},SpidersMessageCodecs.EventMessageEncoder{Vector{UInt8},true}}))
precompile(encode_event, (String, Pair{SubString{String},SpidersMessageCodecs.TensorMessageEncoder{Vector{UInt8},true}}))
precompile(is_valid_uri, (String,))
precompile(offer, (Aeron.Publication, Vector{UInt8}))
precompile(try_claim, (Aeron.Publication, Int))
precompile(Aeron.add_publication, (Aeron.Client, String, Int))
precompile(Aeron.is_connected, (Aeron.Publication,))
precompile(Aeron.aeron_dir!, (Aeron.Context, String))

end
