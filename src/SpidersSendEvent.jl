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

"""
    is_valid_uri(url::AbstractString) -> Bool

Check if the given URL string represents a valid URI with a supported scheme.
"""
function is_valid_uri(url::AbstractString)
    try
        u = URI(url)
        return isvalid(u) && u.scheme in ["file", "http", "https", "ftp", "ftps"]
    catch e
        return false
    end
end

"""
    load_data_from_uri(uri::URI) -> Vector{UInt8}

Load data from a URI source (FITS file, local file, or HTTP resource).
Throws an error for unsupported URI schemes.
"""
function load_data_from_uri(uri::URI)
    if endswith(uri.uri, r"\.fits?(.gz)?")
        return FITS(uri.uri, "r") do hdus
            read(hdus[1])
        end
    elseif uri.scheme == "file"
        if !isfile(uri.path)
            error("File not found: $(uri.path)")
        end
        return open(uri.path, "r") do f
            read(f)
        end
    elseif uri.scheme in ["http", "https", "ftp"]
        io = IOBuffer()
        Downloads.download(uri.uri, io)
        return take!(io)
    else
        error("Unsupported URI scheme: $(uri.scheme)")
    end
end

"""
    offer(p::Aeron.Publication, buffers, max_attempts::Int=10)

Attempt to offer the buffer(s) to the Aeron publication, retrying up to max_attempts times.
Aeron.offer already handles multiple buffers, so we pass the entire vector.
"""
function offer(p::Aeron.Publication, buffers, max_attempts::Int=10)
    attempts = max_attempts
    while attempts > 0
        result = Aeron.offer(p, buffers)
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
    if attempts == 0
        @error "Offer failed"
    end
end

"""
    encode_event(tag::AbstractString, event::Pair{K,V}) where {K<:AbstractString,V}

Encode an event message with the given tag and key-value pair.
"""
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

"""
    encode_event(tag::AbstractString, event::Pair{K,V<:AbstractArray}) where {K<:AbstractString}

Handle encoding of array values by wrapping them as tensor messages within event messages.
"""
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

"""
    encode_event(tag::AbstractString, event::Pair{K,V<:URI}) where {K<:AbstractString}

Load data from the URI and encode as an event.
"""
function encode_event(tag::AbstractString, @nospecialize event::Pair{K,V}) where {K<:AbstractString,V<:URI}
    uri = event.second
    data = load_data_from_uri(uri)
    encode_event(tag, event.first => data)
end

"""
    encode_tensor(tag::AbstractString, event::Pair{K,V<:AbstractArray}) where {K<:AbstractString,V<:AbstractArray}

Encode an array as a tensor message.
"""
function encode_tensor(tag::AbstractString, @nospecialize event::Pair{K,V}) where {K<:AbstractString,V<:AbstractArray}
    data = event.second
    buf = zeros(UInt8, 128 + ndims(data) * sizeof(Int32) + sizeof(data))
    encoder = SpidersMessageCodecs.TensorMessageEncoder(buf)
    header = SpidersMessageCodecs.header(encoder)
    SpidersMessageCodecs.timestampNs!(header, time_nanos(clock))
    SpidersMessageCodecs.correlationId!(header, next_id(id_gen))
    SpidersMessageCodecs.tag!(header, tag)
    SpidersMessageCodecs.encode(encoder, data)

    convert(AbstractArray{UInt8}, encoder)
end

"""
    encode_tensor(tag::AbstractString, event::Pair{K,V<:URI}) where {K<:AbstractString}

Load data from the URI and encode as a tensor.
"""
function encode_tensor(tag::AbstractString, @nospecialize event::Pair{K,V}) where {K<:AbstractString,V<:URI}
    uri = event.second
    data = load_data_from_uri(uri)
    encode_tensor(tag, event.first => data)
end

"""
    encode_tensor(tag::AbstractString, uri::URI)

Directly encode a tensor from a URI with no key.
"""
function encode_tensor(tag::AbstractString, uri::URI)
    data = load_data_from_uri(uri)
    buf = zeros(UInt8, 128 + ndims(data) * sizeof(Int32) + sizeof(data))
    encoder = SpidersMessageCodecs.TensorMessageEncoder(buf)
    header = SpidersMessageCodecs.header(encoder)
    SpidersMessageCodecs.timestampNs!(header, time_nanos(clock))
    SpidersMessageCodecs.correlationId!(header, next_id(id_gen))
    SpidersMessageCodecs.tag!(header, tag)
    SpidersMessageCodecs.encode(encoder, data)
    convert(AbstractArray{UInt8}, encoder)
end

"""
    parse_key_values(key_values::Vector{String})

Parse key-value pairs from command line strings into a vector of pairs.
Returns nothing if the input is empty.
"""
function parse_key_values(key_values::Vector{String})
    if isempty(key_values)
        return nothing
    end
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

"""
    main(ARGS)

Main entry point for the CLI. Handles argument parsing and message encoding/sending.
"""
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
        "--stream"
        help = "Stream ID for publications"
        arg_type = String
        "--tag"
        help = "SPIDERS Message Tag"
        arg_type = String
        required = true
        "--tensor"
        help = "Send data as tensor. Use file-type=URI format (fits=URI, csv=URI, etc.)"
        action = :store_true
        "key-values"
        help = "Key-Value pairs to send"
        nargs = '*'
        arg_type = String
    end
    parsed_args = parse_args(ARGS, s)
    if isnothing(parsed_args)
        return -1
    end

    # Environment and basic setup code remains the same
    aerondir = get(parsed_args, "aerondir", nothing)
    if isnothing(aerondir)
        aerondir = get(ENV, "AERON_DIR", nothing)
    end

    uri = get(parsed_args, "uri", nothing)
    if isnothing(uri)
        uri = get(ENV, "STREAM_URI", nothing)
    end
    if isnothing(uri)
        @error "STREAM_URI environment variable not set"
        return -1
    end

    stream_str = get(parsed_args, "stream", nothing)
    if isnothing(stream_str)
        stream_str = get(ENV, "STREAM_ID", nothing)
    end
    if isnothing(stream_str)
        @error "STREAM_ID environment variable not set"
        return -1
    end
    stream = parse(Int, stream_str)

    tag = parsed_args["tag"]
    send_tensor = get(parsed_args, "tensor", false)

    # Fetch the current time
    fetch!(clock, EpochClock())

    messages = Vector{UInt8}[]
    key_values = get(parsed_args, "key-values", String[])

    if isempty(key_values)
        @error "No key-value pairs provided"
        return -1
    end

    # Parse key-values regardless of mode
    kwargs = parse_key_values(key_values)
    if isnothing(kwargs)
        @error "Error parsing key-value pairs"
        return -1
    end

    if send_tensor
        # In tensor mode, each key is treated as a tensor URI
        for (key, _) in kwargs
            push!(messages, encode_tensor(tag, key => parse(URI, key)))
        end
    else
        for arg in kwargs
            push!(messages, encode_event(tag, arg))
        end
    end

    # Aeron context and sending code remains the same
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

                @info "Sending $(length(messages)) messages"
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
precompile(encode_tensor, (String, URI))
precompile(is_valid_uri, (String,))
precompile(offer, (Aeron.Publication, Vector{Vector{UInt8}}))
precompile(Aeron.add_publication, (Aeron.Client, String, Int))
precompile(Aeron.is_connected, (Aeron.Publication,))
precompile(Aeron.aeron_dir!, (Aeron.Context, String))
precompile(load_data_from_uri, (URI,))

end
