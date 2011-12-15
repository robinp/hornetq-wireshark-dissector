-- HornetQ 2.2.5 dissector
HQPROTO = Proto("hornetq","HornetQ Protocol")

local tPing = 10
local tDisconnect = 11
local tException = 20
local tNullResponse = 21
local tPacketsConfirmed = 22
local tCreateSession = 30
local tCreateSessionResp = 31
local tSessCreateConsumer = 40
local tSessAcknowledge = 41
local tSessExpired = 42
local tSessCommit = 43
local tSessRollback = 44
local tSessQueueQuery = 45
local tSessQueueQueryResp = 46
local tSessBindingQuery = 49
local tSessBindingQueryResp = 50
local tSessStart = 67
local tSessStop = 68
local tSessClose = 69
local tSessFlowToken = 70
local tSessSend = 71
local tSessConsumerClose = 74
local tSessReceiveMsg = 75
local tSessReceiveLargeMsg = 75
local tSessProducerRequestCredits = 79
local tSessProducerCredits = 80
local tSessAndMeta2 = 105

local f = HQPROTO.fields
local types = { 
    [tPing] = "Ping", 
    [tDisconnect] = "Disconnect !",
    [tException] = "Exception !",
    [tNullResponse] = "NullResponse !",
    [tPacketsConfirmed] = "PacketsConfirmed !",
    [tCreateSession] = "CreateSession",
    [tCreateSessionResp] = "CreateSessionResp !",
    [tSessCreateConsumer] = "SessCreateConsumer !",
    [tSessAcknowledge] = "SessAcknowledge !",
    [tSessExpired] = "SessExpired !",
    [tSessCommit] = "SessCommit !",
    [tSessRollback] = "SessRollback !",
    [tSessQueueQuery] = "SessQueueQuery !",
    [tSessQueueQueryResp] = "SessQueueQueryResp !",
    [tSessBindingQuery] = "SessBindingQuery !",
    [tSessBindingQueryResp] = "SessBindingQueryResp !",
    [tSessStart] = "SessStart !",
    [tSessStop] = "SessStop !",
    [tSessClose] = "SessClose !",
    [tSessFlowToken] = "SessFlowToken !",
    [tSessSend] = "SessSend !",
    [tSessConsumerClose] = "SessConsumerClose !",
    [tSessReceiveMsg] = "SessReceiveMsg !",
    [tSessReceiveLargeMsg] = "SessReceiveLargeMsg !",
    [tSessProducerRequestCredits] = "SessProducerRequestCredits !",
    [tSessProducerCredits] = "SessProducerCredits !",
    [tSessAndMeta2] = "SessionAndMetadata2 !",
}

f.msglen    = ProtoField.uint32     ("hornetq.length", "Message Length")
f.type      = ProtoField.uint8      ("hornetq.type", "Message Type", nil, types) 
f.channel   = ProtoField.uint64     ("hornetq.channel", "Channel ID")
f.data      = ProtoField.bytes      ("hornetq.data", "Data")

-- Abstract
f.nullstr   = ProtoField.bytes      ("hornetq.nullstr", "Nullable String")

-- Ping
f.connectionTTL = ProtoField.uint64 ("hornetq.connection_ttl", "Connection TTL")

-- CreateSession
f.session_name      = ProtoField.string     ("hornetq.session_name", "Session Name")
f.session_channel   = ProtoField.uint64     ("hornetq.session_channel", "Session Channel ID")
f.version           = ProtoField.uint32     ("hornetq.version", "Version") -- is this session version?
f.username          = ProtoField.string     ("hornetq.username", "User Name")
f.password          = ProtoField.string     ("hornetq.password", "Password")
f.min_large_msg_size    = ProtoField.uint32     ("hornetq.min_large_msg_size", "Minimum large message size")
f.xa                    = ProtoField.bool       ("hornetq.xa", "XA")
f.auto_commit_sends     = ProtoField.bool       ("hornetq.auto_commit_sends", "Auto-commit Sends")
f.auto_commit_acks      = ProtoField.bool       ("hornetq.auto_commit_acks", "Auto-commit ACKs")
f.window_size           = ProtoField.uint32     ("hornetq.window_size", "Window size")
f.pre_ack           = ProtoField.bool       ("hornetq.pre_ack", "Pre-acknowledge")
f.default_address   = ProtoField.string     ("hornetq.default_address", "Default Address")

-- SessionAndMetadata2
f.key               = ProtoField.string     ("hornetq.key",  "Metadata Key")
f.value             = ProtoField.string     ("hornetq.data", "Metadata Value")
f.requires_confirmation = ProtoField.bool   ("hornetq.requires_confirmation", "Requires Confirmation")

function HQPROTO.dissector(buffer,pinfo,tree)
    pinfo.cols.protocol = "HORNETQ"
    local packetnum = pinfo.number
    
    local offset = 0

    -- length
    local remains = buffer(offset, 4)
    local remainsVal = remains:uint()
    local hqPacketLength = remainsVal + 4

    local bufferLength = buffer:len()

    -- desegment is packet not entirely in buffer
    if hqPacketLength > bufferLength then
        pinfo.desegment_offset = offset
        pinfo.desegment_len = hqPacketLength - bufferLength
        debug("wantmore: got " .. bufferLength .. " want " .. hqPacketLength)
        -- return hqPacketLength
        return
    end
    debug("processing: got " .. bufferLength .. " want " .. hqPacketLength)

    local subtree = tree:add(HQPROTO, buffer(offset, hqPacketLength))
    
    function addField(field, length)
        local buf = buffer(offset, length)
        subtree:add(field, buf)
        offset = offset + length
        return buf
    end    
    
    function addStringField(field, t, origOffset)
        local origOffset = origOffset or offset
        local strlen = buffer(offset, 4):uint()
        offset = offset + 4
        if strlen < 9 then
            local wideStr = buffer(offset, strlen * 2):string()
            offset = offset + strlen * 2            
            local asciiStr = string.gsub(wideStr, "(.)(.)", "%2")                        
            t:add(field, buffer(origOffset, offset - origOffset), asciiStr)
        elseif strlen < 0xfff then
            local bytecount = buffer(offset, 2):uint()
            offset = offset + 2
            -- this is not decoded correctly if string is non-ascii
            local asciiStr = buffer(offset, bytecount):string()
            offset = offset + bytecount
            t:add(field, buffer(origOffset, offset - origOffset), asciiStr)
        else
            local simplelen = buffer(offset, 4):uint()
            offset = offset + 4
            local simplebuf = buffer(offset, simplelen)
            offset = offset + simplelen
            t:add(field, simplebuf, "!!! Decode Simple String")
        end
    end
    
    function addNullableStringField(field, t)
        local origOffset = offset
        local present = buffer(offset, 1):uint()
        offset = offset + 1
        if present == 0 then
            subtree:add(field, buffer(origOffset, 1), "<nil>")            
        else
            addStringField(field, t, origOffset)
        end        
    end
    
    addField(f.msglen, 4)
    subtree:append_text(", Length: " .. remainsVal)
    
    -- type
    local typeBuf = addField(f.type, 1)
    local typeVal = typeBuf:uint()

    pinfo.cols.info = types[typeVal]
    
    -- channel
    addField(f.channel, 8)
    
    if typeVal == tPing then    
        addField(f.connectionTTL, 8)        
    elseif typeVal == tCreateSession then
        addStringField(f.session_name, subtree)
        addField(f.session_channel, 8)
        addField(f.version, 4)
        addNullableStringField(f.username, subtree)
        addNullableStringField(f.password, subtree)
        addField(f.min_large_msg_size, 4)
        addField(f.xa, 1)
        addField(f.auto_commit_sends, 1)
        addField(f.auto_commit_acks, 1)
        addField(f.window_size, 4)
        addField(f.pre_ack, 1)
        addNullableStringField(f.default_address, subtree)
    elseif typeVal == tSessAndMeta2 then
        addStringField(f.key, subtree)
        addStringField(f.value, subtree)
        addField(f.requires_confirmation, 1)
    end

    debug("done: got " .. bufferLength .. " want " .. hqPacketLength .. " type " .. typeVal .. " offs " .. offset)

    if hqPacketLength < bufferLength then
        -- desegment if packet is smaller than buffer
        debug("desegmenting")
        pinfo.desegment_offset = offset
        pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
    end
    
end

tcp_table = DissectorTable.get("tcp.port")
tcp_table:add(5445, HQPROTO)
