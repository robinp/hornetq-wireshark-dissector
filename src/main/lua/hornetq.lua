-- HornetQ 2.2.5 dissector
HQPROTO = Proto("hornetq","HornetQ Protocol")

local tPing = 10
local tDisconnect = 11
local tException = 20
local tNullResponse = 21
local tPacketsConfirmed = 22
local tCreateSession = 30
local tCreateSessionResp = 31
local tReattachSession = 32
local tReattachSessionResp = 33
local tCreateQueue = 34;
local tDeleteQueue = 35;
local tCreateReplication = 36;
local tSessCreateConsumer = 40
local tSessAcknowledge = 41
local tSessExpired = 42
local tSessCommit = 43
local tSessRollback = 44
local tSessQueueQuery = 45
local tSessQueueQueryResp = 46
local tSessBindingQuery = 49
local tSessBindingQueryResp = 50
local tSessXaStart = 51;
local tSessXaEnd = 52;
local tSessXaCommit = 53;
local tSessXaPrepare = 54;
local tSessXaResp = 55;
local tSessXaRollback = 56;
local tSessXaJoin = 57;
local tSessXaSuspend = 58;
local tSessXaResume = 59;
local tSessXaForget = 60;
local tSessXaIndouubtXids = 61;
local tSessXaIndouubtXidsResp = 62;
local tSessXaSetTimeout = 63;
local tSessXaSetTimeoutResp = 64;
local tSessXaGetTimeout = 65;
local tSessXaGetTimeoutResp = 66;
local tSessStart = 67
local tSessStop = 68
local tSessClose = 69
local tSessFlowToken = 70
local tSessSend = 71
local tSessSendLarge = 72;
local tSessSendContinuation = 73;
local tSessConsumerClose = 74
local tSessReceiveMsg = 75
local tSessReceiveLargeMsg = 76
local tSessReceiveContinuation = 77;
local tSessForceConsumerDelivery = 78;
local tSessProducerRequestCredits = 79
local tSessProducerCredits = 80
local tSessIndividualAcknowledge = 81;
local tReplicationResponse = 90;
local tReplicationAppend = 91;
local tReplicationAppendTx = 92;
local tReplicationDelete = 93;
local tReplicationDeleteTx = 94;
local tReplicationPrepare = 95;
local tReplicationCommitRollback = 96;
local tReplicationPageWrite = 97;
local tReplicationPageEvent = 98;
local tReplicationLargeMessageBegin = 99;
local tReplicationLargeMessageEnd = 100;
local tReplicationLargeMessageWrite = 101;
local tReplicationCompareData = 102;
local tReplicationSync = 103;
local tSessAddMetadata = 104;
local tSessAddMetadata2 = 105
local tSessUniqueAddMetadata = 106;
local tClusterTopology = 110;
local tNodeAnnounce = 111;
local tSubscribeTopology = 112;
local tSubscribeTopologyV2 = 113;
local tClusterTopology_V2 = 114;

local f = HQPROTO.fields
local types = {
    [tPing] = "Ping",
    [tDisconnect] = "Disconnect !",
    [tException] = "Exception !",
    [tNullResponse] = "NullResponse !",
    [tPacketsConfirmed] = "PacketsConfirmed !",
    [tCreateSession] = "CreateSession",
    [tCreateSessionResp] = "CreateSessionResp !",
    [tReattachSession] = "ReattachSession !",
    [tReattachSessionResp] = "ReattachSessionResp !",
    [tCreateQueue] = "CreateQueue !",
    [tDeleteQueue] = "DeleteQueue !",
    [tCreateReplication] = "CreateReplication !",
    [tSessCreateConsumer] = "SessCreateConsumer !",
    [tSessAcknowledge] = "SessAcknowledge !",
    [tSessExpired] = "SessExpired !",
    [tSessCommit] = "SessCommit !",
    [tSessRollback] = "SessRollback !",
    [tSessQueueQuery] = "SessQueueQuery !",
    [tSessQueueQueryResp] = "SessQueueQueryResp !",
    [tSessBindingQuery] = "SessBindingQuery !",
    [tSessBindingQueryResp] = "SessBindingQueryResp !",
    [tSessXaStart] = "SessXaStart !",
    [tSessXaEnd] = "SessXaEnd !",
    [tSessXaCommit] = "SessXaCommit !",
    [tSessXaPrepare] = "SessXaPrepare !",
    [tSessXaResp] = "SessXaResp !",
    [tSessXaRollback] = "SessXaRollback !",
    [tSessXaJoin] = "SessXaJoin !",
    [tSessXaSuspend] = "SessXaSuspend !",
    [tSessXaResume] = "SessXaResume !",
    [tSessXaForget] = "SessXaForget !",
    [tSessXaIndouubtXids] = "SessXaIndouubtXids !",
    [tSessXaIndouubtXidsResp] = "SessXaIndouubtXidsResp !",
    [tSessXaSetTimeout] = "SessXaSetTimeout !",
    [tSessXaSetTimeoutResp] = "SessXaSetTimeoutResp !",
    [tSessXaGetTimeout] = "SessXaGetTimeout !",
    [tSessXaGetTimeoutResp] = "SessXaGetTimeoutResp !",
    [tSessStart] = "SessStart !",
    [tSessStop] = "SessStop !",
    [tSessClose] = "SessClose !",
    [tSessFlowToken] = "SessFlowToken !",
    [tSessSend] = "SessSend !",
    [tSessSendLarge] = "SessSendLarge !",
    [tSessSendContinuation] = "SessSendContinuation !",
    [tSessConsumerClose] = "SessConsumerClose !",
    [tSessReceiveMsg] = "SessReceiveMsg !",
    [tSessReceiveLargeMsg] = "SessReceiveLargeMsg !",
    [tSessReceiveContinuation] = "SessReceiveContinuation !",
    [tSessForceConsumerDelivery] = "SessForceConsumerDelivery !",
    [tSessProducerRequestCredits] = "SessProducerRequestCredits !",
    [tSessProducerCredits] = "SessProducerCredits !",
    [tSessIndividualAcknowledge] = "SessIndividualAcknowledge !",
    [tReplicationResponse] = "ReplicationResponse !",
    [tReplicationAppend] = "ReplicationAppend !",
    [tReplicationAppendTx] = "ReplicationAppendTx !",
    [tReplicationDelete] = "ReplicationDelete !",
    [tReplicationDeleteTx] = "ReplicationDeleteTx !",
    [tReplicationPrepare] = "ReplicationPrepare !",
    [tReplicationCommitRollback] = "ReplicationCommitRollback !",
    [tReplicationPageWrite] = "ReplicationPageWrite !",
    [tReplicationPageEvent] = "ReplicationPageEvent !",
    [tReplicationLargeMessageBegin] = "ReplicationLargeMessageBegin !",
    [tReplicationLargeMessageEnd] = "ReplicationLargeMessageEnd !",
    [tReplicationLargeMessageWrite] = "ReplicationLargeMessageWrite !",
    [tReplicationCompareData] = "ReplicationCompareData !",
    [tReplicationSync] = "ReplicationSync !",
    [tSessAddMetadata] = "SessAddMetadata !",
    [tSessAddMetadata2] = "SessAddMetadata2 !",
    [tSessUniqueAddMetadata] = "SessUniqueAddMetadata !",
    [tClusterTopology] = "ClusterTopology !",
    [tNodeAnnounce] = "NodeAnnounce !",
    [tSubscribeTopology] = "SubscribeTopology !",
    [tSubscribeTopologyV2] = "SubscribeTopologyV2 !",
    [tClusterTopology_V2] = "ClusterTopology_V2 !",
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
        print("wantmore: got " .. bufferLength .. " want " .. hqPacketLength)
        -- return hqPacketLength
        return
    end
    print("processing: got " .. bufferLength .. " want " .. hqPacketLength)

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
    -- pinfo.cols.info = typeVal

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

    print("done: got " .. bufferLength .. " want " .. hqPacketLength .. " type " .. typeVal .. " offs " .. offset)

    if hqPacketLength < bufferLength then
        -- desegment if packet is smaller than buffer
        print("desegmenting")
        pinfo.desegment_offset = offset
        pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
    end

end

tcp_table = DissectorTable.get("tcp.port")
tcp_table:add(5445, HQPROTO)