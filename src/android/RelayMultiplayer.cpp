#include "RelayMultiplayer.h"
#include "net/MPInterface.h"

#include <cstring>
#include <chrono>
#include <android/log.h>

#define LOG_TAG "RelayMultiplayer"
#define LOGD(...) __android_log_print(ANDROID_LOG_DEBUG, LOG_TAG, __VA_ARGS__)

namespace melonDS
{

static const u32 kMagic = 0x4946494E; // "NIFI"

RelayMPInterface::PacketSentCallback RelayMPInterface::sCallback = nullptr;
std::mutex RelayMPInterface::sPacketMutex;
std::condition_variable RelayMPInterface::sPacketCv;
std::queue<std::vector<u8>> RelayMPInterface::sPacketQueue;
std::mutex RelayMPInterface::sReplyMutex;
std::condition_variable RelayMPInterface::sReplyCv;
std::queue<std::vector<u8>> RelayMPInterface::sReplyQueue;
int RelayMPInterface::sLastHostID = -1;

void RelayMPInterface::SetPacketSentCallback(PacketSentCallback callback)
{
    sCallback = callback;
}

void RelayMPInterface::InjectPacket(const u8* data, int len)
{
    if (len < (int)sizeof(MPPacketHeader)) return;

    const MPPacketHeader* hdr = reinterpret_cast<const MPPacketHeader*>(data);
    if (hdr->Magic != kMagic) return;

    std::vector<u8> buf(data, data + len);
    u32 type = hdr->Type & 0xFFFF;
    if (type == 2)
    {
        std::lock_guard<std::mutex> lock(sReplyMutex);
        // Drop oldest if queue is full to prevent latency spiral
        if (sReplyQueue.size() >= 16) sReplyQueue.pop();
        sReplyQueue.push(std::move(buf));
        sReplyCv.notify_one();
    }
    else
    {
        std::lock_guard<std::mutex> lock(sPacketMutex);
        if (sPacketQueue.size() >= 32) sPacketQueue.pop();
        sPacketQueue.push(std::move(buf));
        sPacketCv.notify_one();
    }
}

void RelayMPInterface::Begin(int inst)
{
    std::lock_guard<std::mutex> lock(sPacketMutex);
    while (!sPacketQueue.empty()) sPacketQueue.pop();
    LOGD("Begin inst=%d", inst);
}

void RelayMPInterface::End(int inst)
{
    {
        std::lock_guard<std::mutex> lock(sPacketMutex);
        while (!sPacketQueue.empty()) sPacketQueue.pop();
    }
    {
        std::lock_guard<std::mutex> lock(sReplyMutex);
        while (!sReplyQueue.empty()) sReplyQueue.pop();
    }
    sPacketCv.notify_all();
    sReplyCv.notify_all();
    LOGD("End inst=%d", inst);
}

void RelayMPInterface::SendPacketGeneric(int inst, u32 type, u8* packet, int len, u64 timestamp, u16 aid)
{
    if (!sCallback) return;

    int totalLen = sizeof(MPPacketHeader) + len;
    std::vector<u8> buf(totalLen);

    MPPacketHeader* hdr = reinterpret_cast<MPPacketHeader*>(buf.data());
    hdr->Magic     = kMagic;
    hdr->SenderID  = (u32)inst;
    hdr->Type      = type | ((u32)aid << 16);
    hdr->Length    = (u32)len;
    hdr->Timestamp = timestamp;

    if (len > 0)
        memcpy(buf.data() + sizeof(MPPacketHeader), packet, len);

    sCallback(buf.data(), totalLen);
}

int RelayMPInterface::SendPacket(int inst, u8* data, int len, u64 timestamp)
{
    SendPacketGeneric(inst, 0, data, len, timestamp);
    return len;
}

int RelayMPInterface::SendCmd(int inst, u8* data, int len, u64 timestamp)
{
    SendPacketGeneric(inst, 1, data, len, timestamp);
    return len;
}

int RelayMPInterface::SendReply(int inst, u8* data, int len, u64 timestamp, u16 aid)
{
    SendPacketGeneric(inst, 2, data, len, timestamp, aid);
    return len;
}

int RelayMPInterface::SendAck(int inst, u8* data, int len, u64 timestamp)
{
    SendPacketGeneric(inst, 3, data, len, timestamp);
    return len;
}

int RelayMPInterface::RecvFromQueue(std::mutex& mu, std::condition_variable& cv,
                                     std::queue<std::vector<u8>>& queue,
                                     u8* outData, u64* outTimestamp, bool block) const
{
    std::unique_lock<std::mutex> lock(mu);

    if (block)
    {
        int timeout = GetRecvTimeout();
        if (!cv.wait_for(lock, std::chrono::milliseconds(timeout),
                         [&]{ return !queue.empty(); }))
            return 0;
    }
    else
    {
        if (queue.empty()) return 0;
    }

    std::vector<u8> buf = std::move(queue.front());
    queue.pop();
    lock.unlock();

    const MPPacketHeader* hdr = reinterpret_cast<const MPPacketHeader*>(buf.data());
    int payloadLen = (int)hdr->Length;

    if ((hdr->Type & 0xFFFF) == 1)
        sLastHostID = (int)hdr->SenderID;

    if (outTimestamp) *outTimestamp = hdr->Timestamp;
    if (payloadLen > 0)
        memcpy(outData, buf.data() + sizeof(MPPacketHeader), payloadLen);

    return payloadLen;
}

int RelayMPInterface::RecvPacket(int inst, u8* data, u64* timestamp)
{
    return RecvFromQueue(sPacketMutex, sPacketCv, sPacketQueue, data, timestamp, false);
}

int RelayMPInterface::RecvHostPacket(int inst, u8* data, u64* timestamp)
{
    return RecvFromQueue(sPacketMutex, sPacketCv, sPacketQueue, data, timestamp, true);
}

u16 RelayMPInterface::RecvReplies(int inst, u8* packets, u64 timestamp, u16 aidmask)
{
    u16 ret = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(GetRecvTimeout());

    while (ret != aidmask)
    {
        std::unique_lock<std::mutex> lock(sReplyMutex);
        if (!sReplyCv.wait_until(lock, deadline, [&]{ return !sReplyQueue.empty(); }))
            break;

        std::vector<u8> buf = std::move(sReplyQueue.front());
        sReplyQueue.pop();
        lock.unlock();

        const MPPacketHeader* hdr = reinterpret_cast<const MPPacketHeader*>(buf.data());
        if (hdr->Magic != kMagic) continue;

        u32 aid = hdr->Type >> 16;
        int payloadLen = (int)hdr->Length;

        if (aid > 0 && payloadLen > 0)
        {
            memcpy(&packets[(aid - 1) * 1024], buf.data() + sizeof(MPPacketHeader), payloadLen);
            ret |= (1 << aid);
        }
    }

    return ret;
}

}
