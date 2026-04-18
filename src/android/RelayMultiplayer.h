#ifndef RELAYMULTIPLAYER_H
#define RELAYMULTIPLAYER_H

#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include "net/MPInterface.h"

namespace melonDS
{

class RelayMPInterface : public MPInterface
{
public:
    typedef void (*PacketSentCallback)(const u8* data, int len);

    static void SetPacketSentCallback(PacketSentCallback callback);
    static void InjectPacket(const u8* data, int len);

    void Process() override {}

    void Begin(int inst) override;
    void End(int inst) override;

    int SendPacket(int inst, u8* data, int len, u64 timestamp) override;
    int RecvPacket(int inst, u8* data, u64* timestamp) override;
    int SendCmd(int inst, u8* data, int len, u64 timestamp) override;
    int SendReply(int inst, u8* data, int len, u64 timestamp, u16 aid) override;
    int SendAck(int inst, u8* data, int len, u64 timestamp) override;
    int RecvHostPacket(int inst, u8* data, u64* timestamp) override;
    u16 RecvReplies(int inst, u8* data, u64 timestamp, u16 aidmask) override;

private:
    static PacketSentCallback sCallback;

    static std::mutex sPacketMutex;
    static std::condition_variable sPacketCv;
    static std::queue<std::vector<u8>> sPacketQueue;

    static std::mutex sReplyMutex;
    static std::condition_variable sReplyCv;
    static std::queue<std::vector<u8>> sReplyQueue;

    static int sLastHostID;

    void SendPacketGeneric(int inst, u32 type, u8* packet, int len, u64 timestamp, u16 aid = 0);
    int RecvFromQueue(std::mutex& mu, std::condition_variable& cv,
                      std::queue<std::vector<u8>>& queue,
                      u8* outData, u64* outTimestamp, bool block) const;
};

}

#endif // RELAYMULTIPLAYER_H
