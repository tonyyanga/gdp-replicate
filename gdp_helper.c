#include "gdp_types.h"

void bridgeMsgCallbackFunc(MsgCallbackFunc f, PeerAddr peer, Msg msg) {
    f(peer, msg);
}
