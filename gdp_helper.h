#ifndef GDP_HELPER
#define GDP_HELPER

/* This file includes bridge functions and other helper functions for
 * the Go library. */

#include "gdp_types.h"

void bridgeMsgCallbackFunc(MsgCallbackFunc f, PeerAddr peer, Msg msg);

#endif
