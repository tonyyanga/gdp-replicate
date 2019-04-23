#include <iostream>
#include <assert.h>

#include "../gdp_replicate.h"

using namespace std;

int main() {
    const char log1_name[] = "log1.db";
    const char log2_name[] = "log2.db";

    PeerAddr peer;
    for (int i=0; i < 32; i++) {
        peer.addr[i] = 'a';
    }

    GoString log1;
    GoString log2;

    log1.n = 7;
    log2.n = 7;
    log1.p = log1_name;
    log2.p = log2_name;

    auto ret1 = CreateLogSyncHandle(log1);
    assert(ret1.r1 == 0);

    auto ret2 = CreateLogSyncHandle(log2);
    assert(ret2.r1 == 0);

    auto handle1 = ret1.r0;
    auto handle2 = ret2.r0;

    // begin sync

    auto ret3 = InitSync(handle1, peer);
    assert(ret3.r1 == 0);

    cout<<"InitSync succeeds"<<endl;

    auto ret4 = HandleMsg(handle2, peer, ret3.r0);
    assert(ret4.r1 == 0);

    cout<<"HandleMsg succeeds"<<endl;

    auto ret5 = HandleMsg(handle1, peer, ret4.r0);
    assert(ret5.r1 == 0);

    auto ret6 = HandleMsg(handle2, peer, ret5.r0);
    assert(ret6.r1 == 0);

    auto ret7 = HandleMsg(handle1, peer, ret6.r0);
    assert(ret7.r1 == -1);

    auto msg1 = ret3.r0;
    auto msg2 = ret4.r0;
    auto msg3 = ret5.r0;
    auto msg4 = ret6.r0;

    cout<<"MSG 1 length = "<<msg1.length<<endl;
    cout<<"MSG 2 length = "<<msg2.length<<endl;
    cout<<"MSG 3 length = "<<msg3.length<<endl;
    cout<<"MSG 4 length = "<<msg4.length<<endl;

    free(msg1.data);
    free(msg2.data);
    free(msg3.data);
    free(msg4.data);

    return 0;
}

