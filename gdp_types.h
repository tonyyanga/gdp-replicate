#ifndef GDP_TYPES
#define GDP_TYPES

#include <stdint.h>

/* This file includes types for the C API
 * of gdp_replicate library */

typedef uint32_t PeerAddr;

/* LogSyncHandle allows access to global sync status of this log.
 * Allocation and release should be handled by Go */
typedef struct {
    uint32_t handleTicket;
} LogSyncHandle;

/* Msg defines the contents of a replication message
 * Data should be allocated by the caller, but released by the callee. */
typedef struct {
    uint32_t length;
    void* data;
} Msg;

/* MsgCallbackFunc should be implemented by the user of the
 * library to handle an outgoing replication message
 * Its implementation needs to release Msg. */
typedef void (*MsgCallbackFunc) (PeerAddr, Msg);

#endif
