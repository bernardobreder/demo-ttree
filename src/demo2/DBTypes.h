/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#ifndef __DB_TYPES__
#define __DB_TYPES__

#include <pthread.h> /* for pthread_mutex_t and pthread_cond_t */
#include "Types.h" /* for KeyVal and KeyType */

/* Forward declarations*/
struct Stack;
struct BufferManager;

/* Type definitions */
typedef struct IndexData         IndexData;
typedef struct IDXState          IDXState;
typedef struct XACTState         XACTState;
typedef struct XactPendingList   XactPendingList;
typedef struct LockControlBlock  LockControlBlock;
typedef struct ThreadOpenIndexes ThreadOpenIndexes;
typedef struct XactLCBs          XactLCBs;

struct IndexData
{
    /*   pthread_mutex_t           slock;*/
    pthread_mutex_t           xlock;
    pthread_mutex_t           lcb_lock;
    struct BufferManager *    buffer_manager;
    char *                    name;
    void *                    index;
    struct IndexData *        next;
    struct LockControlBlock * lcbChain;
    struct LockControlBlock * lcbTail;
 
    /*functions*/
    /*int (*cmp)(const KeyVal *, const KeyVal *);*/
    int (*hasRecords)(const void *);
    ErrCode (*findRecord)(IDXState *, const KeyVal *, Record *);
    ErrCode (*insertItem)(IndexData *, struct RecordWrapper *, int);
    ErrCode (*getFirst)(IDXState *, Record *);
    ErrCode (*getNextRecord)(IDXState *, Record *);
    ErrCode (*removeRecord)(IndexData *, struct RecordWrapper *, XACTState *);

    KeyType type;
    /*    int numThreads;*/
    int isXlocked;
    int isSlocked;
    int hasWaiters;
};

struct LockControlBlock
{
    pthread_cond_t            cond_var;
    IndexData *               index;     
    int                       lockMode; /* 0--> S-lock, 1--> X-lock */
    int                       currLockStatus; /* 0-->granted, 1-->waiting */
    struct LockControlBlock * next;
    struct LockControlBlock * prev;
    int64_t                   xactId;
};

struct IDXState
{
    KeyVal         lastKey;    
    IndexData *    index;
    const char *   name;
    void *         lastTNode;
    struct Stack * payloadStack;
    KeyType        type;
    int            lastTNodeRecordIdx;
    int            keyNotFound;
};

struct XACTState
{
    XactLCBs *                 xactLCBs;
    XactLCBs *                 xactLCBsTail;
    struct ThreadOpenIndexes * openIndexesList;    
    int64_t                    xactId;
    int                        firstGetOrGetNext;
};

struct XactPendingList {
    struct AbortRecord list[INITIAL_PENDING_COUNT];
    int                currItem;
};

struct ThreadOpenIndexes
{
    IndexData *                index;
    struct ThreadOpenIndexes * next;
    struct ThreadOpenIndexes * pendingNext;
    struct XactPendingList     abortList;
};

struct XactLCBs
{
    LockControlBlock * lcb;
    XactLCBs *         next;
};

#endif
