/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#ifndef __BUF_MANAGER__
#define __BUF_MANAGER__

#include "DBTypes.h" /* for KeyType, LockControlBlock and ThreadOpenIndexes */

/* Forward Declarations */
struct TNode;
struct TNodeInt;
struct TNodeShort;
struct Stack;
struct LockControlBlock;
struct ThreadOpenIndexes;
union  KeyVal;

/*forward declaration of stack_pop() */
void   stack_init(struct Stack ** stack);
void * stack_pop (struct Stack * stack);
void   stack_push(struct Stack * stack, void * data);


typedef struct BufferManager BufferManager;
typedef struct TNodeBuf      TNodeBuf;
typedef struct TNodeShortBuf TNodeShortBuf;
typedef struct TNodeIntBuf   TNodeIntBuf;
typedef struct PayloadBuf    PayloadBuf;
typedef struct LCB_Buf       LCB_Buf;
typedef struct AbortPool     AbortPool;
typedef struct XactLCBPool   XactLCBPool;


typedef char Payload[PAYLOAD_ALLOC_SIZE];

struct BufferManager
{
    void *             tnode_buf;
    PayloadBuf *       payload_buf;
    struct Stack *     free_list;
    LCB_Buf *          lcb_buf;
    struct Stack *     free_lcb_list;
    struct AbortPool * abortPool;
    struct XactLCBPool * xactLCBPool;
    /*functions*/
    void * (*makeTNode)(BufferManager *);
    void * (*allocator)(void);
    /*int (*comparator)(const union KeyVal *, const union KeyVal *);*/
};


struct AbortPool
{
    struct ThreadOpenIndexes threadOpenIdx[NUM_PENDING_LISTS];
    struct Stack       *freeList;
    struct AbortPool *  next;
    int                 curr;
    /*int size; NUM_PENDING_LISTS*/
};

struct XactLCBPool
{
    struct XactLCBs       xactLCBs[NUM_PENDING_LISTS_XACTLCB];
    struct Stack       *  freeList;
    struct XactLCBPool *  next;
    int                   curr;
};


struct LCB_Buf
{
    struct LockControlBlock ** buffer;
    struct LCB_Buf *           next_buf;
    
    int num_allocated_buffers;
    int num_buffers;
    int curr_buf;
    int curr_buf_lcb;
};

struct TNodeIntBuf
{
    struct TNodeInt ** buffer;
    struct TNodeIntBuf * next_buf;
    
    int num_allocated_buffers;
    int num_buffers;
    int curr_buf;
    int curr_buf_tnode;
};

struct TNodeShortBuf
{
    struct TNodeShort ** buffer;
    struct TNodeShortBuf * next_buf;
    
    int num_allocated_buffers;
    int num_buffers;
    int curr_buf;
    int curr_buf_tnode;
};

struct TNodeBuf
{
    struct TNode ** buffer;
    struct TNodeBuf * next_buf;
    
    int num_allocated_buffers;
    int num_buffers;
    int curr_buf;
    int curr_buf_tnode;
};

struct PayloadBuf
{
    char ** buffer;
    struct PayloadBuf * next_buf;
    
    int num_allocated_buffers;
    int num_buffers;
    int curr_buf;
    int curr_buf_payload;
};

void * makeTNodeShort(BufferManager * buf_man );
void * makeTNodeInt(BufferManager * buf_man );
void * makeTNodeVarchar(BufferManager * buf_man);

void *       allocTNodeBuf(void);
void *       allocTNodeShortBuf(void);
void *       allocTNodeIntBuf(void);
PayloadBuf * allocPayloadBuf(const KeyType type);
LCB_Buf *    allocLCB_Buf(void);
struct LockControlBlock * getLCBfromBuffer(BufferManager * buf_man);

AbortPool * createAbortPool(void);
inline void releaseToAbortPool(BufferManager * buf_man, struct ThreadOpenIndexes * item);
struct ThreadOpenIndexes * allocateFromAbortPool(BufferManager * buf_man);

/*
XactLCBs *    getXactLCBfromPool(BufferManager * buf_man);
XactLCBPool * createXactLCBPool(void);
inline void   releaseToXactLCBPool(BufferManager * buf_man, XactLCBs * xlcb);
*/

void * (*allocateBuffer[3])(void);
void * (*makeTNode[3])(BufferManager * buf_man);

#endif
