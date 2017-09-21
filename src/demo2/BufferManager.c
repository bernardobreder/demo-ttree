/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#include <stdio.h>
#include <stdlib.h>

#include "BufferManager.h"


void * (*allocateBuffer[3])(void) =
{
    allocTNodeShortBuf,
    allocTNodeIntBuf,
    allocTNodeBuf
};

void * (*makeTNode[3])(BufferManager * buf_man) =
{
    makeTNodeShort,
    makeTNodeInt,
    makeTNodeVarchar
};

void * allocTNodeShortBuf(void)
{
    TNodeShortBuf * tnode_buf = (TNodeShortBuf*)malloc(sizeof(TNodeShortBuf));
    void * mem;
    int ret;

    
    tnode_buf->buffer = (TNodeShort**)malloc( INIT_NUM_BUFS*sizeof(TNodeShort*) );
    ret = posix_memalign (&mem, ALIGN_SIZE, NUM_TNODE_PER_BUF * sizeof(TNodeShort));
    if(ret)
        fprintf(stderr, "Memory allocation failure in allocTNodeShortBuf err=%d\n",ret);
    

    tnode_buf->buffer[0] = (TNodeShort*)mem;/*malloc( NUM_TNODE_PER_BUF*sizeof(TNodeShort) );*/
    tnode_buf->num_buffers = INIT_NUM_BUFS;
    tnode_buf->num_allocated_buffers = 1;
    tnode_buf->curr_buf = 0;
    tnode_buf->curr_buf_tnode = 0;
    tnode_buf->next_buf = NULL;
    return tnode_buf;
}

void * allocTNodeIntBuf(void)
{
    TNodeIntBuf * tnode_buf = (TNodeIntBuf*)malloc(sizeof(TNodeIntBuf));
    void * mem;
    int ret;
    
    tnode_buf->buffer = (TNodeInt**)malloc( INIT_NUM_BUFS*sizeof(TNodeInt*) );
    ret = posix_memalign (&mem, ALIGN_SIZE, NUM_TNODE_PER_BUF * sizeof(TNodeInt));
    if(ret)
        fprintf(stderr, "Memory allocation failure in allocTNodeIntBuf err=%d\n",ret);

    tnode_buf->buffer[0] = (TNodeInt*)mem;
    /*tnode_buf->buffer[0] = (TNodeInt*)malloc( NUM_TNODE_PER_BUF*sizeof(TNodeInt) );*/
    tnode_buf->num_buffers = INIT_NUM_BUFS;
    tnode_buf->num_allocated_buffers = 1;
    tnode_buf->curr_buf = 0;
    tnode_buf->curr_buf_tnode = 0;
    tnode_buf->next_buf = NULL;
    return tnode_buf;
}

void * allocTNodeBuf(void)
{
    TNodeBuf * tnode_buf = (TNodeBuf*)malloc(sizeof(TNodeBuf));
    /*    int ret;
    void * mem;

    ret = posix_memalign (&mem, ALIGN_SIZE, NUM_TNODE_PER_BUF * sizeof(TNode));
    if(ret)
        fprintf(stderr, "Memory allocation failure in makeTNodeVarchar\n");
    */
    tnode_buf->buffer = (TNode**)malloc( INIT_NUM_BUFS*sizeof(TNode*) );
    tnode_buf->buffer[0] = (TNode*)malloc( NUM_TNODE_PER_BUF*sizeof(TNode) );
    tnode_buf->num_buffers = INIT_NUM_BUFS;
    tnode_buf->num_allocated_buffers = 1;
    tnode_buf->curr_buf = 0;
    tnode_buf->curr_buf_tnode = 0;
    tnode_buf->next_buf = NULL;
    return tnode_buf;
}

PayloadBuf * allocPayloadBuf(const KeyType type)
{
    PayloadBuf * payload_buf = (PayloadBuf*)malloc(sizeof(PayloadBuf));
    void * mem;
    int size;
    int ret;

    switch(type)
    {
      case SHORT:
          size = PAYLOAD_SHORTBUF_SIZE;          
          break;
      case INT:
          size = PAYLOAD_INTBUF_SIZE;
          break;
      default:
          size = PAYLOAD_BUF_SIZE;          
          break;
    }
    /*    
    if(type == SHORT)
        size = PAYLOAD_SHORTBUF_SIZE;          
    else if(type == INT)
        size = PAYLOAD_INTBUF_SIZE;
    else
        size = PAYLOAD_BUF_SIZE;          
    */

    payload_buf->buffer = (char**)malloc( INIT_NUM_PAYLOAD_BUFS * sizeof(char*) );    
    ret = posix_memalign(&mem, ALIGN_SIZE, size * sizeof(char));
    if(ret)
        fprintf(stderr, "Memory allocation failure in allocPayloadBuf\n");
    
    payload_buf->buffer[0] = (char*)mem;/*(char*)malloc( size * sizeof(char) );*/
    payload_buf->num_buffers = INIT_NUM_PAYLOAD_BUFS;
    payload_buf->num_allocated_buffers = 1;
    payload_buf->curr_buf = 0;
    payload_buf->curr_buf_payload = 0;
    payload_buf->next_buf = NULL;
    return payload_buf;
}

LCB_Buf * allocLCB_Buf(void)
{
    LCB_Buf * lcb_buf = (LCB_Buf*)malloc(sizeof(LCB_Buf));
    
    lcb_buf->buffer = (LockControlBlock**)malloc( INIT_NUM_LCB_BUFS * sizeof(LockControlBlock*) );
    
    lcb_buf->buffer[0] = (LockControlBlock*)malloc(NUM_LCB_PER_BUF * sizeof(LockControlBlock));
    lcb_buf->num_buffers = INIT_NUM_LCB_BUFS;
    lcb_buf->num_allocated_buffers = 1;
    lcb_buf->curr_buf = 0;
    lcb_buf->curr_buf_lcb = 0;
    lcb_buf->next_buf = NULL;
    return lcb_buf;
}

LockControlBlock * getLCBfromBuffer(BufferManager * buf_man)
{
    LockControlBlock * lcb;

    if( (lcb = stack_pop(buf_man->free_lcb_list)) )
        return lcb;
    else
    {
        LCB_Buf * lcb_buf = buf_man->lcb_buf;

        while(lcb_buf->num_allocated_buffers == lcb_buf->num_buffers)
        {
            if( lcb_buf->next_buf )
                lcb_buf = lcb_buf->next_buf;
            else
            {
                lcb_buf->next_buf = allocLCB_Buf();
                lcb_buf = lcb_buf->next_buf;
                break;
            }
        }
        
        if( lcb_buf->curr_buf_lcb == NUM_LCB_PER_BUF )
        {
            lcb_buf->curr_buf_lcb = 0;
            lcb_buf->curr_buf++;
            if(lcb_buf->curr_buf == lcb_buf->num_allocated_buffers)
            {

                lcb_buf->buffer[lcb_buf->num_allocated_buffers++] = (LockControlBlock*)malloc(NUM_LCB_PER_BUF * sizeof(LockControlBlock));
            }
        }
        
        lcb = *(lcb_buf->buffer + lcb_buf->curr_buf) + lcb_buf->curr_buf_lcb++;
        return lcb;
    }
}

void * makeTNodeVarchar(BufferManager * buf_man )
{
    TNode * newNode;
    Payload * next_payload_buf;
    int fromFreeList = 0;
	register int i;

    if( (newNode = stack_pop(buf_man->free_list)) )
        fromFreeList = 1;
    else
    {
        TNodeBuf * tnode_buf = buf_man->tnode_buf;

        while(tnode_buf->num_allocated_buffers == tnode_buf->num_buffers)
        {
            if( tnode_buf->next_buf )
                tnode_buf = tnode_buf->next_buf;
            else
            {
                tnode_buf->next_buf = allocTNodeBuf();
                tnode_buf = tnode_buf->next_buf;
                break;
            }
        }
        
        if( tnode_buf->curr_buf_tnode == NUM_TNODE_PER_BUF )
        {
            tnode_buf->curr_buf_tnode = 0;
            tnode_buf->curr_buf++;            
            if(tnode_buf->curr_buf == tnode_buf->num_allocated_buffers)
            {
                /*
                int ret;
                void * mem;

                ret = posix_memalign (&mem, ALIGN_SIZE, NUM_TNODE_PER_BUF * sizeof(TNode));
                if(ret)
                    fprintf(stderr, "Memory allocation failure in makeTNodeVarchar\n");
                */
                tnode_buf->buffer[tnode_buf->num_allocated_buffers++] = (TNode*)malloc( NUM_TNODE_PER_BUF * sizeof(TNode) );
            }
        }
    
        newNode = *(tnode_buf->buffer + tnode_buf->curr_buf) + tnode_buf->curr_buf_tnode++;
    }
    
	newNode->left = newNode->right = newNode->parent = NULL;
	newNode->currSize = 0;
	newNode->balance = 0;
	newNode->height = 0;

    if(!fromFreeList)
    {
        PayloadBuf * payload_buf = buf_man->payload_buf;

        while(payload_buf->num_allocated_buffers == payload_buf->num_buffers)
        {
            if( payload_buf->next_buf )
                payload_buf = payload_buf->next_buf;
            else
            {
                payload_buf->next_buf = allocPayloadBuf(VARCHAR);
                payload_buf = payload_buf->next_buf;
                break;
            }
        }

        
        if( payload_buf->curr_buf_payload == PAYLOAD_BUF_SIZE )
        {
            payload_buf->curr_buf_payload = 0;
            payload_buf->curr_buf++;  
            if(payload_buf->curr_buf == payload_buf->num_allocated_buffers)
            {
                int ret;
                void * mem;
            
                ret = posix_memalign(&mem, ALIGN_SIZE,  PAYLOAD_BUF_SIZE * sizeof(char));
                if(ret)
                    fprintf(stderr, "Memory allocation failure in makeTNode\n");

                payload_buf->buffer[payload_buf->num_allocated_buffers++] = (char*)mem;
            }
        }
        
        next_payload_buf = (Payload*)(*(payload_buf->buffer + payload_buf->curr_buf) + payload_buf->curr_buf_payload);
        payload_buf->curr_buf_payload += TNODE_SIZE*PAYLOAD_ALLOC_SIZE;
        
        for(i=0; i < TNODE_SIZE; i++)
            newNode->data[i].payload = (char*)next_payload_buf++;
    }
    
    for(i=TNODE_SIZE-1; i>=0; i--)
        newNode->data[i].payloadTree = NULL;

	return newNode;
}

void * makeTNodeShort(BufferManager * buf_man )
{
    TNodeShort * newNode;
    char * next_payload_buf;
    PayloadWrapper * payloadTrees;
    Payload * payloadBufs;
    int fromFreeList = 0;
	register int i;

    if( (newNode = stack_pop(buf_man->free_list)) )
        fromFreeList = 1;
    else
    {
        TNodeShortBuf * tnode_buf = buf_man->tnode_buf;

        while(tnode_buf->num_allocated_buffers == tnode_buf->num_buffers)
        {
            if( tnode_buf->next_buf )
                tnode_buf = tnode_buf->next_buf;
            else
            {
                tnode_buf->next_buf = allocTNodeShortBuf();
                tnode_buf = tnode_buf->next_buf;
                break;
            }
        }
        
        if( tnode_buf->curr_buf_tnode == NUM_TNODE_PER_BUF )
        {
            tnode_buf->curr_buf_tnode = 0;
            tnode_buf->curr_buf++;            
            if(tnode_buf->curr_buf == tnode_buf->num_allocated_buffers)
            {
                int ret;
                void * mem;

                ret = posix_memalign (&mem, ALIGN_SIZE, NUM_TNODE_PER_BUF * sizeof(TNodeShort));
                if(ret)
                    fprintf(stderr, "Memory allocation failure in makeTNodeShort\n");

                tnode_buf->buffer[tnode_buf->num_allocated_buffers++] = (TNodeShort*)mem;
            }
        }
        
        newNode = *(tnode_buf->buffer + tnode_buf->curr_buf) + tnode_buf->curr_buf_tnode++;
    }

	newNode->left = newNode->right = newNode->parent = NULL;
	newNode->currSize = 0;
	newNode->balance = 0;
	newNode->height = 0;
    
    payloadTrees = newNode->payloads;

    if( !fromFreeList )
    {
        PayloadBuf * payload_buf = buf_man->payload_buf;

        while(payload_buf->num_allocated_buffers == payload_buf->num_buffers)
        {
            if( payload_buf->next_buf )
                payload_buf = payload_buf->next_buf;
            else
            {
                payload_buf->next_buf = allocPayloadBuf(SHORT);
                payload_buf = payload_buf->next_buf;
                break;
            }
        }
        
        if( payload_buf->curr_buf_payload == PAYLOAD_SHORTBUF_SIZE )
        {
            payload_buf->curr_buf_payload = 0;
            payload_buf->curr_buf++;            
            if(payload_buf->curr_buf == payload_buf->num_allocated_buffers)
            {
                void * mem;
                int ret;
            
                ret = posix_memalign(&mem, ALIGN_SIZE,  PAYLOAD_SHORTBUF_SIZE * sizeof(char));
                if(ret)
                    fprintf(stderr, "Memory allocation failure in makeTNodeShort\n");
    
                payload_buf->buffer[payload_buf->num_allocated_buffers++] = (char*)mem;
            }
        }
        
        next_payload_buf = *(payload_buf->buffer + payload_buf->curr_buf) + payload_buf->curr_buf_payload;
        payload_buf->curr_buf_payload += (SHORT_TNODE_SIZE+1)*PAYLOAD_ALLOC_SIZE+SHORT_PAYLOAD_OFFSET;

        newNode->payloads = (PayloadWrapper*)next_payload_buf;
        payloadTrees = newNode->payloads;
        payloadBufs = (Payload*)(next_payload_buf+PAYLOAD_ALLOC_SIZE+SHORT_PAYLOAD_OFFSET);

        for(i=0; i < SHORT_TNODE_SIZE; i++)
            payloadTrees[i].payload = (char*)payloadBufs++;
    }

    for(i=0; i < SHORT_TNODE_SIZE; i++)
        payloadTrees[i].payloadTree = NULL;

	return newNode;
}

void * makeTNodeInt(BufferManager * buf_man )
{
    TNodeInt * newNode;
    char * next_payload_buf;
    PayloadWrapper * payloadTrees;
    Payload * payloadBufs;
    int fromFreeList = 0;
    
	register int i;

    if( (newNode = stack_pop(buf_man->free_list)) )
        fromFreeList = 1;
    else
    {
        TNodeIntBuf * tnode_buf = buf_man->tnode_buf;        

        while(tnode_buf->num_allocated_buffers == tnode_buf->num_buffers)
        {
            if( tnode_buf->next_buf )
                tnode_buf = tnode_buf->next_buf;
            else
            {
                tnode_buf->next_buf = allocTNodeIntBuf();
                tnode_buf = tnode_buf->next_buf;                
                break;
            }
        }

        if( tnode_buf->curr_buf_tnode == NUM_TNODE_PER_BUF )
        {
            tnode_buf->curr_buf_tnode = 0; 
            tnode_buf->curr_buf++;
            if(tnode_buf->curr_buf == tnode_buf->num_allocated_buffers)
            {
                int ret;
                void * mem;

                ret = posix_memalign (&mem, ALIGN_SIZE, NUM_TNODE_PER_BUF * sizeof(TNodeInt));
                if(ret)
                    fprintf(stderr, "Memory allocation failure in makeTNodeInt\n");
                tnode_buf->buffer[tnode_buf->num_allocated_buffers++] = (TNodeInt*)mem;
            }
        }
        newNode = *(tnode_buf->buffer + tnode_buf->curr_buf) + tnode_buf->curr_buf_tnode++;
    }

	newNode->left = newNode->right = newNode->parent = NULL;
	newNode->currSize = 0;
	newNode->balance = 0;
	newNode->height = 0;

    payloadTrees = newNode->payloads;

    if(!fromFreeList)
    {
        PayloadBuf * payload_buf = buf_man->payload_buf;

        while(payload_buf->num_allocated_buffers == payload_buf->num_buffers)
        {
            if( payload_buf->next_buf )
                payload_buf = payload_buf->next_buf;
            else
            {
                payload_buf->next_buf = allocPayloadBuf(INT);
                payload_buf = payload_buf->next_buf;                
                break;
            }
        }

        if( payload_buf->curr_buf_payload == PAYLOAD_INTBUF_SIZE )
        {
            payload_buf->curr_buf_payload = 0;
            payload_buf->curr_buf++;            
            if(payload_buf->curr_buf == payload_buf->num_allocated_buffers)
            {
                void * mem;
                int ret;
            
                ret = posix_memalign(&mem, ALIGN_SIZE,  PAYLOAD_INTBUF_SIZE * sizeof(char));
                if(ret)
                    fprintf(stderr, "Memory allocation failure in makeTNodeInt\n");
            
                payload_buf->buffer[payload_buf->num_allocated_buffers++] = (char*)mem;
            }
        }

        next_payload_buf = *(payload_buf->buffer + payload_buf->curr_buf) + payload_buf->curr_buf_payload;
        payload_buf->curr_buf_payload += (INT_TNODE_SIZE+1)*PAYLOAD_ALLOC_SIZE+INT_PAYLOAD_OFFSET;

        newNode->payloads = (PayloadWrapper*)next_payload_buf;
        payloadTrees = newNode->payloads;
        payloadBufs = (Payload*)(next_payload_buf+PAYLOAD_ALLOC_SIZE+INT_PAYLOAD_OFFSET);

        for(i=0; i < INT_TNODE_SIZE; i++)
            payloadTrees[i].payload = (char*)payloadBufs++;
    }
    
    for(i=0; i < INT_TNODE_SIZE; i++)
        payloadTrees[i].payloadTree = NULL;

	return newNode;
}

ThreadOpenIndexes * allocateFromAbortPool(BufferManager * buf_man)
{
    ThreadOpenIndexes * item;
    AbortPool * thisPool = buf_man->abortPool;
    
    if( !( item = stack_pop(thisPool->freeList) ) )
    {
        while( thisPool->curr == NUM_PENDING_LISTS )
        {
            if(thisPool->next)
                thisPool = thisPool->next;
            else
            {
                thisPool->next = createAbortPool();
                thisPool = thisPool->next;
                break;
            }
        }

        item = thisPool->threadOpenIdx+thisPool->curr;
        thisPool->curr++;
    }

    return item;
}

AbortPool * createAbortPool(void)
{
    AbortPool * pool = (AbortPool*)malloc(sizeof(AbortPool));

    stack_init(&pool->freeList);
    pool->next = NULL;
    pool->curr = 0;

    return pool;
}

inline void releaseToAbortPool(BufferManager * buf_man, ThreadOpenIndexes * item)
{
    stack_push( buf_man->abortPool->freeList, item );
}

/*
XactLCBs *    getXactLCBfromPool(BufferManager * buf_man)
{
    XactLCBs * item;
    XactLCBPool * thisPool = buf_man->xactLCBPool;
    
    if( !( item = stack_pop(thisPool->freeList) ) )
    {
        while( thisPool->curr == NUM_PENDING_LISTS_XACTLCB )
        {
            if(thisPool->next)
                thisPool = thisPool->next;
            else
            {
                thisPool->next = createAbortPool();
                thisPool = thisPool->next;
                break;
            }
        }

        item = thisPool->xactLCBs + thisPool->curr;
        thisPool->curr++;
    }

    return item;
    
}

inline void releaseToXactLCBPool(BufferManager * buf_man, XactLCBs * xlcb)
{
    stack_push( buf_man->xactLCBPool->freeList, xlcb );
}

XactLCBPool * createXactLCBPool(void)
{
    XactLCBPool * pool = (XactLCBPool*)malloc(sizeof(XactLCBPool));

    stack_init(&pool->freeList);
    pool->next = NULL;
    pool->curr = 0;

    return pool;
}
*/
