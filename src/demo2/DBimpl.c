/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

#include "TTree.h"
#include "BufferManager.h"

/*************** DEFINES ***************/
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define HASH_CONST 1019

#define DEBUG 0
#define PAYLOAD_COPY(DEST, SRC) strcpy(DEST, SRC) /* MAX_PAYLOAD_LEN+1 */
/* #define LOG(MSG,XACT,IDX) prinft(MSG,XACT,IDX) */
/* #define free(x)  */

#define LOG(MSG,XACT,IDX)
#define RELEASE(MTX) pthread_mutex_unlock(&MTX)
#define GET_MUTEX(RET, MTX, MSG)                    \
    if ((RET = pthread_mutex_lock(&MTX)) != 0)      \
        printf("Error in getting mutex: %s\n", MSG) \


/***** forward decleration of stack methods ******/
struct Stack;
inline void   stack_push(struct Stack * stack, void * data);
void   stack_init(struct Stack ** stack);

/******************** GLOBALS ********************/    
IndexData * indexes              = NULL;
IndexData * indexNameTable[1024] = {0};
int64_t     NEXT_XACT_ID         = 0; /*not used*/

/******************** LOCKS **********************/
pthread_mutex_t IDX_CREATE_LOCK     = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t IDX_OPEN_CLOSE_LOCK = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t NEXT_XACT_ID_LOCK   = PTHREAD_MUTEX_INITIALIZER;


/******************** DECLERATIONS ***************/
int64_t getNextXactId();
void    freeIndexList(IndexData * _list);
void    addLCBtoXACT(XACTState * xactState, LockControlBlock * lcb);


/******************** FOR DEBUGGING **************/
void printIndexLCBs(IndexData * idx)
{
    LockControlBlock * lcbs = idx->lcbChain;

    printf("<INDEX=%s> ==> ", idx->name);
    while(lcbs)
    {
        printf("{[%s %s on %s][TXN-ID=%d]} - ",
               (lcbs->currLockStatus==0?"Granted":"Waiting"),
               (lcbs->lockMode==0?"S-lock":"X-lock"),
               (lcbs->index->name), (int)(lcbs->xactId) );
        lcbs = lcbs->next;
    }
    printf("\n");
}

void printXactLCBs(TxnState* xact)
{
    XactLCBs * lcbs = ((XACTState *)xact)->xactLCBs;

    printf("<TXN-ID=%d> ==> ", (int)((XACTState *)xact)->xactId);
    while(lcbs)
    {
        printf("{[%s %s on %s][TXN-ID=%d]} - ",
               (lcbs->lcb->currLockStatus==0?"Granted":"Waiting"),
               (lcbs->lcb->lockMode==0?"S-lock":"X-lock"),
               (lcbs->lcb->index->name), (int)(lcbs->lcb->xactId) );
        lcbs = lcbs->next;
    }
    printf("\n");
}
/******************** END of DEBUGGING ********************/

/*
inline int hasWaiters(IndexData * index)
{
    if(index->lcbChain==NULL)
        return 0;
    else
    {
        LockControlBlock * chain = index->lcbChain;
        while(chain)
        {
            if(chain->currLockStatus == 1)
                return 1;
            chain = chain->next;
        }
    }
    return 0;
}
*/
int anySlockOnIndex(IndexData * index, LockControlBlock * mylcb)
{
    LockControlBlock * chain;
    int                any = 0, ret;

    GET_MUTEX(ret, index->lcb_lock,  "@ anySlockOnIndex(index->lcb_lock)");
    chain = index->lcbChain;
    while(chain)
    {
        if( mylcb != chain &&
            chain->currLockStatus == 0 && chain->lockMode == 0 )
        {
            any = 1;
            break;
        }
        chain = chain->next;
    }

    RELEASE(index->lcb_lock);
    return any;
}


int hasTheLock(IndexData * index, XACTState * xactState, int exclusive)
{
    XactLCBs * myLCBs = xactState->xactLCBs;
    int        res    = 0, ret;
    
    while(myLCBs!=NULL)
    {
        if(myLCBs->lcb->index == index)
        {
            if( myLCBs->lcb->currLockStatus == 0 )/*already has it*/
                res = 1;
            
            if( res && exclusive && myLCBs->lcb->lockMode == 0 )
            {
                /* but not an x-lock*/
                if( anySlockOnIndex( index, myLCBs->lcb ) )
                    res = 0;
                else
                {
                    /* convert my s-lock to x-lock */
                    GET_MUTEX(ret, index->xlock, "@ hasTheLock(xlock)");
                    LOG("XACT[%d] converts S-lock to X-lock on %s\n",
                        (xactState?xactState->xactId:myLCBs->lcb->xactId),index->name);
                    
                    index->isXlocked = 1;
                    index->isSlocked = 0;
                    RELEASE(index->xlock);                    
                    GET_MUTEX(ret, index->lcb_lock,  "@ hasTheLock(index->lcb_lock)");
                    myLCBs->lcb->lockMode = 1;
                    RELEASE(index->lcb_lock);

                }
            }
            
            break;
        }
        myLCBs = myLCBs->next;
    }

    return res;
}

void addToPendingList(IndexData * id, XACTState * xactState,
                      RecordWrapper * recwrap, int del)
{
    ThreadOpenIndexes * oi = xactState->openIndexesList, * add;
    XactPendingList *   plist;
    AbortRecord *       abrec;
    
    while( oi!=NULL )
    {
        if( id == oi->index )
            break;
        oi = oi->next;
    }

    if( oi )
        add = oi;
    else
    {
        add = allocateFromAbortPool(id->buffer_manager);
        add->index = id;
        add->next = xactState->openIndexesList;
        add->pendingNext = NULL;
        add->abortList.currItem = 0;
        xactState->openIndexesList = add;
    }

    plist = &add->abortList;
    
    while( plist->currItem == INITIAL_PENDING_COUNT )
    {

        if(add->pendingNext)
            add = add->pendingNext;
        else
        {
            /* re-allocation required */
            add->pendingNext = allocateFromAbortPool(id->buffer_manager);
            add = add->pendingNext;
            add->index = id;
            add->next = NULL;
            add->pendingNext = NULL;
            add->abortList.currItem = 0;

            
            plist = &add->abortList;
            break;
        }
    }

    abrec = plist->list+plist->currItem;

    switch(id->type)
    {
      case SHORT:
          abrec->key.shortkey = recwrap->key->shortkey;          
          break;
      case INT:
          abrec->key.intkey = recwrap->key->intkey;
          break;
      default:
          abrec->key = *recwrap->key;
          break;
    }
    /*    
    if(id->type == VARCHAR)
        abrec->key = *recwrap->key;
    else if(id->type == SHORT)
        abrec->key.shortkey = recwrap->key->shortkey;
    else
        abrec->key.intkey = recwrap->key->intkey;
    */
    abrec->payloadTree = recwrap->payloadTree;
    PAYLOAD_COPY(abrec->payload, recwrap->payload);
    abrec->isDelete = del;

    plist->currItem++;
}

void freeIndexList(IndexData * _list)
{
    IndexData * temp, * list = _list;
    
    while(list)
    {
        temp = list->next;
        free(list->name);
        freeTree(list->index);
        free(list);
        list = temp;
    }
}


inline int64_t getNextXactId()
{
    static int64_t nextId = 0; 
    int            ret;

    GET_MUTEX(ret, NEXT_XACT_ID_LOCK, "@ getNextXactId()");
    /*nextId = ++NEXT_XACT_ID;*/
    nextId++;
    RELEASE(NEXT_XACT_ID_LOCK);
    
    return nextId;
}

void addLCBtoXACT(XACTState * xactState, LockControlBlock * lcb)
{
    XactLCBs * tmp = (XactLCBs*)malloc(sizeof(XactLCBs));
    /*XactLCBs * tmp = getXactLCBfromPool(lcb->index->buffer_manager);*/
    
    tmp->lcb = lcb;
    tmp->next = NULL;

    if(xactState->xactLCBs)
    {
        xactState->xactLCBsTail->next = tmp;
        xactState->xactLCBsTail = tmp;
    }
    else
        xactState->xactLCBs = xactState->xactLCBsTail = tmp;
}

/*
void testCorrectness()
{
    IndexData * index = indexes;
    int count = 0;
    
    while(index)
    {
        if(index->type == VARCHAR)
            checkTTree(index->index);
        else if(index->type == INT)
            checkTTree_int(index->index);
        else
            checkTTree_short(index->index);

        index = index->next;
        count++;
    }
    printf("NUMBER OF INDICES = %d\n", count);
}
*/
static inline unsigned long hash(const unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

ErrCode create(KeyType type, char * name)
{
    IndexData *     index;
    IndexData *     newIndex;
    BufferManager * buffer_manager;
    int             ret;
    int             hashVal;

    hashVal = hash((const unsigned char *)name)%HASH_CONST;
    if((newIndex = indexNameTable[hashVal]))
    {
        index = indexes;        
        if( strcmp(newIndex->name, name) != 0 )
        {
            while(index)
            {
                if( strcmp(name, index->name) == 0 )
                {
                    break;
                }
                else {
                    index = index->next;
                }
            }
        }

        if(index)
            return DB_EXISTS;
    }
    
    newIndex = (IndexData*)malloc(sizeof(IndexData));
    newIndex->name = strdup(name);

    /*set up BufferManager*/
    buffer_manager = (BufferManager*)malloc(sizeof(BufferManager));
    newIndex->buffer_manager = buffer_manager;
    
    /*initialize free list*/
    stack_init(&buffer_manager->free_list);
    
    /*allocate memory for the buffers*/
    /*buffer_manager->comparator = _keycmp[type];    */
    buffer_manager->allocator = allocateBuffer[type];
    buffer_manager->tnode_buf = allocateBuffer[type]();
    
    /*allocate memory for the buffers*/
    buffer_manager->payload_buf = allocPayloadBuf(type);
    buffer_manager->makeTNode   = makeTNode[type];
    newIndex->index = makeTNode[type](newIndex->buffer_manager);

    /*allocate memory for lcb buffers*/
    buffer_manager->lcb_buf = allocLCB_Buf();
    stack_init(&buffer_manager->free_lcb_list);

    /*allocate memory for Abort lists*/
    buffer_manager->abortPool = createAbortPool();

    /*allocate memory for XactLCBs 
      buffer_manager->xactLCBPool = createXactLCBPool();*/
    
    newIndex->next = NULL;
    newIndex->type = type;
    /*newIndex->cmp = _keycmp[type];*/
    /*newIndex->numThreads = 0;*/
    newIndex->lcbChain   = NULL;
    newIndex->lcbTail    = NULL;
    newIndex->isXlocked  = 0;
    newIndex->isSlocked  = 0;
    newIndex->hasWaiters = 0;
    
    /*set-up function pointers*/
    newIndex->hasRecords    = hasRecordsFunc[type];
    newIndex->findRecord    = findRecord[type];
    newIndex->insertItem    = insertItem[type];
    newIndex->getFirst      = getFirst[type];
    newIndex->getNextRecord = getNextRecord[type];
    newIndex->removeRecord  = removeRecord[type];
    
    if ((pthread_mutex_init(&newIndex->lcb_lock, NULL)) != 0) {
        printf("could not init pthread lcb_lock for index %s\n", newIndex->name);
    }
    if ((pthread_mutex_init(&newIndex->xlock, NULL)) != 0) {
        printf("could not init pthread xlock for index %s\n", newIndex->name);
    }
    /*    if ((pthread_mutex_init(&newIndex->slock, NULL)) != 0) {
            printf("could not init pthread slock for index %s\n", newIndex->name);
            }*/

    
    GET_MUTEX(ret, IDX_CREATE_LOCK, "@ create()");    

    indexNameTable[hashVal] = newIndex;
    index                   = indexes;    
    if(index)
    {
        newIndex->next = index;
        index          = newIndex;
    }
    else
        indexes = newIndex;

    RELEASE(IDX_CREATE_LOCK);
    
    return SUCCESS;
}

ErrCode openIndex(const char * name, IdxState ** idxState)
{
    int         ret;
    IndexData * index;
    IDXState *  state;
    
    ret = hash((const unsigned char*)name)%HASH_CONST;
    if(unlikely( !(index = indexNameTable[ret]) ))
        return DB_DNE;


    if( unlikely(strcmp(index->name, name) != 0) )
    {
        GET_MUTEX(ret, IDX_OPEN_CLOSE_LOCK, "@ openIndex()");
        index = indexes;        
        while(index)
        {
            if( strcmp(name, index->name) == 0 )
            {
                break;
            }
            index = index->next;
        }
        RELEASE(IDX_OPEN_CLOSE_LOCK);
    }

    
    /*index->numThreads++;*/
    state = (IDXState*)malloc(sizeof(IDXState));
    state->index              = index;
    state->type               = index->type;
    state->name               = index->name;
    state->keyNotFound        = 2; /* no get() or getNext() calls yet */
    state->lastTNode          = NULL;
    state->lastTNodeRecordIdx = 0;
    state->payloadStack       = NULL;
    *idxState = (IdxState*)state;    

    return SUCCESS;
}


ErrCode closeIndex(IdxState *idxState)
{
    IDXState *  state = (IDXState*)idxState;
    IndexData * index = state->index;

    if( unlikely(strcmp( index->name, state->name ) != 0) )
        return DB_DNE;
    
    /*index->numThreads--;*/
    free(state);
    
    return SUCCESS;
}

ErrCode commitSingle(IndexData * index, LockControlBlock * lcb)
{
    /* int lastLockMode = 0; */
    /* int anyOtherSlocks = 0; */
    LockControlBlock * lcbList, * lcbListPrev, *temp;
    int                myLockMode, myLockStatus, xactId;
    int                anySlockWakenUp = 0, anyXlockWakenUp = 0;
    int                ret;
    
    GET_MUTEX(ret, index->xlock, "@ commitSingle(xlock)");
    GET_MUTEX(ret, index->lcb_lock,  "@ commitSingle(index->lcb_lock)");

    myLockMode   = lcb->lockMode;
    myLockStatus = lcb->currLockStatus;
    xactId       = lcb->xactId;

    lcbList     = lcb->next;
    lcbListPrev = lcb->prev;
    temp        = lcb->prev;
    
    if(lcb->prev)
        lcb->prev->next = lcbList;
    else
    {
        index->lcbChain = lcbList;
        /* lcbList->prev = NULL; */
    }

    if(lcbList)
        lcbList->prev = lcb->prev;
    else
        index->lcbTail = lcb->prev;

    stack_push(index->buffer_manager->free_lcb_list, lcb);
    lcb = NULL;

    if(myLockStatus == 0)
    {
        if(likely(myLockMode)){
            LOG("XACT[%d] releases X-lock(sets = 0) on %s\n",xactId,index->name);
            index->isXlocked = 0;
            index->isSlocked = 0;
        }
        else
        {
            index->isSlocked = 0;/*it may change in the following loop*/            
            /*it was s-lock, now check if there are any xacts holding
              s-lock as well*/
            temp = index->lcbChain;

            while(likely(temp!=NULL))
            {
                if(temp->lockMode == 0 && temp->currLockStatus == 0)
                {
                    index->isSlocked = 1;
                    break;
                }
                temp = temp->next;
            }
        }

        if( index->isXlocked == 0)
        {
            /*TODO: this section may change */
            /*scan after the current lcb*/

            /*lcbList = index->lcbChain;*/
            
            while(lcbList)
            {
                if(lcbList->currLockStatus == 1)
                {
                    /* waiting for lock */
                    if(lcbList->lockMode == 0)
                    {
                        /*for s-lock, wake it up*/
                        anySlockWakenUp = 1;
                        pthread_cond_signal(&lcbList->cond_var);
                        LOG("Waking up XACT[%d] for s-lock %s\n", lcbList->xactId, index->name);
                    }
                    else if(anySlockWakenUp == 0 && index->isSlocked == 0)
                    {
                        /*for x-lock, wake it up if not s-locked already*/
                        anyXlockWakenUp = 1;
                        pthread_cond_signal(&lcbList->cond_var);
                        LOG("Waking up xact[%d] for x-lock on %s\n", lcbList->xactId, index->name);
                        break;/*only one can acquire x-lock*/
                    }
                }
        
                lcbList = lcbList->next;                
            }
            
            /*scan items before the current lcb*/
            if(anyXlockWakenUp==0)
            {
                while(lcbListPrev)
                {
                    if(lcbListPrev->currLockStatus == 1)
                    {
                        /* waiting for lock */
                        if(lcbListPrev->lockMode == 0)
                        {
                            /*for s-lock, wake it up*/
                            anySlockWakenUp = 1;
                            pthread_cond_signal(&lcbListPrev->cond_var);
                            LOG("Waking up XACT[%d] for s-lock %s\n", lcbListPrev->xactId, index->name);
                        }
                        else if(anySlockWakenUp == 0 && index->isSlocked == 0)
                        {
                            /*for x-lock, wake it up if not s-locked already*/
                            /*anyXlockWakenUp = 0;*/
                            pthread_cond_signal(&lcbListPrev->cond_var);
                            LOG("Waking up xact[%d] for x-lock on %s\n", lcbListPrev->xactId, index->name);
                            break;/*only one can acquire x-lock*/
                        }
                    }
        
                    lcbListPrev = lcbListPrev->prev;                
                }
            }
        }
    }
    RELEASE(index->lcb_lock);
    RELEASE(index->xlock);

    return SUCCESS;/* what else may it return? */

}

ErrCode beginTransaction(TxnState **txn)
{
    XACTState * xactState;

    
    xactState = (XACTState*)malloc(sizeof(XACTState));
    xactState->xactId            = getNextXactId();
    xactState->xactLCBs          = NULL;
    xactState->openIndexesList   = NULL;
    xactState->firstGetOrGetNext = 1;
    
    *txn = (TxnState*)xactState;

    return SUCCESS;
}

ErrCode abortTransaction(TxnState *txn)
{
    XACTState * xactState = (XACTState*)txn;
    XactLCBs *  tmp       = xactState->xactLCBs, *tmp2;
    
    /* go over the insert list and remove them from the index */
    ThreadOpenIndexes * oi = xactState->openIndexesList, *oi2;

    while( oi )
    {
        XactPendingList * toRollback = &oi->abortList;

        rollbackDB( oi->index, toRollback->list, toRollback->currItem, oi->index->buffer_manager );

        oi2 = oi->next;
        
        releaseToAbortPool(oi->index->buffer_manager, oi);
        
        oi = oi2;
    }

    while(tmp)
    {
        tmp2 = tmp->next;
        commitSingle( tmp->lcb->index, tmp->lcb );
        free(tmp);
        /*releaseToXactLCBPool( tmp->lcb->index->buffer_manager, tmp );*/
        
        tmp = tmp2;
    }
    
    free(xactState);
    return SUCCESS;
}


ErrCode commitTransaction(TxnState *txn)
{
    XACTState * xactState = (XACTState*)txn;
    XactLCBs *  tmp       = xactState->xactLCBs, *tmp2;

    ThreadOpenIndexes * oi = xactState->openIndexesList, * oi2;

    while( oi )
    {
        oi2 = oi->next;
        releaseToAbortPool(oi->index->buffer_manager, oi);
        
        oi = oi2;
    }
    
    while(tmp)
    {
        tmp2 = tmp->next;
        commitSingle( tmp->lcb->index, tmp->lcb );
        free(tmp);
        tmp = NULL;
        /*releaseToXactLCBPool( tmp->lcb->index->buffer_manager, tmp );*/
       
        tmp = tmp2;
    }

    free(xactState);

    return SUCCESS;
}

/*
KeyVal temp;

char * getIndexName(IdxState * idxState)
{
    return ((IDXState*)idxState)->index->name;
}

void printItems(IdxState* idxState)
{
    printSearch(&temp, ((IDXState*)idxState)->index->index);
}


void logNotFound(Record * firstRecord, IdxState * idxState)
{
    
    printf("KEY -----> %s NOT FOUND IN \nINDEX -----> %s\n", firstRecord->key.keyval.charkey, ((IDXState*)idxState)->index->name );
    printSearch((const KeyVal*)(&firstRecord->key.keyval), ((IDXState*)idxState)->index->index);

}
*/

ErrCode get(IdxState *idxState, TxnState *txn, Record *record)
{
    LockControlBlock * lcb       = NULL;
    IDXState *         idx_state = (IDXState*)idxState;
    XACTState *        xactState = (XACTState*)txn;
    IndexData *        index     = idx_state->index;

    int     ret;
    ErrCode result;

    int flag = 0;
#if DEBUG
    int rand_=-1;    
    rand_ = (int)( rand() % 5000 + 1 );
#endif    

    switch(record->key.type)
    {
      case SHORT:
          idx_state->lastKey.shortkey = record->key.keyval.shortkey;
          break;
      case INT:
          idx_state->lastKey.intkey = record->key.keyval.intkey;
          break;
      default:
          strcpy( idx_state->lastKey.charkey, record->key.keyval.charkey );
          break;
    }

    if( record->key.keyval.charkey[0] == 'f' && record->key.keyval.charkey[1] == 'R' )
    {
        flag = 1;
    }
    /*
    if(record->key.type == VARCHAR)
        strcpy( idx_state->lastKey.charkey, record->key.keyval.charkey );
    else if(record->key.type == SHORT)
        idx_state->lastKey.shortkey = record->key.keyval.shortkey;
    else
        idx_state->lastKey.intkey = record->key.keyval.intkey;
    */        
    idx_state->keyNotFound        = 0;
    idx_state->lastTNode          = NULL;
    idx_state->lastTNodeRecordIdx = -1;

    if(unlikely(xactState!=NULL && hasTheLock(index, xactState, 0)))
    {
        if(index->hasRecords(index->index))
            xactState->firstGetOrGetNext = 0;
        
        /* Do the actual operation */
        
        result = index->findRecord(idx_state, (const KeyVal*)(&record->key.keyval), record);
        
        if(unlikely(result != SUCCESS ))
            idx_state->keyNotFound = 1;

        return result;
    }


    GET_MUTEX(ret, index->xlock, "@ get() xlock not acquired");
        
    lcb = getLCBfromBuffer(index->buffer_manager);
    
    if(unlikely((!index->isXlocked && !index->hasWaiters)))
    {
        
        /*GET_MUTEX(ret, index->slock, "@ get() slock not acquired\n");*/
#if DEBUG            
        LOG("XACT[%d] acquires S-lock on %s\n",(xactState?xactState->xactId:rand_),index->name);
#endif        
        index->isSlocked = 1;
        lcb->currLockStatus = 0;/* it's now granted */        
        /*RELEASE(index->slock);*/
        RELEASE(index->xlock);

        /*lcb = (LockControlBlock*)malloc(sizeof(LockControlBlock));*/
        lcb->xactId   = xactState ? xactState->xactId : -1; /*if it's not an xact give -1*/
        lcb->lockMode = 0;                                  /* S-lock, read-lock */
        /* lcb->currLockStatus = -1; /unknown status of lock*/
        lcb->next     = NULL;
        lcb->prev     = NULL;
        lcb->index    = index;
        pthread_cond_init (&lcb->cond_var, NULL);        
        
        /* add to lcb chain */
        GET_MUTEX(ret, index->lcb_lock, "@ get() lcb_lock not acquired\n");
        if(index->lcbChain)
        {
            index->lcbTail->next = lcb;
            lcb->prev = index->lcbTail;
            index->lcbTail = lcb;
        }
        else
            index->lcbChain = index->lcbTail = lcb;
        RELEASE(index->lcb_lock);
        
        if(likely(xactState!=NULL))
            addLCBtoXACT(xactState, lcb);

        if(xactState && index->hasRecords(index->index))
            xactState->firstGetOrGetNext = 0;
        
        /* Do the actual operation */
        result = index->findRecord(idx_state, (const KeyVal*)(&record->key.keyval), record);
        
        if(unlikely(result != SUCCESS ))
            idx_state->keyNotFound = 1;

        if(unlikely(!xactState))
        {
            /* out of xact, immediately commit */
            commitSingle(index, lcb);
        }
        return result;
    }

    /*lcb = (LockControlBlock*)malloc(sizeof(LockControlBlock));*/
    lcb->xactId   = xactState ? xactState->xactId : -1; /*if it's not an xact give -1*/
    lcb->lockMode = 0;                                  /* S-lock, read-lock */
    /* lcb->currLockStatus = -1; /unknown status of lock*/
    lcb->next     = NULL;
    lcb->prev     = NULL;
    lcb->index    = index;
    pthread_cond_init (&lcb->cond_var, NULL);        
    
    lcb->currLockStatus = 1;/* it's waiting */
    GET_MUTEX(ret, index->lcb_lock, "@ get() lcb_lock not acquired\n");
    if(index->lcbChain)
    {
        index->lcbTail->next = lcb;
        lcb->prev = index->lcbTail;
        index->lcbTail = lcb;
    }
    else
        index->lcbChain = index->lcbTail = lcb;
    index->hasWaiters++;
    RELEASE(index->lcb_lock);
    
    if(likely(xactState!=NULL))
        addLCBtoXACT(xactState, lcb);

    /*do
    {*/
#if DEBUG        
    LOG("XACT[%d] will block until %s is available\n",
        xactState?xactState->xactId:rand_, index->name);
/* #endif */
    printIndexLCBs(index);
#endif
    
    pthread_cond_wait( &lcb->cond_var, &index->xlock );/* wait() until someone wakes up */
    /*}while(unlikely(index->isXlocked));*/

#if DEBUG    
    LOG("%d XACT[%d] after pthread_cond_wait\n", rand_,
        xactState?xactState->xactId:rand_);
#endif
    

    /*GET_MUTEX(ret, index->slock, "@ get() slock not acquired");*/
#if DEBUG        
    LOG("XACT[%d] acquires S-lock on %s\n",(xactState?xactState->xactId:rand_),index->name);
#endif
    index->isSlocked = 1;
    /*RELEASE(index->slock);*/
    RELEASE(index->xlock);

    GET_MUTEX(ret, index->lcb_lock, "@ get() lcb_lock not acquired\n");
    lcb->currLockStatus = 0;/* it's now granted */
    index->hasWaiters--;
    RELEASE(index->lcb_lock);
        
    if(xactState && index->hasRecords(index->index))
        xactState->firstGetOrGetNext = 0;
        
    /* Do the actual operation */
    result = index->findRecord(idx_state, (const KeyVal*)(&record->key.keyval), record);
        
    if(unlikely(result != SUCCESS))
        idx_state->keyNotFound = 1;

    if(unlikely(!xactState))
    {
        /* out of xact, immediately commit */
        commitSingle(index, lcb);
    }

    return result;
}

ErrCode insertRecord(IdxState *idxState, TxnState *txn, Key *k, const char* payload)
{
    RecordWrapper      recwrap;    
    LockControlBlock * lcb       = NULL;
    IDXState *         idx_state = (IDXState*)idxState;
    XACTState *        xactState = (XACTState*)txn;
    IndexData *        index     = idx_state->index;
    
    ErrCode res;
    int     ret;
    
    recwrap.key = (const KeyVal*)(&k->keyval);
    recwrap.payload = payload;
    recwrap.payloadTree = NULL;

#if DEBUG
    int rand_=-1;    
    rand_ = (int)( rand() % 5000 + 1 );
#endif

    if(xactState && hasTheLock(index, xactState, 1))
    {
        res = index->insertItem(index, &recwrap, 0);
        if( res == SUCCESS )
            addToPendingList( index, xactState, &recwrap, 0 );
        
        return res;
    }

    GET_MUTEX(ret, index->xlock, "@ insertRecord(index->xlock)");
        
    lcb = getLCBfromBuffer(index->buffer_manager);
    
    if((!index->isXlocked && !index->hasWaiters && !index->isSlocked ))
    {
#if DEBUG            
        LOG("XACT[%d] acquires X-lock on %s\n",(xactState?xactState->xactId:rand_),index->name);
#endif            
        index->isXlocked = 1;
        lcb->currLockStatus = 0;/* it's now granted */        
        RELEASE(index->xlock);

        /*lcb = (LockControlBlock*)malloc(sizeof(LockControlBlock));*/
        lcb->xactId = xactState ? xactState->xactId : -1; /*if it's not an xact give -1*/
        lcb->lockMode = 1; /* x-lock, write-lock */
        lcb->next = NULL;
        lcb->prev = NULL;
        lcb->index = index;
        pthread_cond_init (&lcb->cond_var, NULL);
        
        /* add to lcb chain */
        GET_MUTEX(ret, index->lcb_lock, "@ insertRecord(index->lcb_lock)");
        if(index->lcbChain)
        {
            index->lcbTail->next = lcb;
            lcb->prev = index->lcbTail;
            index->lcbTail = lcb;
        }
        else
            index->lcbChain = index->lcbTail = lcb;
        RELEASE(index->lcb_lock);
        
        if(xactState)
            addLCBtoXACT(xactState, lcb);

        
        
        res = index->insertItem(index, &recwrap, 0);
        if( xactState && res == SUCCESS )
            addToPendingList( index, xactState, &recwrap, 0 );

        
        if(!xactState)
        {
            /* out of xact, immediately commit */
            commitSingle(index, lcb);
        }

        return res;

    }

    /*lcb = (LockControlBlock*)malloc(sizeof(LockControlBlock));*/
    lcb->xactId = xactState ? xactState->xactId : -1; /*if it's not an xact give -1*/
    lcb->lockMode = 1; /* x-lock, write-lock */
    lcb->next = NULL;
    lcb->prev = NULL;
    lcb->index = index;
    pthread_cond_init (&lcb->cond_var, NULL);
    
    lcb->currLockStatus = 1;/* it's waiting */
    GET_MUTEX(ret, index->lcb_lock, "@ insertRecord(lcb_lock not acquired)");
    if(index->lcbChain)
    {
        index->lcbTail->next = lcb;
        lcb->prev = index->lcbTail;
        index->lcbTail = lcb;
    }
    else
        index->lcbChain = index->lcbTail = lcb;
    index->hasWaiters++;
    RELEASE(index->lcb_lock);

    if(xactState)
        addLCBtoXACT(xactState, lcb);


    /* wait() until someone wakes up for giving us the lock */
    do{
#if DEBUG        
        LOG("XACT[%d] will block until %s is available\n",
            xactState?xactState->xactId:rand_, index->name);
/* #endif */        
        printIndexLCBs(index);
#endif        
        
        pthread_cond_wait( &lcb->cond_var, &index->xlock);
    }while(index->isXlocked);    

#if DEBUG    
    LOG("%d XACT[%d] after pthread_cond_wait\n", rand_,
        xactState?xactState->xactId:rand_);
#endif    
    
#if DEBUG        
    LOG("XACT[%d] acquires X-lock on %s\n",(xactState?xactState->xactId:rand_),index->name);
#endif        
    index->isXlocked = 1;
    RELEASE(index->xlock);

    GET_MUTEX(ret, index->lcb_lock, "@ get() lcb_lock not acquired\n");
    lcb->currLockStatus = 0;/* it's now granted */
    index->hasWaiters--;
    RELEASE(index->lcb_lock);

    res = index->insertItem(index, &recwrap, 0);
    if( xactState && res == SUCCESS )
        addToPendingList( index, xactState, &recwrap, 0 );
        
    if(!xactState)
    {
        /* out of xact, immediately commit */
        commitSingle(index, lcb);
    }

    return res;
    /* OLD return ((res != SUCCESS) ? SUCCESS : ENTRY_EXISTS);*/
}

ErrCode getNext(IdxState *idxState, TxnState *txn, Record *record)
{
    LockControlBlock * lcb       = NULL;
    IDXState *         idx_state = (IDXState*)idxState;
    XACTState *        xactState = (XACTState*)txn;
    IndexData *        index     = idx_state->index;

    int     ret;
    ErrCode returnVal;
    ErrCode result;


#if DEBUG
    int rand_=-1;    
    rand_ = (int)( rand() % 5000 + 1 );
#endif    
    
    if(xactState && hasTheLock(index, xactState, 0))
    {
        if(idx_state->keyNotFound == 2
           || xactState->firstGetOrGetNext)
        {
            /* either out of xact or no calls before */
            result = index->getFirst(idx_state, record);

            xactState->firstGetOrGetNext = 0;
        }
        else
            /* Do the actual operation */
            result = index->getNextRecord(idx_state, record);

        if(result == SUCCESS)
            returnVal = SUCCESS;
        else if( idx_state->lastTNodeRecordIdx == -2 )
            returnVal = DB_END;
        else
        {
            idx_state->keyNotFound = 1;
            returnVal = KEY_NOTFOUND;
        }

        return returnVal;
    }

    GET_MUTEX(ret, index->xlock, "@ get() xlock not acquired");

    lcb = getLCBfromBuffer(index->buffer_manager);
    
    if((!index->isXlocked && !index->hasWaiters))
    {
        
        /*GET_MUTEX(ret, index->slock, "@ get() slock not acquired\n");*/
#if DEBUG            
        LOG("XACT[%d] acquires S-lock on %s\n",(xactState?xactState->xactId:rand_),index->name);
#endif
        index->isSlocked = 1;
        lcb->currLockStatus = 0;/* it's now granted */        
        /*RELEASE(index->slock);*/
        RELEASE(index->xlock);

        /*lcb = (LockControlBlock*)malloc(sizeof(LockControlBlock));*/
        lcb->xactId = xactState ? xactState->xactId : -1; /*if it's not an xact give -1*/
        lcb->lockMode = 0; /* S-lock, read-lock */
        /* lcb->currLockStatus = -1; /unknown status of lock*/
        lcb->next = NULL;
        lcb->prev = NULL;
        lcb->index = index;
        pthread_cond_init (&lcb->cond_var, NULL);
        
        /* add to lcb chain */
        GET_MUTEX(ret, index->lcb_lock, "@ get() lcb_lock not acquired\n");
        if(index->lcbChain)
        {
            index->lcbTail->next = lcb;
            lcb->prev = index->lcbTail;
            index->lcbTail = lcb;
        }
        else
            index->lcbChain = index->lcbTail = lcb;
        RELEASE(index->lcb_lock);

        if(xactState)
                addLCBtoXACT(xactState, lcb);
        
        if(xactState == NULL || idx_state->keyNotFound == 2
           || xactState->firstGetOrGetNext)
        {
            /* either out of xact or no calls before */
            result = index->getFirst(idx_state, record);

            if(xactState)
                xactState->firstGetOrGetNext = 0;
        }
        else
            /* Do the actual operation */
            result = index->getNextRecord(idx_state, record);

        if(result == SUCCESS)
            returnVal = SUCCESS;
        else if( idx_state->lastTNodeRecordIdx == -2 )
            returnVal = DB_END;
        else
        {
            idx_state->keyNotFound = 1;
            returnVal = KEY_NOTFOUND;
        }

        if(!xactState)
        {
            /* out of xact, immediately commit */
            commitSingle(index, lcb);
        }
        return returnVal;
    }

    /*lcb = (LockControlBlock*)malloc(sizeof(LockControlBlock));*/
    lcb->xactId = xactState ? xactState->xactId : -1; /*if it's not an xact give -1*/
    lcb->lockMode = 0; /* S-lock, read-lock */
    /* lcb->currLockStatus = -1; /unknown status of lock*/
    lcb->next = NULL;
    lcb->prev = NULL;
    lcb->index = index;
    pthread_cond_init (&lcb->cond_var, NULL);
    
    lcb->currLockStatus = 1;/* it's waiting */
    GET_MUTEX(ret, index->lcb_lock, "@ get() lcb_lock not acquired\n");
    if(index->lcbChain)
    {
        index->lcbTail->next = lcb;
        lcb->prev = index->lcbTail;
        index->lcbTail = lcb;
    }
    else
        index->lcbChain = index->lcbTail = lcb;
    index->hasWaiters++;
    RELEASE(index->lcb_lock);
    
    if(xactState)
        addLCBtoXACT(xactState, lcb);

    /*    do{*/
#if DEBUG        
    LOG("XACT[%d] will block until %s is available\n",
        xactState?xactState->xactId:rand_, index->name);
/* #endif */    
    printIndexLCBs(index);
#endif        
        
    pthread_cond_wait( &lcb->cond_var, &index->xlock );/* wait() until someone wakes up */
    /*}while(index->isXlocked);*/

#if DEBUG    
    LOG("%d XACT[%d] after pthread_cond_wait\n", rand_,
        xactState?xactState->xactId:rand_);
#endif
    

    /*GET_MUTEX(ret, index->slock, "@ get() slock not acquired");*/
#if DEBUG        
    LOG("XACT[%d] acquires S-lock on %s\n",(xactState?xactState->xactId:rand_),index->name);
#endif        
    index->isSlocked = 1;
    /*RELEASE(index->slock);*/
    RELEASE(index->xlock);

    GET_MUTEX(ret, index->lcb_lock, "@ get() lcb_lock not acquired\n");
    lcb->currLockStatus = 0;/* it's now granted */
    index->hasWaiters--;
    RELEASE(index->lcb_lock);
        
    if(xactState == NULL || idx_state->keyNotFound == 2
       || xactState->firstGetOrGetNext)
    {
        /* either out of xact or no calls before */
        result = index->getFirst(idx_state, record);

        if(xactState)
            xactState->firstGetOrGetNext = 0;
    }
    else
        /* Do the actual operation */
        result = index->getNextRecord(idx_state, record);
        
    if(result==SUCCESS)
        returnVal = SUCCESS;
    else if( idx_state->lastTNodeRecordIdx == -2 )
        returnVal = DB_END;
    else
    {
        idx_state->keyNotFound = 1;
        returnVal = KEY_NOTFOUND;
    }
        
    if(!xactState)
    {
        /* out of xact, immediately commit */
        commitSingle(index, lcb);
    }

    return returnVal;
}

ErrCode deleteRecord(IdxState *idxState, TxnState *txn, Record *record)
{
    RecordWrapper      recwrapper;
    LockControlBlock * lcb;
    IDXState *         idx_state = (IDXState*)idxState;
    XACTState *        xactState = (XACTState*)txn;
    IndexData *        index     = idx_state->index;

    int     ret;
    ErrCode returnVal;
    

    recwrapper.key = (const KeyVal*)(&record->key.keyval);
    recwrapper.payload = record->payload;
    
#if DEBUG
    int rand_ = -1;    
    rand_ = (int)( rand() % 5000 + 1 );
#endif    
    
    if(xactState && hasTheLock(index, xactState, 1))
    {
        /* actual deletion here */
        returnVal = index->removeRecord(index, &recwrapper, xactState );
        
        return returnVal;
    }

    GET_MUTEX(ret, index->xlock, "@ deleteRecord() xlock not acquired");
        
    lcb = getLCBfromBuffer(index->buffer_manager);
    
    if(!(index->isXlocked || index->hasWaiters))
    {
        
        /*needs lock here*/
        index->isXlocked = 1;
        lcb->currLockStatus = 0;/* it's now granted */        
        RELEASE(index->xlock);

        /*lcb = (LockControlBlock*)malloc(sizeof(LockControlBlock));*/
        lcb->xactId = xactState ? xactState->xactId : -1; /*if it's not an xact give -1*/
        lcb->lockMode = 1; /* X-lock, write-lock */
        /* lcb->currLockStatus = -1; /unknown status of lock*/
        lcb->next = NULL;
        lcb->prev = NULL;
        lcb->index = index;
        pthread_cond_init (&lcb->cond_var, NULL);
        
        /* add to lcb chain */
        GET_MUTEX(ret, index->lcb_lock, "@ deleteRecord() index->lcb_lock)");
        if(index->lcbChain)
        {
            index->lcbTail->next = lcb;
            lcb->prev = index->lcbTail;
            index->lcbTail = lcb;
        }
        else
            index->lcbChain = index->lcbTail = lcb;
        RELEASE(index->lcb_lock);

        if(xactState)
            addLCBtoXACT(xactState, lcb);

        /* actual deletion here */
        returnVal = index->removeRecord(index, &recwrapper, xactState );
        
        if(!xactState)
        {
            /* out of xact, immediately commit */
            commitSingle(index, lcb);
        }
        return returnVal;
        
    }

    /*lcb = (LockControlBlock*)malloc(sizeof(LockControlBlock));*/
    lcb->xactId = xactState ? xactState->xactId : -1; /*if it's not an xact give -1*/
    lcb->lockMode = 1; /* X-lock, write-lock */
    /* lcb->currLockStatus = -1; /unknown status of lock*/
    lcb->next = NULL;
    lcb->prev = NULL;
    lcb->index = index;
    pthread_cond_init (&lcb->cond_var, NULL);
    
    lcb->currLockStatus = 1;/* it's waiting */
    GET_MUTEX(ret, index->lcb_lock, "@ deleteRecord() lcb_lock not acquired)");
    if(index->lcbChain)
    {
        index->lcbTail->next = lcb;
        lcb->prev = index->lcbTail;
        index->lcbTail = lcb;
    }
    else
        index->lcbChain = index->lcbTail = lcb;
    index->hasWaiters++;
    RELEASE(index->lcb_lock);

    if(xactState)
        addLCBtoXACT(xactState, lcb);

    do{
        
#if DEBUG        
        LOG("XACT[%d] will block until %s is available\n",
            xactState?xactState->xactId:rand_, index->name);
/* #endif */        
        printIndexLCBs(index);
#endif        
        
        /* wait() until someone wakes up for giving us the lock */
        pthread_cond_wait( &lcb->cond_var, &index->xlock);
    }while(index->isXlocked);

#if DEBUG            
    LOG("%d XACT[%d] after pthread_cond_wait\n", rand_,
        xactState?xactState->xactId:rand_);
#endif
    

#if DEBUG                
    LOG("XACT[%d] acquires X-lock on %s\n",(xactState?xactState->xactId:rand_),index->name);
#endif        
    index->isXlocked = 1;
    RELEASE(index->xlock);


    GET_MUTEX(ret, index->lcb_lock, "@ deleteRecord(index->lcb_lock)");
    lcb->currLockStatus = 0;/* it's now granted */
    index->hasWaiters--;
    RELEASE(index->lcb_lock);

    /* actual deletion here */
    returnVal = index->removeRecord(index, &recwrapper, xactState );
        
    if(!xactState)
    {
        /* out of xact, immediately commit */
        commitSingle(index, lcb);
    }
        
    return returnVal;

}

