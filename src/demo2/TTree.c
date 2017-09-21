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
#include <string.h>
#include <pthread.h>

#include "BinarySearchTree.h"
#include "Stack.h"
#include "BufferManager.h" /* for allocators and makeTNodes */
#include "TTree.h"


/******************* DEFINITIONS **********************************************/
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define DEBUG 0
#define abs(X) myabs(X)
#define mystrcmp(X, Y) strcmp(X,Y)
#define INTKEY_CMP(K1, K2) ((K1)-(K2))
#define SHORTKEY_CMP(K1, K2) ((K1)-(K2))
#define PAYLOAD_COPY(DEST, SRC) strcpy(DEST, SRC) /* MAX_PAYLOAD_LEN+1 */

#define VARCHARKEY_CMP(K1, K2)                                          \
    ( (*(K1) != *(K2)) ? (int)((unsigned char)*(K1)-(unsigned char)*(K2)) : \
      strcmp((K1), (K2)) )

/*#define MYFREE(x,y) addToFreeList(x, y)*/
#define MYFREE(x, y)                                    \
    stack_push(y->free_list, x);                        \
    x = NULL


/***************Debugging related***************/

#define CHECK_VARCHAR(T)
#define CHECK_SHORT(T)
#define CHECK_INT(T)

/* #define CHECK_VARCHAR(T) checkTTree(T) */
/* #define CHECK_SHORT(T) checkTTree_short(T) */
/* #define CHECK_INT(T) checkTTree_int(T) */

void randomTests(int numberOfTests, int maxInserts, int maxDeletes);
void loadTest(const char * testName, int **insertKeys, int *numInsert, int ** delKeys, int *numDel);
void runAllTestsAgain();
void testTemplate(const char * testName);


/********** FUNCTION POINTERS **********/

int (*hasRecordsFunc[3])(const void *) =
{
    hasRecords_short,
    hasRecords_int,
    hasRecords
};

ErrCode (*findRecord[3])(IDXState *, const KeyVal *, Record *) =
{
    findRecordAndTNode_short,
    findRecordAndTNode_int,
    findRecordAndTNode
};

    
ErrCode (*insertItem[3])(IndexData *, RecordWrapper *, int) = 
{
    insertItem_short,
    insertItem_int,
    insertItem_new
};

ErrCode (*getFirst[3])(IDXState *, Record * out) =
{
    getFirstRecordAndTNode_short,
    getFirstRecordAndTNode_int,
    getFirstRecordAndTNode
};

ErrCode (*getNextRecord[3])(IDXState *, Record * out) =
{
    getNextRecordAndTNode_short,
    getNextRecordAndTNode_int,
    getNextRecordAndTNode
};

ErrCode (*removeRecord[3])(IndexData *, RecordWrapper *, XACTState *) = 
{
    deleteOneOrAllRecords_short,
    deleteOneOrAllRecords_int,
    deleteOneOrAllRecords
};

static inline int _keyvarchar_cmp(const KeyVal *key1, const KeyVal *key2)
{
    return (*key1->charkey != *key1->charkey) ?
        ((int)(*key1->charkey - *key2->charkey)) :
        strcmp( key1->charkey, key2->charkey );
}

inline int _keyshort_cmp(const KeyVal *key1, const KeyVal *key2)
{
    return (key1->shortkey-key2->shortkey);
}

inline int _keyint_cmp(const KeyVal *key1, const KeyVal *key2)
{
    return (key1->intkey-key2->intkey);
}

int (*_keycmp[3])(const KeyVal *key1, const KeyVal *key2) = 
{
    _keyshort_cmp,
    _keyint_cmp,
    _keyvarchar_cmp
};

inline int hasRecords(const void * node)
{
    return ((const TNode*)node)->currSize > 0;
}

inline int hasRecords_short(const void * node)
{
    return ((const TNodeShort*)node)->currSize > 0;
}

inline int hasRecords_int(const void * node)
{
    return ((const TNodeInt*)node)->currSize > 0;
}


static inline int myabs(int ecx)
{
    int ebx = ecx;
    ecx = ecx >> 31;
    ebx = ebx ^ ecx;
    ebx -= ecx;
    return ebx;
}

static inline int max(int a, int b)
{
  return a > b ? a : b; 
}

int myrandom(int max)
{
    int res=(int)(max * ((double)rand()/(double)RAND_MAX));
    return (res<max&&res>=0)?res:max-1;
}

/*** IMPLEMENTATIOS ***/
inline void addToFreeList(void * tnode, BufferManager * buf_man)
{
    /* *((int*)tnode+1) = 0xDEADBEEF;*/
    stack_push(buf_man->free_list, tnode);
}

void transferRecursively(BSTNode **to, BSTNode * from)
{
    if( !from )
        return;

    bst_add( to, from->data );
    transferRecursively( to, from->left );
    transferRecursively( to, from->right );
}

ErrCode transferAllPayloads( BSTNode ** to, RecordWrapper * from )
{
    bst_add( to, from->payload );
    transferRecursively( to, from->payloadTree );

    /* do i need to free really? */
    bst_free( from->payloadTree );
    from->payloadTree = NULL;
    
    return SUCCESS;
}

inline static int height( const TNode * tree )
{
    return ( tree == NULL ? -1 : tree->height );
}

inline static int isLeaf(const TNode * node)
{
    return (node != NULL && (node->left == NULL && node->right == NULL));
}

inline static int isHalfLeaf(const TNode * node)
{
    return (node != NULL && 
            ((node->left == NULL && node->right != NULL) 
             || (node->left != NULL && node->right == NULL)));
}

inline static short height_short( const TNodeShort * tree )
{
    return ( tree == NULL ? -1 : tree->height );
}

inline static int height_int( const TNodeInt * tree )
{
    return ( tree == NULL ? -1 : tree->height );
}

inline static int isLeaf_short(const TNodeShort * node)
{
    return (node != NULL && (node->left == NULL && node->right == NULL));
}

inline static int isLeaf_int(const TNodeInt * node)
{
    return (node != NULL && (node->left == NULL && node->right == NULL));
}

inline static int isHalfLeaf_short(const TNodeShort * node)
{
    return (node != NULL && 
            ((node->left == NULL && node->right != NULL) 
             || (node->left != NULL && node->right == NULL)));
}

inline static int isHalfLeaf_int(const TNodeInt * node)
{
    return (node != NULL && 
            ((node->left == NULL && node->right != NULL) 
             || (node->left != NULL && node->right == NULL)));
}

int binarySearch( const TNode * node, const char * key )
{
    register int low = 0, high = node->currSize - 1;
    int mid, cmp;
    
    while( likely(low <= high) )
    {
        mid = (low+high)>>1;

        cmp = strcmp( key, (node->data+mid)->key.charkey );

        if( cmp < 0 )
            high = mid-1;
        else if( cmp > 0 )
            low = mid+1;
        else
            return mid;
    }

    return -1;
}

int new_binarySearch_short( const TNodeShort * node, int32_t key )
{
    int low = 0, high = node->currSize-1, mid;
    int N = high;
    
    while (low < high) {
        mid = (low+high)>>1;
        if (node->keys[mid] < key)
            low = mid + 1; 
        else
            high = mid; 
    }

    if ((low < N) && (node->keys[low] == key))
        return low;
    else
        return -1;       
}

int binarySearch_short( const TNodeShort * node, int32_t _key )
{
    int low = 0, high = node->currSize - 1;
    int mid;
    register int32_t key = _key;
    register int32_t ckey;
    
    while( likely(low <= high) )
    {
        mid = (low+high)>>1;
        ckey = *(node->keys+mid);
        
        if( key < ckey )
            high = mid-1;
        else if( key > ckey )
            low = mid+1;
        else
            return mid;
        /*
        cmp = SHORTKEY_CMP( key, node->keys[mid] );

        if( cmp == 0 )
            return mid;
        else if( cmp < 0 )
            high = mid-1;
        else
        low = mid+1;
        */
    }

    return -1;
}

int new_binarySearch_int( const TNodeInt * node, int64_t key )
{

    int low = 0, high = node->currSize-1, mid;
    int N = high;
    
    while (low < high) {
        mid = (low+high)>>1;
        
        if (node->keys[mid] < key)
            low = mid + 1; 
        else
            high = mid; 
    }

    if ((low < N) && (node->keys[low] == key))
        return low;
    else
        return -1;       
}

int binarySearch_int( const TNodeInt * node, int64_t _key )
{
    int low = 0, high = node->currSize - 1;
    int mid;
    register int64_t key = _key;
    register int64_t ckey;

    while( likely(low <= high) )
    {
        mid = (low+high)>>1;
        ckey = *(node->keys+mid);
        
        if( key < ckey )
            high = mid-1;
        else if( key > ckey )
            low = mid+1;
        else
            return mid;
        /*
        cmp = INTKEY_CMP( key, node->keys[mid] );

        if( cmp == 0 )
            return mid;
        else if( cmp < 0 )
            high = mid-1;
        else
        low = mid+1;
        */
    }

    return -1;
}


ErrCode _findRecordAndTNode(IDXState * idxState, const KeyVal * _key, Record * result)
{
    TNode *      root = idxState->index->index;
    const char * key  = _key->charkey;
    
    while( likely(root != NULL && root->currSize > 0) )
    {
        /* if less than the min value of the node -> search down left */
        if( likely(VARCHARKEY_CMP( key, root->data->key.charkey ) < 0) )
            root = root->left;
        /* if greater than the max value of the node -> search down right */
        else if( likely(VARCHARKEY_CMP( key, root->data[root->currSize-1].key.charkey ) > 0) )
            root = root->right;
        /* search the current node */
        else
        {
            int found = binarySearch( root, key );

            idxState->lastTNode = root;
            if( found != -1 )
            {
                MyRecord * rec = root->data+found;
                
                if(rec->payloadTree)
                {
                    BSTNode * top = rec->payloadTree;
                    Stack * s;

                    if(idxState->payloadStack)
                    {
                        stack_free(idxState->payloadStack);
                        idxState->payloadStack = NULL;
                    }

                    stack_init(&idxState->payloadStack);
                    s = idxState->payloadStack;
                    
                    while(top)
                    {
                        stack_push(s, top);
                        top = top->left;
                    }

                    idxState->payloadStack = s;
                }
                idxState->lastTNodeRecordIdx = found;
                PAYLOAD_COPY( result->payload, rec->payload );
                
                return SUCCESS;
            }
            break;
        }
    }

    return KEY_NOTFOUND;
}

ErrCode findRecordAndTNode(IDXState * idxState, const KeyVal * _key, Record * result)
{
    TNode *      root = idxState->index->index;
    TNode *      mark = root;
    const char * key  = _key->charkey;
    int          found;
    
    while( root && root->currSize > 0 )
    {
        /* if less than the min value of the node -> search down left */
        if( VARCHARKEY_CMP( key, root->data->key.charkey ) < 0 )
            root = root->left;
        /* search the current node */
        else
        {
            mark = root;
            root = root->right;
        }
    }

    found = binarySearch( mark, key );

    idxState->lastTNode = mark;
    if( found != -1 )
    {
        MyRecord * rec = mark->data+found;
                
        if(rec->payloadTree)
        {
            BSTNode * top = rec->payloadTree;
            Stack * s;

            if(idxState->payloadStack)
            {
                stack_free(idxState->payloadStack);
                idxState->payloadStack = NULL;
            }

            stack_init(&idxState->payloadStack);
            s = idxState->payloadStack;
                    
            while(top)
            {
                stack_push(s, top);
                top = top->left;
            }

            idxState->payloadStack = s;
        }
        idxState->lastTNodeRecordIdx = found;
        PAYLOAD_COPY( result->payload, rec->payload );
                
        return SUCCESS;
    }


    return KEY_NOTFOUND;
}

ErrCode findRecordAndTNode_short(IDXState * idxState, const KeyVal * key, Record * result)
{
    TNodeShort * root     = idxState->index->index;
    int32_t      shortkey = key->shortkey;
    
    while( likely(root != NULL && root->currSize > 0) )
    {
        /* if less than the min value of the node -> search down left */
        if( shortkey < *root->keys )
            root = root->left;
        /* if greater than the max value of the node -> search down right */
        else if( shortkey > root->maxKey )
            root = root->right;
        /* search the current node */
        else
        {
            int found = binarySearch_short(root, shortkey);

            if( found != -1 )
            {
                BSTNode * top = (root->payloads+found)->payloadTree;
                
                if(top)
                {
                    Stack * s;

                    if(idxState->payloadStack)
                    {
                        stack_free(idxState->payloadStack);
                        idxState->payloadStack = NULL;
                    }

                    stack_init(&idxState->payloadStack);
                    s = idxState->payloadStack;

                    while(top)
                    {
                        stack_push(s, top);
                        top = top->left;
                    }

                    idxState->payloadStack = s;
                }
                
                idxState->lastTNode = root;
                idxState->lastTNodeRecordIdx = found;                
                PAYLOAD_COPY( result->payload, (root->payloads+found)->payload );
                
                return SUCCESS;
            }
            break;
        }
    }

    return KEY_NOTFOUND;
}

ErrCode findRecordAndTNode_int(IDXState * idxState, const KeyVal * key, Record * result)
{
    TNodeInt * root   = idxState->index->index;
    int64_t    intkey = key->intkey;
    
    while( likely(root != NULL && root->currSize > 0) )
    {
        /* if less than the min value of the node -> search down left */
        if( likely(intkey < *root->keys) )
            root = root->left;
        /* if greater than the max value of the node -> search down right */
        else if( likely(intkey > root->maxKey) )
            root = root->right;
        /* search the current node */
        else
        {
            int found = binarySearch_int(root, intkey);
            
            if( found != -1 )
            {
                BSTNode * top = (root->payloads+found)->payloadTree;
                
                if(top)
                {
                    Stack * s;

                    if(idxState->payloadStack)
                    {
                        stack_free(idxState->payloadStack);
                        idxState->payloadStack = NULL;
                    }

                    stack_init(&idxState->payloadStack);
                    s = idxState->payloadStack;
                    
                    while(top)
                    {
                        stack_push(s, top);
                        top = top->left;
                    }

                    idxState->payloadStack = s;
                }
                
                idxState->lastTNode = root;
                idxState->lastTNodeRecordIdx = found;                
                PAYLOAD_COPY( result->payload, (root->payloads+found)->payload );

                return SUCCESS;
            }
            break;
        }
    }

    return KEY_NOTFOUND;
}

ErrCode getFirstRecordAndTNode(IDXState * idxState, Record * resultOut)
{
    TNode *    root = idxState->index->index;
    MyRecord * rec;
    char *     pay;
    
    while( root->left )
        root = root->left;

    idxState->lastTNode = root;


    rec = (MyRecord*)root->data;
    pay = rec->payload;
    if( rec->payloadTree )
    {
        BSTNode * top = rec->payloadTree;
        Stack * s;

        if(idxState->payloadStack)
        {
            stack_free(idxState->payloadStack);
            idxState->payloadStack = NULL;
        }

        stack_init(&idxState->payloadStack);
        s = idxState->payloadStack;
                    
        while(top)
        {
            stack_push(s, top);
            top = top->left;
        }

        idxState->payloadStack = s;
    }


    strcpy( resultOut->key.keyval.charkey, rec->key.charkey );
    /*memcpy( &resultOut->key.keyval, &rec->key, sizeof(KeyVal) );*/
    resultOut->key.type = VARCHAR;
    PAYLOAD_COPY(resultOut->payload, pay);
    idxState->lastTNodeRecordIdx = 0;
    idxState->keyNotFound = 0;

    /* TODO : check if we do really have record in the DB */
    return SUCCESS;
}

ErrCode getFirstRecordAndTNode_short(IDXState * idxState, Record * resultOut)
{
    TNodeShort * root = idxState->index->index;
    char *       pay;
    BSTNode *    ptree;
    
    while( root->left)
        root = root->left;

    idxState->lastTNode = root;

    pay = root->payloads->payload;/*0*/
    ptree = root->payloads->payloadTree;
    
    if( ptree )
    {
        BSTNode * top = ptree;
        Stack * s;

        if(idxState->payloadStack)
        {
            stack_free(idxState->payloadStack);
            idxState->payloadStack = NULL;
        }

        stack_init(&idxState->payloadStack);
        s = idxState->payloadStack;
                    
        while(top)
        {
            stack_push(s, top);
            top = top->left;
        }

        idxState->payloadStack = s;
    }

    resultOut->key.keyval.shortkey = *root->keys;
    resultOut->key.type = SHORT;
    PAYLOAD_COPY(resultOut->payload, pay);
    return SUCCESS;
}

ErrCode getFirstRecordAndTNode_int(IDXState * idxState, Record * resultOut)
{
    TNodeInt * root = idxState->index->index;
    char *     pay;
    BSTNode *  ptree;
    
    while( root->left)
        root = root->left;

    idxState->lastTNode = root;

    pay = root->payloads->payload;/*0*/
    ptree = root->payloads->payloadTree;
    
    if( ptree )
    {
        BSTNode * top = ptree;
        Stack * s;

        if(idxState->payloadStack)
        {
            stack_free(idxState->payloadStack);
            idxState->payloadStack = NULL;
        }

        stack_init(&idxState->payloadStack);
        s = idxState->payloadStack;
                    
        while(top)
        {
            stack_push(s, top);
            top = top->left;
        }

        idxState->payloadStack = s;
    }

    resultOut->key.keyval.intkey = *root->keys;
    resultOut->key.type = INT;
    PAYLOAD_COPY(resultOut->payload, pay);
    
    return SUCCESS;
}

/* Returns NULL if curr is the first node */
TNode * getPrevTNode(TNode * curr)
{
    TNode * tmpCurr = curr;
    TNode * prev    = findGreatestLowerLeaf(tmpCurr);

    if(!prev)
    {
        prev = tmpCurr;
        do
        {
            tmpCurr = prev;
            prev = tmpCurr->parent;
        }
        while(prev && prev->right != tmpCurr);
    }

    return prev;
}

/* Returns NULL if curr is the first node */
TNodeShort * getPrevTNode_short(TNodeShort * curr)
{
    TNodeShort * tmpCurr = curr;
    TNodeShort * prev    = findGreatestLowerLeaf_short(tmpCurr);

    if(!prev)
    {
        prev = tmpCurr;
        do
        {
            tmpCurr = prev;
            prev = tmpCurr->parent;
        }
        while(prev && prev->right != tmpCurr);
    }

    return prev;
}

/* Returns NULL if curr is the first node */
TNodeInt * getPrevTNode_int(TNodeInt * curr)
{
    TNodeInt * tmpCurr = curr;
    TNodeInt * prev    = findGreatestLowerLeaf_int(tmpCurr);

    if(!prev)
    {
        prev = tmpCurr;
        do
        {
            tmpCurr = prev;
            prev = tmpCurr->parent;
        }
        while(prev && prev->right != tmpCurr);
    }

    return prev;
}

/* Returns NULL if curr is the last node */
TNode * getNextTNode(TNode * curr)
{
    TNode * tmpCurr = curr;
    TNode * next    = findLeastUpperLeaf(tmpCurr);

    if(!next)
    {
        next = tmpCurr;
        do
        {
            tmpCurr = next;
            next = tmpCurr->parent;
        }
        while(next && next->left != tmpCurr);
    }

    return next;
}

/* Returns NULL if curr is the last node */
TNodeShort * getNextTNode_short(TNodeShort * curr)
{
    TNodeShort * tmpCurr = curr;
    TNodeShort * next    = findLeastUpperLeaf_short(tmpCurr);

    if(!next)
    {
        next = tmpCurr;
        do
        {
            tmpCurr = next;
            next = tmpCurr->parent;
        }
        while(next && next->left != tmpCurr);
    }

    return next;
}

/* Returns NULL if curr is the last node */
TNodeInt * getNextTNode_int(TNodeInt * curr)
{
    TNodeInt * tmpCurr = curr;
    TNodeInt * next    = findLeastUpperLeaf_int(tmpCurr);

    if(!next)
    {
        next = tmpCurr;
        do
        {
            tmpCurr = next;
            next = tmpCurr->parent;
        }
        while(next && next->left != tmpCurr);
    }

    return next;
}

int scanTNode(TNode * node, KeyVal * lastKey)
{
    register int idx  = 0;
    char *       last = lastKey->charkey;

    while( idx < node->currSize
           && VARCHARKEY_CMP( (node->data+idx)->key.charkey, last ) <= 0 )
        idx++;
    
    if(idx == node->currSize)
        idx = -1;

    return idx;
}

int scanTNode_short(TNodeShort * node, KeyVal * lastKey)
{
    register int     idx = 0;
    register int32_t key = lastKey->shortkey;
    
    while( idx < node->currSize
           && node->keys[idx] <= key )
        idx++;
    
    if(idx == node->currSize)
        idx = -1;

    return idx;
}

int scanTNode_int(TNodeInt * node, KeyVal * lastKey)
{
    register int     idx = 0;
    register int64_t key = lastKey->intkey;
    
    while( idx < node->currSize
           && node->keys[idx] <= key )
        idx++;
    
    if(idx == node->currSize)
        idx = -1;

    return idx;
}

ErrCode getNextRecordAndTNode(IDXState * idx_state, Record * resultOut)
{
    ErrCode returnVal;    
    int     recIdx    = idx_state->lastTNodeRecordIdx;
    TNode * lastTNode = idx_state->lastTNode;
    char *  pay       = NULL;
    
    if(lastTNode != NULL)
    {
        if( !idx_state->keyNotFound )
        {
            /*last was found so return the same key with the next
              payload from payload stack */
            Stack * s;
            BSTNode * itm;
            
            s = idx_state->payloadStack;

            itm = stack_pop(s);
            if( itm )
            {
                BSTNode * itm2 = itm->right;
                while(itm2)
                {
                    stack_push( s, itm2 );
                    itm2 = itm2->left;
                }
                pay = itm->data;
            }
            else
            {
                /* there is no next on the same key, incr recIdx */
                if(++recIdx >= lastTNode->currSize)
                {
                    recIdx = 0;
                    lastTNode = getNextTNode(lastTNode);
                }

                if( lastTNode )
                {
                    BSTNode * top = (lastTNode->data+recIdx)->payloadTree;
                
                    if(top)
                    {
                        if(idx_state->payloadStack)
                        {
                            stack_free(idx_state->payloadStack);
                            idx_state->payloadStack = NULL;
                        }
                        stack_init(&idx_state->payloadStack);
                        s = idx_state->payloadStack;
                    
                        while(top)
                        {
                            stack_push(s, top);
                            top = top->left;
                        }

                        idx_state->payloadStack = s;
                    }
                }
                
            }
        }
        else
        {
            recIdx = scanTNode( lastTNode, &idx_state->lastKey );
            
            while( recIdx == -1 && (lastTNode = getNextTNode(lastTNode)) )
            {
                    recIdx = scanTNode( lastTNode, &idx_state->lastKey );
            }
            
            if(lastTNode)
            {
                Stack * s;
                /* construct the stack and return first payload */
                BSTNode * top = (lastTNode->data+recIdx)->payloadTree;
                
                if(top)
                {
                    if(idx_state->payloadStack)
                    {
                        stack_free(idx_state->payloadStack);
                        idx_state->payloadStack = NULL;
                    }
                    stack_init(&idx_state->payloadStack);
                    s = idx_state->payloadStack;
                    
                    while(top)
                    {
                        stack_push(s, top);
                        top = top->left;
                    }

                    idx_state->payloadStack = s;
                }

            }
            
        }
    }

    /*may change inside the previous if block, so check it again */
    if(lastTNode == NULL)
    {
        returnVal = DB_END;
        idx_state->lastTNode = NULL;
        idx_state->lastTNodeRecordIdx = -2; /*END of DB*/
    }
    else
    {
        MyRecord * result = (lastTNode->data+recIdx);

        if(!pay)
            pay = result->payload;

        strcpy( resultOut->key.keyval.charkey, result->key.charkey );
        /*memcpy(&resultOut->key.keyval, &result->key, sizeof(KeyVal));*/
        resultOut->key.type = VARCHAR;
        PAYLOAD_COPY(resultOut->payload, pay);
        idx_state->keyNotFound = 0;
        idx_state->lastTNode = lastTNode;
        idx_state->lastTNodeRecordIdx = recIdx;

        strcpy(idx_state->lastKey.charkey, result->key.charkey);
        /*memcpy(&idx_state->lastKey, &result->key, sizeof(KeyVal));*/
        returnVal = SUCCESS;
    }

    return returnVal;
}

ErrCode getNextRecordAndTNode_short(IDXState * idx_state, Record * resultOut)
{
    ErrCode      returnVal;    
    int          recIdx    = idx_state->lastTNodeRecordIdx;
    TNodeShort * lastTNode = idx_state->lastTNode;
    char *       pay       = NULL;
    
    if(lastTNode != NULL)
    {
        if( !idx_state->keyNotFound )
        {
            /*last was found so return the same key with the next
              payload from payload stack */
            Stack * s;
            BSTNode * itm;
            
            s = idx_state->payloadStack;

            itm = stack_pop(s);
            if( itm )
            {
                BSTNode * itm2 = itm->right;
                while(itm2)
                {
                    stack_push( s, itm2 );
                    itm2 = itm2->left;
                }
                pay = itm->data;
            }
            else
            {
                /* there is no next on the same key, incr recIdx */
                if(++recIdx >= lastTNode->currSize)
                {
                    recIdx = 0;
                    lastTNode = getNextTNode_short(lastTNode);
                }

                if( lastTNode )
                {
                    BSTNode * ptree = (lastTNode->payloads+recIdx)->payloadTree;
                
                    if(ptree)
                    {
                        BSTNode * top = ptree;
                    
                        if(idx_state->payloadStack)
                        {
                            stack_free(idx_state->payloadStack);
                            idx_state->payloadStack = NULL;
                        }
                        stack_init(&idx_state->payloadStack);
                        s = idx_state->payloadStack;
                    
                        while(top)
                        {
                            stack_push(s, top);
                            top = top->left;
                        }

                        idx_state->payloadStack = s;
                    }
                }
                
            }
        }
        else
        {
            recIdx = scanTNode_short( lastTNode, &idx_state->lastKey );

            while( recIdx == -1 && (lastTNode = getNextTNode_short(lastTNode)) )
            {
                    recIdx = scanTNode_short( lastTNode, &idx_state->lastKey );
            }
            
            if(lastTNode)
            {
                Stack * s;
                BSTNode * top = (lastTNode->payloads+recIdx)->payloadTree;
                /* construct the stack and return first payload */
                
                if(top)
                {
                    if(idx_state->payloadStack)
                    {
                        stack_free(idx_state->payloadStack);
                        idx_state->payloadStack = NULL;
                    }
                    stack_init(&idx_state->payloadStack);
                    s = idx_state->payloadStack;
                    
                    while(top)
                    {
                        stack_push(s, top);
                        top = top->left;
                    }

                    idx_state->payloadStack = s;
                }

            }
            
        }
    }

    /*may change inside the previous if block, so check it again */
    if(lastTNode == NULL)
    {
        returnVal = DB_END;
        idx_state->lastTNode = NULL;
        idx_state->lastTNodeRecordIdx = -2; /*END of DB*/
    }
    else
    {
        if(!pay)
            pay = (lastTNode->payloads+recIdx)->payload;
        
        idx_state->lastTNode = lastTNode;
        idx_state->lastKey.shortkey = lastTNode->keys[recIdx];
        idx_state->lastTNodeRecordIdx = recIdx;
        idx_state->keyNotFound = 0;
        
        resultOut->key.keyval.shortkey = lastTNode->keys[recIdx];
        resultOut->key.type = SHORT;
        PAYLOAD_COPY(resultOut->payload, pay);

        returnVal = SUCCESS;
    }

    return returnVal;
}

ErrCode getNextRecordAndTNode_int(IDXState * idx_state, Record * resultOut)
{
    ErrCode    returnVal;
    int        recIdx    = idx_state->lastTNodeRecordIdx;
    TNodeInt * lastTNode = idx_state->lastTNode;
    char *     pay       = NULL;
    
    if(lastTNode != NULL)
    {
        if( !idx_state->keyNotFound )
        {
            /*last was found so return the same key with the next
              payload from payload stack */
            Stack * s;
            BSTNode * itm;
            
            s = idx_state->payloadStack;

            itm = stack_pop(s);
            if( itm )
            {
                BSTNode * itm2 = itm->right;
                while(itm2)
                {
                    stack_push( s, itm2 );
                    itm2 = itm2->left;
                }
                pay = itm->data;
            }
            else
            {
                /* there is no next on the same key, incr recIdx */
                if(++recIdx >= lastTNode->currSize)
                {
                    recIdx = 0;
                    lastTNode = getNextTNode_int(lastTNode);
                }

                if( lastTNode )
                {
                    BSTNode * top = (lastTNode->payloads+recIdx)->payloadTree;
                
                    if(top)
                    {
                        if(idx_state->payloadStack)
                        {
                            stack_free(idx_state->payloadStack);
                            idx_state->payloadStack = NULL;
                        }
                        stack_init(&idx_state->payloadStack);
                        s = idx_state->payloadStack;
                    
                        while(top)
                        {
                            stack_push(s, top);
                            top = top->left;
                        }

                        idx_state->payloadStack = s;
                    }
                }
                
            }
        }
        else
        {
            recIdx = scanTNode_int( lastTNode, &idx_state->lastKey );

            while( recIdx == -1 && (lastTNode = getNextTNode_int(lastTNode)) )
            {
                    recIdx = scanTNode_int( lastTNode, &idx_state->lastKey );
            }
            
            if(lastTNode)
            {
                Stack * s;
                BSTNode * top = (lastTNode->payloads+recIdx)->payloadTree;
                /* construct the stack and return first payload */
                
                if(top)
                {
                    if(idx_state->payloadStack)
                    {
                        stack_free(idx_state->payloadStack);
                        idx_state->payloadStack = NULL;
                    }
                    stack_init(&idx_state->payloadStack);
                    s = idx_state->payloadStack;
                    
                    while(top)
                    {
                        stack_push(s, top);
                        top = top->left;
                    }

                    idx_state->payloadStack = s;
                }

            }
            
        }
    }

    /*may change inside the previous if block, so check it again */
    if(lastTNode == NULL)
    {
        returnVal = DB_END;
        idx_state->lastTNode = NULL;
        idx_state->lastTNodeRecordIdx = -2; /*END of DB*/
    }
    else
    {
        if(!pay)
            pay = (lastTNode->payloads+recIdx)->payload;
        
        idx_state->lastTNode = lastTNode;
        idx_state->lastKey.intkey = lastTNode->keys[recIdx];
        idx_state->lastTNodeRecordIdx = recIdx;
        idx_state->keyNotFound = 0;
        
        resultOut->key.keyval.intkey = lastTNode->keys[recIdx];
        resultOut->key.type = INT;
        PAYLOAD_COPY(resultOut->payload, pay);

        returnVal = SUCCESS;
    }

    return returnVal;
}

/* assumption: _root is never NULL */
TNode * findBoundingNode(TNode * _root, const char * _key, int * isFound)
{
    TNode *      root, * par;
    TNode *      mark;
    const char * key = _key;
    int          found;
    
    mark = par = root = _root;
    
    while( likely(root && root->currSize > 0) )
    {
        par = root;
        /* if less than the min value of the node -> search down left */
        if( likely( VARCHARKEY_CMP( key, root->data->key.charkey ) < 0 ) )/*&root->data[0]->key*/
        {
            /*
            if( root->left == NULL )
                break;
            */
            root = root->left;
        }
        /* if greater than the max value of the node -> search down right /
        else if( VARCHARKEY_CMP( key, root->data[root->currSize-1].key.charkey ) > 0 )
        {
            if( root->right == NULL )
                break;

            root = root->right;
        }
        / search the current node */
        else
        {
            /*
            if( root->right == NULL )
                break;
            */
            mark = root;
            root  = root->right;
        }
    }
    
    if(mark == _root && VARCHARKEY_CMP(key, mark->data->key.charkey) < 0)
        found = -1;
    else
        found = binarySearch( mark, key );
    
    if(found != -1)
    {
        *isFound = found;
        return mark;
    }
    
    *isFound = -1;
    return par;
}

TNodeShort * findBoundingNode_short(TNodeShort * _root, const KeyVal * _key, int * isFound)
{
    TNodeShort * root = _root;
    int32_t      key  = _key->shortkey;
    
    while( likely(root->currSize > 0) )
    {
        /* if less than the min value of the node -> search down left */
        if( likely(key < *root->keys) )/*&root->data[0]->key*/
        {
            if( root->left == NULL )
                break;

            root = root->left;
        }
        /* if greater than the max value of the node -> search down right */
        else if( likely(key > root->maxKey) )
        {
            if( root->right == NULL )
                break;

            root = root->right;
        }
        /* search the current node */
        else
        {
            *isFound = 1;
            return root;
        }
    }

    *isFound = 0;
    return root;
}

TNodeInt * findBoundingNode_int(TNodeInt * _root, const KeyVal * _key, int * isFound)
{
    TNodeInt * root = _root;
    int64_t    key  = _key->intkey;
    
    while( likely(root->currSize > 0) )
    {
        /* if less than the min value of the node -> search down left */
        if( likely(key < *root->keys) )/*&root->data[0]->key*/
        {
            if( root->left == NULL )
                break;

            root = root->left;
        }
        /* if greater than the max value of the node -> search down right */
        else if( likely(key > root->maxKey) )
        {
            if( root->right == NULL )
                break;

            root = root->right;
        }
        /* search the current node */
        else
        {
            *isFound = 1;
            return root;
        }
    }

    *isFound = 0;
    return root;
}
/*
TNode * findLowestBoundingNode(TNode * _root, const KeyVal * key, int * isFound)
{
    TNode * root = _root;
    int (*cmp)(const KeyVal *, const KeyVal *) = root->cmp;
    
    while( root->currSize > 0 )
    {
        * if less than the min value of the node -> search down left *
        if( cmp( key, &root->data->key ) <= 0 )
        {
            if( root->left == NULL )
                break;

            root = root->left;
        }
        * if greater than the max value of the node -> search down right *
        else if( cmp( key, &root->data[root->currSize-1].key ) > 0 )
        {
            if( root->right == NULL )
                break;

            root = root->right;
        }
        * search the current node *
        else
        {
            *isFound = 1;
            return root;
        }
    }

    *isFound = 0;
    return root;
}
*/
ErrCode deleteOneOrAllRecords(IndexData * id, RecordWrapper * record, XACTState * xactState)
{
    RecordWrapper recwrap;    
    TNode *       boundingNode;
    TNode *       check = NULL;
    TNode *       root  = id->index;

    const KeyVal * key     = record->key;
    const char *   payload = record->payload;
    
    int found = 0;

    
    boundingNode = findBoundingNode(root, key->charkey, &found);
	
    if( found != -1 )
    {
        /*found = binarySearch( boundingNode, key->charkey );*/

        int searchPayload = (payload != NULL && *(payload) != '\0');
            
        if(searchPayload)
        {
            MyRecord * rec = boundingNode->data+found;

            int pay_found = (mystrcmp( rec->payload, payload ) == 0);

            if(pay_found)
            {

                if(rec->payloadTree)
                {
                    const char * min_pay = bst_first(rec->payloadTree);

                    if(xactState)
                    {
                        recwrap.key = key;
                        recwrap.payload = payload;
                        recwrap.payloadTree = NULL;
                            
                        addToPendingList(id, xactState, &recwrap, 1);
                    }

                    PAYLOAD_COPY(rec->payload, min_pay);
                    bst_delete(&rec->payloadTree, min_pay);
                    return SUCCESS;
                }
            }
            else if(rec->payloadTree)
            {
                    
                pay_found = bst_search(rec->payloadTree, payload);
                if(!pay_found)
                    return ENTRY_DNE;

            }

            if(xactState)
            {
                recwrap.key = key;
                recwrap.payload = payload;
                recwrap.payloadTree = NULL;
            }
        }
        else if(xactState)
        {
            MyRecord * rec = boundingNode->data+found;
            
            recwrap.key = &rec->key;
            recwrap.payload = rec->payload;
            recwrap.payloadTree = rec->payloadTree;
        }
            
        /* add the item to delete List for the transaction */
        if(xactState)
            addToPendingList(id, xactState, &recwrap, 1);
            
        if( boundingNode->currSize > MIN_TNODE_SIZE )
        {
            shiftDownAllItems( boundingNode, found );/*shift down to found-index */
            boundingNode->currSize--;
        }
        else
        {
            /* Underflow situation */
            if( boundingNode->left != NULL && boundingNode->right != NULL )
            {
                /* Internal node */
                TNode * greatestLowerLeaf;
                char * temp_pay;
                MyRecord * tmp_rec;
                int size = boundingNode->currSize;

                boundingNode->currSize = found;
                shiftUpAllItems( boundingNode, 1 );/*shift up items from 0 to index-found*/
                greatestLowerLeaf = findGreatestLowerLeaf( boundingNode );
                temp_pay = boundingNode->data->payload;
                greatestLowerLeaf->currSize--;
                tmp_rec = greatestLowerLeaf->data+greatestLowerLeaf->currSize;
                *boundingNode->data = *tmp_rec;
                tmp_rec->payload = temp_pay;

                boundingNode->currSize = size;

                /*NEW CODE*/
                if(greatestLowerLeaf->currSize==0)
                {
                    check = greatestLowerLeaf->parent;
                    if(greatestLowerLeaf->left)
                    {
                        /* we can not simply delete greatestLowerLeaf */
                        TNode * gllPar = greatestLowerLeaf->parent;
                        int hr, hl;
                            
                        if(gllPar->left==greatestLowerLeaf)
                            gllPar->left = greatestLowerLeaf->left;
                        else
                            gllPar->right = greatestLowerLeaf->left;
                        hr = height(gllPar->right);
                        hl = height(gllPar->left);
                        gllPar->height = 1+max(hr,hl);
                        gllPar->balance = hr-hl;
                        greatestLowerLeaf->left->parent = gllPar;
                    }
                    else
                        if(check->left == greatestLowerLeaf)
                            check->left = NULL;
                        else
                            check->right = NULL;

                    correctHeights(check);
                    MYFREE(greatestLowerLeaf, id->buffer_manager);
                        
                }
            }
            else if( boundingNode->left == NULL && boundingNode->right == NULL )
            {
                /* this is a leaf node, underflow is permitted */
                shiftDownAllItems( boundingNode, found );/*shift down to found-index */
                boundingNode->currSize--;
                /* TODO what if currSize becomes 0 */
                if(boundingNode->currSize==0 && boundingNode->parent)/*if not root*/
                {
                    check = boundingNode->parent;
                    if( check->left == boundingNode )
                        check->left = NULL;
                    else
                        check->right = NULL;
                    correctHeights(check);
                    MYFREE(boundingNode, id->buffer_manager);
                }
            }
            else
            {
                /* half-leaf case */
                if( boundingNode->left )
                {
                    /* half leaf and left is not not null: if can be merged with a leaf coalesce the 2 nodes into 1 leaf */
                    TNode * greatestLowerLeaf;
                    
                    greatestLowerLeaf = findGreatestLowerLeaf( boundingNode );
                    /* transfer items from this node to the greatest lower leaf */
                    if( greatestLowerLeaf->currSize + boundingNode->currSize - 1 > TNODE_SIZE )
                    {
                        /* they are not mergeable, borrow as much as possible from greatestLowerLeaf to boundingNode */
                        /* TODO what to do here actually?? */
                        shiftDownAllItems( boundingNode, found );/*shift down to found-index */
                        boundingNode->currSize--;
#if DEBUG                                        
                        printf("DEBUG: TODO they are not mergeable, what to do here actually?? \n");
#endif                            
                    }
                    else
                    {
                        /* they are mergable  */
                        char * tmp_pay;                        
                        int temp = greatestLowerLeaf->currSize;
                        register int i;

                        for(i=0; i < boundingNode->currSize; i++)
                        {
                            if( i != found )
                            {
                                tmp_pay = (greatestLowerLeaf->data+temp)->payload;
                                greatestLowerLeaf->data[temp++] = boundingNode->data[i];
                                boundingNode->data[i].payload = tmp_pay;
                            }
                        }
                        greatestLowerLeaf->currSize = temp;
                        if( boundingNode->parent )
                        {
                            TNode * par = boundingNode->parent;
                            
                            if( par->left == boundingNode )
                                par->left = boundingNode->left;
                            else
                                par->right = boundingNode->left;
                            
                            boundingNode->left->parent = par;
                        }
                        else
                        {
                            root = boundingNode->left;
                            root->parent = NULL;
                            id->index = root;
                                
                        }

                        MYFREE(boundingNode, id->buffer_manager);
                        correctHeights(greatestLowerLeaf);
                        check = greatestLowerLeaf;
                    }
                }
                else
                {
                    /* half leaf and  right is not not null: if can be merged with a leaf coalesce the 2 nodes into 1 leaf */
                    TNode * leastUpperLeaf;
                    
                    leastUpperLeaf = findLeastUpperLeaf( boundingNode );
                    /* transfer items from this node to the greatest lower leaf */
                    if( leastUpperLeaf->currSize + boundingNode->currSize - 1 > TNODE_SIZE )
                    {
                        /* they are not mergeable, borrow as much as possible from leastUpperLeaf to boundingNode */
                        /* TODO what to do here actually?? */
                        shiftDownAllItems( boundingNode, found );/*shift down to found-index */
                        boundingNode->currSize--;
#if DEBUG                                        
                        printf("DEBUG: TODO they are not mergeable, what to do here actually?? \n");
#endif                            
                    }
                    else
                    {
                        /* they are mergable  */
                        char * tmp_pay;
                        int j = 0;
                        register int i;

                        if(boundingNode->currSize > 1)
                            shiftUpAllItems( leastUpperLeaf, boundingNode->currSize-1 );
                        for(i=0; i < boundingNode->currSize; i++)
                        {
                            if( i != found )
                            {
                                tmp_pay = leastUpperLeaf->data[j].payload;
                                leastUpperLeaf->data[j++] = boundingNode->data[i];
                                boundingNode->data[i].payload = tmp_pay;
                            }
                        }
                        leastUpperLeaf->currSize = leastUpperLeaf->currSize + boundingNode->currSize - 1;
                        if( boundingNode->parent )
                        {
                            TNode * par = boundingNode->parent;
                            
                            if( par->left == boundingNode )
                                par->left = boundingNode->right;
                            else
                                par->right = boundingNode->right;
                            
                            boundingNode->right->parent = par;
                        }
                        else
                        {
                            root = boundingNode->right;
                            root->parent = NULL;
                            id->index = root;
                        }
                            
                        MYFREE(boundingNode, id->buffer_manager);
                        check = leastUpperLeaf;
                        correctHeights(leastUpperLeaf);
                    }						
                }
            }
				
            if(check)
            {
                /* there is a need for balance check */
                TNode * par;
                TNode * self;
                int     notRotated;
                /* check = check->parent; */
                int     hr, hl;
                    
                while( check != NULL )
                {
                    hr = height(check->right);
                    hl = height(check->left);
                    check->balance = hr - hl;
                    check->height = 1+max(hl,hr);
                    par = check->parent;
                    notRotated = 0;
                    if( check->balance == 2 )
                    {
                        if( check->right->balance != -1 )
                        {
                            /* RR single rotation */
                            if( !par )
                                rotateWithRightChild(&root);
                            else if( par->left == check )
                                self = rotateWithRightChild(&par->left);
                            else
                                self = rotateWithRightChild(&par->right);
                        }
                        else
                        {
                            /* RL double rotation */
                            /* Is it the special case?? */
                            if( isHalfLeaf(check) && isHalfLeaf(check->right) 
                                && isLeaf(check->right->left) && check->right->left->currSize == 1 )
                            {
                                /* move items from check->right to check->right->left */
                                TNode *  c     = check->right->left;
                                TNode *  b     = check->right;
                                char    *ptr;                                
                                int      b_len = b->currSize-1;
                                register int b_idx;
                                
                                for(b_idx = 0; b_idx < b_len; b_idx++)
                                {
                                    ptr = c->data[b_idx+1].payload;
                                    c->data[b_idx+1] = b->data[b_idx];
                                    b->data[b_idx].payload = ptr;
                                }
                                
                                c->currSize = b_idx+1;
                                ptr = b->data[0].payload;
                                b->data[0] = b->data[b_len];
                                b->data[b_len].payload = ptr;
                                b->currSize = 1;
                            }
								
                            if( !par )
                            {
                                rotateWithLeftChild( &root->right );
                                rotateWithRightChild(&root);
                            }
                            else if( par->left == check )
                            {
                                rotateWithLeftChild( &par->left->right );
                                self = rotateWithRightChild( &par->left );
                            }
                            else
                            {
                                rotateWithLeftChild( &par->right->right );
                                self = rotateWithRightChild( &par->right );
                            }
                        }
                    }
                    else if( check->balance == -2 )
                    {
                        /*left sub-tree outweighs*/
                        if( check->left->balance == 1 )
                        {
                            /* LR double double rotation */
                            /* Is it the special case?? */
                            if( isHalfLeaf(check) && isHalfLeaf(check->left) 
                                && isLeaf(check->left->right) && check->left->right->currSize == 1 )
                            {
                                /* move items from check->left to check->left->right */
                                TNode *  c     = check->left->right;
                                TNode *  b     = check->left;
                                char *   tmp_pay;
                                MyRecord temp  = c->data[0];                                
                                register int      b_idx = 1;

                                for(;b_idx < b->currSize; b_idx++)
                                {
                                    tmp_pay = c->data[b_idx-1].payload;
                                    c->data[b_idx-1] = b->data[b_idx];
                                    b->data[b_idx].payload = tmp_pay;
                                }
                                
                                tmp_pay = c->data[b_idx-1].payload;
                                c->data[b_idx-1] = temp;
                                b->data->payload = tmp_pay;
                                c->currSize = b_idx;
                                b->currSize = 1;
                            }
								
                            if( !par )
                            {
                                rotateWithRightChild(&root->left);
                                self = rotateWithLeftChild(&root);
                            }
                            else if( par->left == check )
                            {
                                rotateWithRightChild(&par->left->left);
                                self = rotateWithLeftChild(&par->left);
                            }
                            else
                            {
                                rotateWithRightChild(&par->right->left);
                                self = rotateWithLeftChild(&par->right);
                            }
                        }
                        else
                        {
                            /* LL single rotation */
                            if( !par )
                                rotateWithLeftChild(&root);
                            else if( par->left == check )
                                self = rotateWithLeftChild(&par->left);
                            else
                                self = rotateWithLeftChild(&par->right);
                        }						
                    }
                    else
                        notRotated = 1;

                    if(!notRotated)
                    {
                        /*there has been a rotation*/
                        if( !par )
                        {
                            root->parent = NULL;
                            id->index = root;
                        }	
                        else
                            self->parent = par;

                            
                        check->balance = height(check->right) - height(check->left);

                    }
                    check = par;
                }
            }
        }
    }
    else
    {
#if DEBUG                    
        printf("DEBUG: DELETE value not found %d\n",item->keyval.shortkey);
#endif        
        return KEY_NOTFOUND;
    }
    
    CHECK_VARCHAR(id->index);
    return SUCCESS;
}

ErrCode deleteOneOrAllRecords_short(IndexData * id, RecordWrapper * record, XACTState * xactState)
{
    RecordWrapper recwrap;
    KeyVal        tmpKeyval;
    
    TNodeShort * boundingNode;
    TNodeShort * check = NULL;
    TNodeShort * root  = id->index;

    const char * payload = record->payload;
    int32_t      key     = record->key->shortkey;
    int          isFound = 0;
    
    boundingNode = findBoundingNode_short(root, record->key, &isFound);
	
    if( isFound )
    {
        int found;

        found = binarySearch_short( boundingNode, key );

        if( found != -1 )
        {
            int searchPayload = (payload != NULL && *(payload) != '\0');
            
            if(searchPayload)
            {
                char *    foundPayload = (boundingNode->payloads+found)->payload;
                BSTNode * foundPtree   = (boundingNode->payloads+found)->payloadTree;
                
                int pay_found = (mystrcmp( foundPayload, payload ) == 0);

                if(pay_found)
                {

                    if(foundPtree)
                    {
                        const char * min_pay = bst_first(foundPtree);

                        if(xactState)
                        {
                            recwrap.key = record->key;
                            recwrap.payload = payload;
                            recwrap.payloadTree = NULL;
                            
                            addToPendingList(id, xactState, &recwrap, 1);
                        }

                        PAYLOAD_COPY(foundPayload, min_pay);
                        bst_delete(&foundPtree, min_pay);
                        return SUCCESS;
                    }
                }
                else if(foundPtree)
                {
                    
                    pay_found = bst_search(foundPtree, payload);
                    if(!pay_found)
                        return ENTRY_DNE;

                }

                if(xactState)
                {
                    recwrap.key = record->key;
                    recwrap.payload = payload;
                    recwrap.payloadTree = NULL;
                }
            }
            else if(xactState)
            {
                tmpKeyval.shortkey = key;
                recwrap.key = &tmpKeyval;
                recwrap.payload = (boundingNode->payloads+found)->payload;
                recwrap.payloadTree = (boundingNode->payloads+found)->payloadTree;
            }
            
            /* add the item to delete List for the transaction */
            if(xactState)
                addToPendingList(id, xactState, &recwrap, 1);
            
            if( boundingNode->currSize > SHORT_MIN_TNODE_SIZE )
            {
                shiftDownAllItems_short( boundingNode, found );/*shift down to found-index */
                boundingNode->currSize--;
                if(boundingNode->currSize==found)
                    boundingNode->maxKey = boundingNode->keys[boundingNode->currSize-1];
            }
            else
            {
                /* Underflow situation */
                if( boundingNode->left != NULL && boundingNode->right != NULL )
                {
                    /* Internal node */
                    PayloadWrapper temp_pay;                    
                    TNodeShort *   greatestLowerLeaf;
                    int            size = boundingNode->currSize;


                    if(found == size-1)
                        boundingNode->maxKey = boundingNode->keys[found-1];
                    
                    boundingNode->currSize = found;
                    shiftUpAllItems_short( boundingNode, 1 );/*shift up items from 0 to index-found*/
                    greatestLowerLeaf = findGreatestLowerLeaf_short( boundingNode );
                    temp_pay = boundingNode->payloads[0];
                    greatestLowerLeaf->currSize--;
                    greatestLowerLeaf->maxKey = greatestLowerLeaf->keys[greatestLowerLeaf->currSize-1];
                    boundingNode->keys[0] = greatestLowerLeaf->keys[greatestLowerLeaf->currSize];
                    boundingNode->payloads[0] = greatestLowerLeaf->payloads[greatestLowerLeaf->currSize];
                    greatestLowerLeaf->payloads[greatestLowerLeaf->currSize] = temp_pay;

                    boundingNode->currSize = size;

                    /*NEW CODE*/
                    if(greatestLowerLeaf->currSize==0)
                    {
                        check = greatestLowerLeaf->parent;
                        if(greatestLowerLeaf->left)
                        {
                            /* we can not simply delete greatestLowerLeaf */
                            TNodeShort * gllPar = greatestLowerLeaf->parent;
                            short        hr, hl;
                            
                            if(gllPar->left==greatestLowerLeaf)
                                gllPar->left = greatestLowerLeaf->left;
                            else
                                gllPar->right = greatestLowerLeaf->left;
                            hr = height_short(gllPar->right);
                            hl = height_short(gllPar->left);
                            gllPar->height = 1+max(hr,hl);
                            gllPar->balance = hr-hl;
                            greatestLowerLeaf->left->parent = gllPar;
                        }
                        else
                            if(check->left == greatestLowerLeaf)
                                check->left = NULL;
                            else
                                check->right = NULL;

                        correctHeights_short(check);
                        MYFREE(greatestLowerLeaf, id->buffer_manager);
                        
                    }
                }
                else if( boundingNode->left == NULL && boundingNode->right == NULL )
                {
                    /* this is a leaf node, underflow is permitted */
                    shiftDownAllItems_short( boundingNode, found );/*shift down to found-index */
                    boundingNode->currSize--;
                    if(found == boundingNode->currSize)
                        boundingNode->maxKey = boundingNode->keys[found-1];
                    
                    /* TODO what if currSize becomes 0 */
                    if(boundingNode->currSize==0 && boundingNode->parent)/*if not root*/
                    {
                        check = boundingNode->parent;
                        if( check->left == boundingNode )
                            check->left = NULL;
                        else
                            check->right = NULL;
                        correctHeights_short(check);
                        MYFREE(boundingNode, id->buffer_manager);
                    }
                }
                else
                {
                    /* half-leaf case */
                    if( boundingNode->left )
                    {
                        /* half leaf and left is not not null: if can be merged with a leaf coalesce the 2 nodes into 1 leaf */
                        TNodeShort * greatestLowerLeaf;
                        
                        greatestLowerLeaf = findGreatestLowerLeaf_short( boundingNode );
                        /* transfer items from this node to the greatest lower leaf */
                        if( greatestLowerLeaf->currSize + boundingNode->currSize - 1 > SHORT_TNODE_SIZE )
                        {
                            /* they are not mergeable, borrow as much as possible from greatestLowerLeaf to boundingNode */
                            /* TODO what to do here actually?? */
                            shiftDownAllItems_short( boundingNode, found );/*shift down to found-index */
                            boundingNode->currSize--;
                            if(found == boundingNode->currSize)
                                boundingNode->maxKey = boundingNode->keys[found-1];
                            
#if DEBUG                                        
                            printf("DEBUG: TODO they are not mergeable, what to do here actually?? \n");
#endif                            
                        }
                        else
                        {
                            /* they are mergable  */
                            PayloadWrapper tmp_pay;
                            int            temp = greatestLowerLeaf->currSize;
                            register int i;

                            for(i=0; i < boundingNode->currSize; i++)
                            {
                                if( i != found )
                                {
                                    tmp_pay = greatestLowerLeaf->payloads[temp];
                                    greatestLowerLeaf->keys[temp] = boundingNode->keys[i];                                    
                                    greatestLowerLeaf->payloads[temp++] = boundingNode->payloads[i];
                                    boundingNode->payloads[i] = tmp_pay;
                                }
                            }
                            greatestLowerLeaf->currSize = temp;
                            greatestLowerLeaf->maxKey = greatestLowerLeaf->keys[temp-1];
                            
                            if( boundingNode->parent )
                            {
                                TNodeShort * par = boundingNode->parent;
                                
                                if( par->left == boundingNode )
                                    par->left = boundingNode->left;
                                else
                                    par->right = boundingNode->left;
                                boundingNode->left->parent = par;
                            }
                            else
                            {
                                root = boundingNode->left;
                                root->parent = NULL;
                                id->index = root;
                            }

                            MYFREE(boundingNode, id->buffer_manager);
                            correctHeights_short(greatestLowerLeaf);
                            check = greatestLowerLeaf;
                        }
                    }
                    else
                    {
                        /* half leaf and  right is not not null: if can be merged with a leaf coalesce the 2 nodes into 1 leaf */
                        TNodeShort * leastUpperLeaf;
                        
                        leastUpperLeaf = findLeastUpperLeaf_short( boundingNode );
                        /* transfer items from this node to the least upper leaf */
                        if( leastUpperLeaf->currSize + boundingNode->currSize - 1 > SHORT_TNODE_SIZE )
                        {
                            /* they are not mergeable, borrow as much as possible from leastUpperLeaf to boundingNode */
                            /* TODO what to do here actually?? */
                            shiftDownAllItems_short( boundingNode, found );/*shift down to found-index */
                            boundingNode->currSize--;
                            if(found == boundingNode->currSize)
                                boundingNode->maxKey = boundingNode->keys[found-1];
                            
#if DEBUG                                        
                            printf("DEBUG: TODO they are not mergeable, what to do here actually?? \n");
#endif                            
                        }
                        else
                        {
                            /* they are mergable  */
							PayloadWrapper tmp_pay;                            
                            int j = 0;
                            register int i;

                            
                            if(boundingNode->currSize > 1)
                                shiftUpAllItems_short( leastUpperLeaf, boundingNode->currSize-1 );
                            for(i=0; i < boundingNode->currSize; i++)
                            {
                                if( i != found )
                                {
                                    tmp_pay = leastUpperLeaf->payloads[j];
                                    leastUpperLeaf->keys[j] = boundingNode->keys[i];
                                    leastUpperLeaf->payloads[j++] = boundingNode->payloads[i];
                                    boundingNode->payloads[i] = tmp_pay;
                                }
                            }
                            leastUpperLeaf->currSize = leastUpperLeaf->currSize + boundingNode->currSize - 1;
                            if( boundingNode->parent )
                            {
                                TNodeShort * par = boundingNode->parent;
                                
                                if( par->left == boundingNode )
                                    par->left = boundingNode->right;
                                else
                                    par->right = boundingNode->right;
                                boundingNode->right->parent = par;
                            }
                            else
                            {
                                root = boundingNode->right;
                                root->parent = NULL;
                                id->index = root;
                            }
                            
                            MYFREE(boundingNode, id->buffer_manager);
                            check = leastUpperLeaf;
                            correctHeights_short(leastUpperLeaf);
                        }						
                    }
                }
				
                if(check)
                {
                    /* there is a need for balance check */
                    TNodeShort * par;
                    TNodeShort * self;
                    int          notRotated;
                    /* check = check->parent; */
					int          hr, hl;
                    
                    while( check != NULL )
                    {
                        hr = height_short(check->right);
                        hl = height_short(check->left);
                        check->balance = hr - hl;
                        check->height = 1+max(hl,hr);
                        par = check->parent;
                        notRotated = 0;
                        if( check->balance == 2 )
                        {
                            if( check->right->balance != -1 )
                            {
                                /* RR single rotation */
                                if( !par )
                                    rotateWithRightChild_short(&root);
                                else 
                                    if( par->left == check )
                                        self = rotateWithRightChild_short(&par->left);
                                    else
                                        self = rotateWithRightChild_short(&par->right);
                            }
                            else
                            {
                                /* RL double rotation */
                                /* Is it the special case?? */
                                if( isHalfLeaf_short(check) && isHalfLeaf_short(check->right) 
                                   && isLeaf_short(check->right->left) && check->right->left->currSize == 1 )
                                {
                                    /* move items from check->right to check->right->left */
                                    PayloadWrapper ptr;
                                    TNodeShort *   c     = check->right->left;
                                    TNodeShort *   b     = check->right;
                                    int            b_len = b->currSize-1;
                                    register int   b_idx;
                                    
                                    for(b_idx = 0; b_idx < b_len; b_idx++)
                                    {
                                        ptr = c->payloads[b_idx+1];
                                        c->keys[b_idx+1] = b->keys[b_idx];
                                        c->payloads[b_idx+1] = b->payloads[b_idx];
                                        b->payloads[b_idx] = ptr;
                                    }
                                    c->currSize = b_idx+1;
                                    c->maxKey = c->keys[b_idx];
                                    ptr = b->payloads[0];
                                    b->keys[0] = b->keys[b_len];
                                    b->payloads[0] = b->payloads[b_len];
                                    b->payloads[b_len] = ptr;
                                    b->currSize = 1;
                                    b->maxKey = b->keys[0];
                                }
								
                                if( !par )
                                {
                                    rotateWithLeftChild_short( &root->right );
                                    rotateWithRightChild_short(&root);
                                }
                                else 
                                    if( par->left == check )
                                    {
                                        rotateWithLeftChild_short( &par->left->right );
                                        self = rotateWithRightChild_short( &par->left );
                                    }
                                    else
                                    {
                                        rotateWithLeftChild_short( &par->right->right );
                                        self = rotateWithRightChild_short( &par->right );
                                    }
                            }
                        }
                        else if( check->balance == -2 )
                        {
                            /*left sub-tree outweighs*/
                            if( check->left->balance == 1 )
                            {
                                /* LR double double rotation */
                                /* Is it the special case?? */
                                if( isHalfLeaf_short(check) && isHalfLeaf_short(check->left) 
                                   && isLeaf_short(check->left->right) && check->left->right->currSize == 1 )
                                {
                                    /* move items from check->left to check->left->right */
                                    TNodeShort *   c         = check->left->right;
                                    TNodeShort *   b         = check->left;
                                    int32_t        temp_key  = c->keys[0];
                                    PayloadWrapper temp_data = c->payloads[0];
									PayloadWrapper tmp_pay;
                                    register int            b_idx     = 1;
                                    
                                    for(;b_idx < b->currSize; b_idx++)
                                    {
                                        tmp_pay = c->payloads[b_idx-1];
                                        c->keys[b_idx-1] = b->keys[b_idx];
                                        c->payloads[b_idx-1] = b->payloads[b_idx];
                                        b->payloads[b_idx] = tmp_pay;
                                    }
                                    tmp_pay = c->payloads[b_idx-1];
                                    c->keys[b_idx-1] = temp_key;
                                    c->maxKey = temp_key;
                                    c->payloads[b_idx-1] = temp_data;
                                    b->payloads[0] = tmp_pay;
                                    c->currSize = b_idx;
                                    b->currSize = 1;
                                    b->maxKey = b->keys[0];
                                }
								
                                if( !par )
                                {
                                    rotateWithRightChild_short(&root->left);
                                    self = rotateWithLeftChild_short(&root);
                                }
                                else
                                    if( par->left == check )
                                    {
                                        rotateWithRightChild_short(&par->left->left);
                                        self = rotateWithLeftChild_short(&par->left);
                                    }
                                    else
                                    {
                                        rotateWithRightChild_short(&par->right->left);
                                        self = rotateWithLeftChild_short(&par->right);
                                    }
                            }
                            else
                            {
                                /* LL single rotation */
                                if( !par )
                                    rotateWithLeftChild_short(&root);
                                else
                                    if( par->left == check )
                                        self = rotateWithLeftChild_short(&par->left);
                                    else
                                        self = rotateWithLeftChild_short(&par->right);
                            }						
                        }
                        else
                            notRotated = 1;

                        if(!notRotated)
                        {
                            /*there has been a rotation*/
                            if( !par )
                            {
                                root->parent = NULL;
                                id->index = root;
                            }	
                            else
                                self->parent = par;

                            
                            check->balance = height_short(check->right) - height_short(check->left);
                        }
                        check = par;
                    }
                }
            }

            
        }
        else
        {
#if DEBUG                        
            printf("DEBUG: DELETE key found but payload not found %d\n",item->keyval.shortkey);
#endif            
            return KEY_NOTFOUND;/*ENTRY_DNE;*/
        }
    }
    else
    {
#if DEBUG                    
        printf("DEBUG: DELETE value not found %d\n",item->keyval.shortkey);
#endif        
        return KEY_NOTFOUND;
    }
    
    CHECK_SHORT(id->index);
    return SUCCESS;
}

ErrCode deleteOneOrAllRecords_int(IndexData * id, RecordWrapper * record, XACTState * xactState)
{
    RecordWrapper recwrap;
    KeyVal        tmpKeyval;

    TNodeInt *   boundingNode;
    TNodeInt *   check   = NULL;
    TNodeInt *   root    = id->index;
    const char * payload = record->payload;
    
    int64_t key     = record->key->intkey;
    int     isFound = 0;
    
    boundingNode = findBoundingNode_int(root, record->key, &isFound);
	
    if( isFound )
    {
        int found;

        found = binarySearch_int( boundingNode, key );

        if( found != -1 )
        {
            int searchPayload = (payload != NULL && *(payload) != '\0');
            
            if(searchPayload)
            {
                char *    foundPayload = boundingNode->payloads[found].payload;
                BSTNode * foundPtree   = boundingNode->payloads[found].payloadTree;
                
                int pay_found = (mystrcmp( foundPayload, payload ) == 0);

                if(pay_found)
                {

                    if(foundPtree)
                    {
                        const char * min_pay = bst_first(foundPtree);

                        if(xactState)
                        {
                            recwrap.key = record->key;
                            recwrap.payload = payload;
                            recwrap.payloadTree = NULL;
                            
                            addToPendingList(id, xactState, &recwrap, 1);
                        }

                        PAYLOAD_COPY(foundPayload, min_pay);
                        bst_delete(&foundPtree, min_pay);
                        return SUCCESS;
                    }
                }
                else if(foundPtree)
                {
                    
                    pay_found = bst_search(foundPtree, payload);
                    if(!pay_found)
                        return ENTRY_DNE;

                }

                if(xactState)
                {
                    recwrap.key = record->key;
                    recwrap.payload = payload;
                    recwrap.payloadTree = NULL;
                }
            }
            else if(xactState)
            {
                tmpKeyval.intkey = key;
                recwrap.key = &tmpKeyval;
                recwrap.payload = boundingNode->payloads[found].payload;
                recwrap.payloadTree = boundingNode->payloads[found].payloadTree;
            }
            
            /* add the item to delete List for the transaction */
            if(xactState)
                addToPendingList(id, xactState, &recwrap, 1);
            
            if( boundingNode->currSize > INT_MIN_TNODE_SIZE )
            {
                shiftDownAllItems_int( boundingNode, found );/*shift down to found-index */
                boundingNode->currSize--;
                if(boundingNode->currSize==found)
                    boundingNode->maxKey = boundingNode->keys[boundingNode->currSize-1];
            }
            else
            {
                /* Underflow situation */
                if( boundingNode->left != NULL && boundingNode->right != NULL )
                {
                    /* Internal node */
                    PayloadWrapper temp_pay;                    
                    TNodeInt *     greatestLowerLeaf;
                    int            size = boundingNode->currSize;


                    if(found == size-1)
                        boundingNode->maxKey = boundingNode->keys[found-1];
                    
                    boundingNode->currSize = found;
                    shiftUpAllItems_int( boundingNode, 1 );/*shift up items from 0 to index-found*/
                    greatestLowerLeaf = findGreatestLowerLeaf_int( boundingNode );
                    temp_pay = boundingNode->payloads[0];
                    greatestLowerLeaf->currSize--;
                    greatestLowerLeaf->maxKey = greatestLowerLeaf->keys[greatestLowerLeaf->currSize-1];
                    boundingNode->keys[0] = greatestLowerLeaf->keys[greatestLowerLeaf->currSize];
                    boundingNode->payloads[0] = greatestLowerLeaf->payloads[greatestLowerLeaf->currSize];
                    greatestLowerLeaf->payloads[greatestLowerLeaf->currSize] = temp_pay;

                    boundingNode->currSize = size;

                    /*NEW CODE*/
                    if(greatestLowerLeaf->currSize==0)
                    {
                        check = greatestLowerLeaf->parent;
                        if(greatestLowerLeaf->left)
                        {
                            /* we can not simply delete greatestLowerLeaf */
                            TNodeInt * gllPar = greatestLowerLeaf->parent;
                            int hr, hl;
                            
                            if(gllPar->left==greatestLowerLeaf)
                                gllPar->left = greatestLowerLeaf->left;
                            else
                                gllPar->right = greatestLowerLeaf->left;
                            hr = height_int(gllPar->right);
                            hl = height_int(gllPar->left);
                            
                            gllPar->height = 1+max(hr,hl);
                            gllPar->balance = hr-hl;
                            greatestLowerLeaf->left->parent = gllPar;
                        }
                        else
                            if(check->left == greatestLowerLeaf)
                                check->left = NULL;
                            else
                                check->right = NULL;

                        correctHeights_int(check);
                        MYFREE(greatestLowerLeaf, id->buffer_manager);
                        
                    }
                }
                else if( boundingNode->left == NULL && boundingNode->right == NULL )
                {
                    /* this is a leaf node, underflow is permitted */
                    shiftDownAllItems_int( boundingNode, found );/*shift down to found-index */
                    boundingNode->currSize--;
                    if(found == boundingNode->currSize)
                        boundingNode->maxKey = boundingNode->keys[found-1];
                    
                    /* TODO what if currSize becomes 0 */
                    if(boundingNode->currSize==0 && boundingNode->parent)/*if not root*/
                    {
                        check = boundingNode->parent;
                        if( check->left == boundingNode )
                            check->left = NULL;
                        else
                            check->right = NULL;
                        correctHeights_int(check);
                        MYFREE(boundingNode, id->buffer_manager);
                    }
                }
                else
                {
                    /* half-leaf case */
                    if( boundingNode->left )
                    {
                        /* half leaf and left is not not null: if can be merged with a leaf coalesce the 2 nodes into 1 leaf */
                        TNodeInt * greatestLowerLeaf;
                        
                        greatestLowerLeaf = findGreatestLowerLeaf_int( boundingNode );
                        /* transfer items from this node to the greatest lower leaf */
                        if( greatestLowerLeaf->currSize + boundingNode->currSize - 1 > INT_TNODE_SIZE )
                        {
                            /* they are not mergeable, borrow as much as possible from greatestLowerLeaf to boundingNode */
                            /* TODO what to do here actually?? */
                            shiftDownAllItems_int( boundingNode, found );/*shift down to found-index */
                            boundingNode->currSize--;
                            if(found == boundingNode->currSize)
                                boundingNode->maxKey = boundingNode->keys[found-1];
                            
#if DEBUG                                        
                            printf("DEBUG: TODO they are not mergeable, what to do here actually?? \n");
#endif                            
                        }
                        else
                        {
                            /* they are mergable  */
                            PayloadWrapper tmp_pay;                            
                            int temp = greatestLowerLeaf->currSize;
                            register int i;

                            for(i=0; i < boundingNode->currSize; i++)
                            {
                                if( i != found )
                                {
                                    tmp_pay = greatestLowerLeaf->payloads[temp];
                                    greatestLowerLeaf->keys[temp] = boundingNode->keys[i];                                    
                                    greatestLowerLeaf->payloads[temp++] = boundingNode->payloads[i];
                                    boundingNode->payloads[i] = tmp_pay;
                                }
                            }
                            greatestLowerLeaf->currSize = temp;
                            greatestLowerLeaf->maxKey = greatestLowerLeaf->keys[temp-1];
                            
                            if( boundingNode->parent )
                            {
                                TNodeInt * par = boundingNode->parent;
                                
                                if( par->left == boundingNode )
                                    par->left = boundingNode->left;
                                else
                                    par->right = boundingNode->left;
                                boundingNode->left->parent = par;
                            }
                            else
                            {
                                root = boundingNode->left;
                                root->parent = NULL;
                                id->index = root;
                            }

                            MYFREE(boundingNode, id->buffer_manager);
                            correctHeights_int(greatestLowerLeaf);
                            check = greatestLowerLeaf;
                        }
                    }
                    else
                    {
                        /* half leaf and  right is not not null: if can be merged with a leaf coalesce the 2 nodes into 1 leaf */
                        TNodeInt * leastUpperLeaf;
                        
                        leastUpperLeaf = findLeastUpperLeaf_int( boundingNode );
                        /* transfer items from this node to the greatest lower leaf */
                        if( leastUpperLeaf->currSize + boundingNode->currSize - 1 > INT_TNODE_SIZE )
                        {
                            /* they are not mergeable, borrow as much as possible from leastUpperLeaf to boundingNode */
                            /* TODO what to do here actually?? */
                            shiftDownAllItems_int( boundingNode, found );/*shift down to found-index */
                            boundingNode->currSize--;
                            if(found == boundingNode->currSize)
                                boundingNode->maxKey = boundingNode->keys[found-1];
                            
#if DEBUG                                        
                            printf("DEBUG: TODO they are not mergeable, what to do here actually?? \n");
#endif                            
                        }
                        else
                        {
                            /* they are mergable  */
							PayloadWrapper tmp_pay;                            
                            int j = 0;
                            register int i;
                            
                            if(boundingNode->currSize > 1)
                                shiftUpAllItems_int( leastUpperLeaf, boundingNode->currSize-1 );
                            for(i=0; i < boundingNode->currSize; i++)
                            {
                                if( i != found )
                                {
                                    tmp_pay = leastUpperLeaf->payloads[j];
                                    leastUpperLeaf->keys[j] = boundingNode->keys[i];
                                    leastUpperLeaf->payloads[j++] = boundingNode->payloads[i];
                                    boundingNode->payloads[i] = tmp_pay;
                                }
                            }
                            leastUpperLeaf->currSize = leastUpperLeaf->currSize + boundingNode->currSize - 1;
                            if( boundingNode->parent )
                            {
                                TNodeInt * par = boundingNode->parent;
                                
                                if( par->left == boundingNode )
                                    par->left = boundingNode->right;
                                else
                                    par->right = boundingNode->right;
                                boundingNode->right->parent = par;
                            }
                            else
                            {
                                root = boundingNode->right;
                                root->parent = NULL;
                                id->index = root;
                            }
                            
                            MYFREE(boundingNode, id->buffer_manager);
                            check = leastUpperLeaf;
                            correctHeights_int(leastUpperLeaf);
                        }						
                    }
                }
				
                if(check)
                {
                    /* there is a need for balance check */
                    TNodeInt * par;
                    TNodeInt * self;
                    int        notRotated;
                    /* check = check->parent; */
					int        hr, hl;
                    
                    while( check != NULL )
                    {
                        hr = height_int(check->right);
                        hl = height_int(check->left);
                        check->balance = hr - hl;
                        check->height = 1+max(hl,hr);
                        par = check->parent;
                        notRotated = 0;
                        if( check->balance == 2 )
                        {
                            if( check->right->balance != -1 )
                            {
                                /* RR single rotation */
                                if( !par )
                                    rotateWithRightChild_int(&root);
                                else 
                                    if( par->left == check )
                                        self = rotateWithRightChild_int(&par->left);
                                    else
                                        self = rotateWithRightChild_int(&par->right);
                            }
                            else
                            {
                                /* RL double rotation */
                                /* Is it the special case?? */
                                if( isHalfLeaf_int(check) && isHalfLeaf_int(check->right) 
                                   && isLeaf_int(check->right->left) && check->right->left->currSize == 1 )
                                {
                                    /* move items from check->right to check->right->left */
                                    PayloadWrapper ptr;                                    
                                    TNodeInt *     c     = check->right->left;
                                    TNodeInt *     b     = check->right;
                                    int            b_len = b->currSize-1;
                                    register int b_idx;
                                    
                                    for(b_idx = 0; b_idx < b_len; b_idx++)
                                    {
                                        ptr = c->payloads[b_idx+1];
                                        c->keys[b_idx+1] = b->keys[b_idx];
                                        c->payloads[b_idx+1] = b->payloads[b_idx];
                                        b->payloads[b_idx] = ptr;
                                    }
                                    c->currSize = b_idx+1;
                                    c->maxKey = c->keys[b_idx];
                                    ptr = b->payloads[0];
                                    b->keys[0] = b->keys[b_len];
                                    b->payloads[0] = b->payloads[b_len];
                                    b->payloads[b_len] = ptr;
                                    b->currSize = 1;
                                    b->maxKey = b->keys[0];
                                }
								
                                if( !par )
                                {
                                    rotateWithLeftChild_int( &root->right );
                                    rotateWithRightChild_int(&root);
                                }
                                else 
                                    if( par->left == check )
                                    {
                                        rotateWithLeftChild_int( &par->left->right );
                                        self = rotateWithRightChild_int( &par->left );
                                    }
                                    else
                                    {
                                        rotateWithLeftChild_int( &par->right->right );
                                        self = rotateWithRightChild_int( &par->right );
                                    }
                            }
                        }
                        else if( check->balance == -2 )
                        {
                            /*left sub-tree outweighs*/
                            if( check->left->balance == 1 )
                            {
                                /* LR double double rotation */
                                /* Is it the special case?? */
                                if( isHalfLeaf_int(check) && isHalfLeaf_int(check->left) 
                                   && isLeaf_int(check->left->right) && check->left->right->currSize == 1 )
                                {
                                    /* move items from check->left to check->left->right */
                                    TNodeInt *     c         = check->left->right;
                                    TNodeInt *     b         = check->left;
                                    int64_t        temp_key  = c->keys[0];
                                    PayloadWrapper temp_data = c->payloads[0];
									PayloadWrapper tmp_pay;
                                    register int   b_idx     = 1;
                                    
                                    for(;b_idx < b->currSize; b_idx++)
                                    {
                                        tmp_pay = c->payloads[b_idx-1];
                                        c->keys[b_idx-1] = b->keys[b_idx];
                                        c->payloads[b_idx-1] = b->payloads[b_idx];
                                        b->payloads[b_idx] = tmp_pay;
                                    }
                                    tmp_pay = c->payloads[b_idx-1];
                                    c->keys[b_idx-1] = temp_key;
                                    c->maxKey = temp_key;
                                    c->payloads[b_idx-1] = temp_data;
                                    b->payloads[0] = tmp_pay;
                                    c->currSize = b_idx;
                                    b->currSize = 1;
                                    b->maxKey = b->keys[0];
                                }
								
                                if( !par )
                                {
                                    rotateWithRightChild_int(&root->left);
                                    self = rotateWithLeftChild_int(&root);
                                }
                                else
                                    if( par->left == check )
                                    {
                                        rotateWithRightChild_int(&par->left->left);
                                        self = rotateWithLeftChild_int(&par->left);
                                    }
                                    else
                                    {
                                        rotateWithRightChild_int(&par->right->left);
                                        self = rotateWithLeftChild_int(&par->right);
                                    }
                            }
                            else
                            {
                                /* LL single rotation */
                                if( !par )
                                    rotateWithLeftChild_int(&root);
                                else
                                    if( par->left == check )
                                        self = rotateWithLeftChild_int(&par->left);
                                    else
                                        self = rotateWithLeftChild_int(&par->right);
                            }						
                        }
                        else
                            notRotated = 1;

                        if(!notRotated)
                        {
                            /*there has been a rotation*/
                            if( !par )
                            {
                                root->parent = NULL;
                                id->index = root;
                            }	
                            else
                                self->parent = par;

                            
                            check->balance = height_int(check->right) - height_int(check->left);
                        }
                        check = par;
                    }
                }
            }

            
        }
        else
        {
#if DEBUG                        
            printf("DEBUG: DELETE key found but payload not found %d\n",item->keyval.shortkey);
#endif            
            return KEY_NOTFOUND;/*ENTRY_DNE;*/
        }
    }
    else
    {
#if DEBUG                    
        printf("DEBUG: DELETE value not found %d\n",item->keyval.shortkey);
#endif        
        return KEY_NOTFOUND;
    }
    
    CHECK_INT(id->index);
    return SUCCESS;
}

int insertIntoNode(TNode * node, const KeyVal * _key, const char * payload)
{
    MyRecord *   ptr = node->data+node->currSize;
    MyRecord *   beg = node->data, * ptr2;
    char *       tmp = ptr->payload;
    const char * key = _key->charkey;
    
    while( ptr != beg )
    {
        ptr2 = ptr-1;        
        if(strcmp( key, ptr2->key.charkey ) > 0 )
            break;

        *ptr = *ptr2;
        ptr = ptr2;
    }

    ptr->payload = tmp;
    ptr->key = *_key;
    ptr->payloadTree = NULL;
    PAYLOAD_COPY(ptr->payload, payload);

    node->currSize++;
 
    return 0;           
}

int insertIntoNode_short(TNodeShort * node, const KeyVal * key, const char * payload)
{
    PayloadWrapper * tmp     = node->payloads;
    PayloadWrapper   tmp_pay = tmp[node->currSize];
    int32_t *        keys    = node->keys;
    register int     i;
    
    for(i=node->currSize-1; i >= 0; i--)
    {
        if( key->shortkey > keys[i] )
            break;

        *(keys+i+1) = *(keys+i);
        *(tmp+i+1) = *(tmp+i);
    }
    ++i;
    *(keys+i) = key->shortkey;
    *(tmp+i) = tmp_pay;
    PAYLOAD_COPY(tmp[i].payload, payload);

    if(i==node->currSize)
        node->maxKey = keys[i];
    
    node->currSize++;
 
    return 0;
}

int insertIntoNode_int(TNodeInt * node, const KeyVal * _key, const char * payload)
{
    PayloadWrapper * tmp     = node->payloads;
    PayloadWrapper   tmp_pay = tmp[node->currSize];
    int64_t *        keys    = node->keys;
    int64_t          key     = _key->intkey;
    register int     i;
    
    for(i=node->currSize-1; i >= 0; i--)
    {
        if( key > keys[i] )
            break;

        *(keys+i+1) = *(keys+i);
        *(tmp+i+1) = *(tmp+i);
    }
    ++i;
    *(keys+i) = key;
    *(tmp+i) = tmp_pay;
    PAYLOAD_COPY(tmp[i].payload, payload);

    if(i==node->currSize)
        node->maxKey = keys[i];
    
    node->currSize++;
 
    return 0;
}

/**
 * The min element is not in the correct place.
 * Corrects its place and puts the node in order
 */
void tidyUpNode( TNode * node )
{
    MyRecord   temp;
    MyRecord * ptr, *ptr_next, *end;
    
    end = node->data+node->currSize;
    ptr = node->data;
    ptr_next = ptr+1;
    
    while( ptr_next != end )
    {
        if(strcmp( ptr->key.charkey, ptr_next->key.charkey ) > 0 )
        { 
            /* swap the items */
            temp = *ptr;
            *ptr = *ptr_next;
            *ptr_next = temp;
        }
        else
            break;
        ptr = ptr_next;
        ptr_next++;
    }
}

/**
 * The min element is not in the correct place.
 * Corrects its place and puts the node in order
 */
void tidyUpNode_short( TNodeShort * node )
{
    PayloadWrapper   temp_payload;
    PayloadWrapper * payloads   = node->payloads;
    int32_t *        keys       = node->keys;
    int32_t          temp_key;
    int              iplus, end = node->currSize-1;
    register int     i;
    
    for(i=0; i < end; i++)
    {
        iplus = i+1;
        if( keys[i] > keys[iplus] )
        {
            /* swap items */
            temp_key = keys[i];
            *(keys+i) = *(keys+iplus);
            *(keys+iplus) = temp_key;
            temp_payload = *(payloads+i);
            *(payloads+i) = *(payloads+iplus);
            *(payloads+iplus) = temp_payload;
        }
        else
            break;
    }
    
    if(i==end)
        node->maxKey = keys[i];/*MAX_KEY*/
        
}

/**
 * The min element is not in the correct place.
 * Corrects its place and puts the node in order
 */
void tidyUpNode_int( TNodeInt * node )
{
    PayloadWrapper   temp_payload;
    PayloadWrapper * payloads   = node->payloads;
    int64_t *        keys       = node->keys;
    int64_t          temp_key;
    int              iplus, end = node->currSize-1;
    register int     i;
    
    for(i=0; i < end; i++)
    {
        iplus = i+1;
        if( keys[i] > keys[iplus] )
        {
            /* swap items */
            temp_key = *(keys+i);
            keys[i] = *(keys+iplus);
            *(keys+iplus) = temp_key;
            temp_payload = *(payloads+i);
            *(payloads+i) = *(payloads+iplus);
            *(payloads+iplus) = temp_payload;
        }
        else
            break;
    }
    
    if(i==end)
        node->maxKey = keys[i];/*MAX_KEY*/
    
}

void shiftDownAllItems( TNode * node, int downto)
{
    MyRecord * ptr = node->data+downto, *ptr2;
    MyRecord * end = node->data+node->currSize;
    char *     tmp = ptr->payload;
    
    while( ptr != end )
    {
        ptr2 = ptr+1;
        *ptr = *ptr2;
        ptr = ptr2;
    }
    (end-1)->payload = tmp;
}

void shiftDownAllItems_short( TNodeShort * node, int downto)
{
    PayloadWrapper   tmp      = node->payloads[downto];
    PayloadWrapper * wrappers = node->payloads;
    int32_t *        keys     = node->keys;
    int              len      = node->currSize;
    register int     i;
    
    for(i=downto+1; i < len ; i++)
    {
        *(keys+i-1) = *(keys+i);
        *(wrappers+i-1) = *(wrappers+i);
    }

    *(wrappers+i-1) = tmp;
}

void shiftDownAllItems_int( TNodeInt * node, int downto)
{
    PayloadWrapper   tmp      = node->payloads[downto];
    PayloadWrapper * wrappers = node->payloads;
    int64_t *        keys     = node->keys;
    int              len      = node->currSize;
    register int     i;
    
    for(i=downto+1; i < len ; i++)
    {
        *(keys+i-1) = *(keys+i);
        *(wrappers+i-1) = *(wrappers+i);
    }

    *(wrappers+i-1) = tmp;
    
}


void shiftUpAllItems( TNode * node, int by )
{
    MyRecord * ptr = node->data+node->currSize-1;
    MyRecord * beg = node->data-1;
    char *     tmp = (ptr+by)->payload;
    
    while( ptr != beg )
    {
        *(ptr+by) = *ptr;
        ptr = ptr-1;
    }
    (beg+1)->payload = tmp;
}

void shiftUpAllItems_short( TNodeShort * node, int by )
{
    int iplusby = node->currSize+by-1;
    register int i;
    
    for(i=node->currSize-1; i >= 0 ; i--, iplusby--)
    {
        node->keys[iplusby] = node->keys[i];
        node->payloads[iplusby] = node->payloads[i];
    }   
}

void shiftUpAllItems_int( TNodeInt * node, int by )
{
    int iplusby = node->currSize+by-1;
    register int i;
    
    for(i=node->currSize-1; i >= 0 ; i--, iplusby--)
    {
        node->keys[iplusby] = node->keys[i];
        node->payloads[iplusby] = node->payloads[i];
    }   
}

inline TNode * findGreatestLowerLeaf(const TNode * node)
{
    TNode * result;

    if( node->left == NULL )
        return NULL;

    result = node->left;

    while( result->right )
        result = result->right;

    return result;
}

inline TNodeShort * findGreatestLowerLeaf_short(const TNodeShort * node)
{
    TNodeShort * result;

    if( node->left == NULL )
        return NULL;

    result = node->left;

    while( result->right )
        result = result->right;

    return result;
}

inline TNodeInt * findGreatestLowerLeaf_int(const TNodeInt * node)
{
    TNodeInt * result;

    if( node->left == NULL )
        return NULL;

    result = node->left;

    while( result->right )
        result = result->right;

    return result;
}

inline TNode * findLeastUpperLeaf(const TNode * node)
{
    TNode * result;

    if( node->right == NULL )
        return NULL;

    result = node->right;

    while( result->left )
        result = result->left;

    return result;
}

inline TNodeShort * findLeastUpperLeaf_short(const TNodeShort * node)
{
    TNodeShort * result;

    if( node->right == NULL )
        return NULL;

    result = node->right;

    while( result->left )
        result = result->left;

    return result;
}

inline TNodeInt * findLeastUpperLeaf_int(const TNodeInt * node)
{
    TNodeInt * result;

    if( node->right == NULL )
        return NULL;

    result = node->right;

    while( result->left )
        result = result->left;

    return result;
}

void correctHeights(TNode * node)
{
    while( node != NULL )
    {
        node->height = max( height(node->left), height(node->right) ) + 1;
        node = node->parent;
    }
}

void correctHeights_short(TNodeShort * node)
{
    while( node != NULL )
    {
        node->height = max( height_short(node->left), height_short(node->right) ) + 1;
        node = node->parent;
    }
}

void correctHeights_int(TNodeInt * node)
{
    while( node != NULL )
    {
        node->height = max( height_int(node->left), height_int(node->right) ) + 1;
        node = node->parent;
    }
}

void insertToGreatestLowerLeaf(TNode * boundingNode, MyRecord * min, TNode ** newLeaf, BufferManager * buf_man)
{
    char *  thisPayload;
    TNode * greatestLowerLeaf = findGreatestLowerLeaf( boundingNode );
    
    if( greatestLowerLeaf != NULL )
    {

        
        if( greatestLowerLeaf->currSize < TNODE_SIZE )
        {
            thisPayload = greatestLowerLeaf->data[greatestLowerLeaf->currSize].payload;
            greatestLowerLeaf->data[greatestLowerLeaf->currSize].payload = min->payload;
            greatestLowerLeaf->data[greatestLowerLeaf->currSize].payloadTree = min->payloadTree;
            greatestLowerLeaf->data[greatestLowerLeaf->currSize].key = min->key;

            min->payload = thisPayload;
            greatestLowerLeaf->currSize++;
        }
        else
        {
            /* this min is greater than any element in greatestLowerLeaf
               so it will be added as a new right child of greatestLowerLeaf */
            TNode * newNode = buf_man->makeTNode(buf_man);
            MyRecord * rec;
            
            newNode->parent = greatestLowerLeaf;

            rec = newNode->data;
            
            thisPayload = rec->payload;
            rec->payload = min->payload;
            rec->payloadTree = min->payloadTree;
            rec->key = min->key;
            
            min->payload = thisPayload;
            newNode->currSize = 1;

            greatestLowerLeaf->right = newNode;
            greatestLowerLeaf->height = max(1, greatestLowerLeaf->height);
		
            correctHeights(greatestLowerLeaf->parent);
            greatestLowerLeaf->balance = -height(greatestLowerLeaf->left);
            *newLeaf = newNode;
        }
                
    }
    else
    {
        /* a new leaf */
        TNode * newNode = buf_man->makeTNode(buf_man);
        MyRecord * rec;
        
        newNode->parent = boundingNode;

        rec = newNode->data;
        thisPayload = rec->payload;
        rec->payload = min->payload;
        rec->payloadTree = min->payloadTree;
        rec->key = min->key;
            
        min->payload = thisPayload;
        
        newNode->currSize = 1;

        boundingNode->left = newNode;
        boundingNode->height = max(1, boundingNode->height);
		
        correctHeights(boundingNode->parent);
        boundingNode->balance = height(boundingNode->right);
        *newLeaf = newNode;
    }
}

void insertToGreatestLowerLeaf_short(TNodeShort * boundingNode, TNodeShort ** newLeaf, BufferManager * buf_man, int index)
{
    int32_t key = boundingNode->keys[index];
    char *  tmp;
    
    TNodeShort * greatestLowerLeaf = findGreatestLowerLeaf_short( boundingNode );
    
    if( greatestLowerLeaf != NULL )
    {
        if( greatestLowerLeaf->currSize < SHORT_TNODE_SIZE )
        {
            PayloadWrapper * ptemp, * btemp;
            
            greatestLowerLeaf->keys[greatestLowerLeaf->currSize] = key;
            ptemp = greatestLowerLeaf->payloads+greatestLowerLeaf->currSize;
            tmp = ptemp->payload;
            btemp = boundingNode->payloads+index;
            ptemp->payload = btemp->payload;
            btemp->payload = tmp;
            ptemp->payloadTree = btemp->payloadTree;
            greatestLowerLeaf->maxKey = key;/*MAX_KEY*/
            greatestLowerLeaf->currSize++;
        }
        else
        {
            /* this min is greater than any element in greatestLowerLeaf
               so it will be added as a new right child of greatestLowerLeaf */
            TNodeShort * newNode = buf_man->makeTNode(buf_man);
            PayloadWrapper * btemp, * ptemp;
            
            newNode->parent = greatestLowerLeaf;
            newNode->keys[0] = key;
            newNode->maxKey = key;/*MAX_KEY*/

            ptemp = newNode->payloads;
            tmp = ptemp->payload;
            btemp = boundingNode->payloads+index;
            ptemp->payload = btemp->payload;
            btemp->payload = tmp;
            
            ptemp->payloadTree = btemp->payloadTree;
            newNode->currSize = 1;

            greatestLowerLeaf->right = newNode;
            greatestLowerLeaf->height = max(1, greatestLowerLeaf->height);
		
            correctHeights_short(greatestLowerLeaf->parent);
            greatestLowerLeaf->balance = -height_short(greatestLowerLeaf->left);
            *newLeaf = newNode;
        }
                
    }
    else
    {
        /* a new leaf */
        TNodeShort * newNode = buf_man->makeTNode(buf_man);
        PayloadWrapper * btemp, * ptemp;
        
        newNode->parent = boundingNode;
        newNode->keys[0] = key;
        newNode->maxKey = key;/*MAX_KEY*/

        ptemp = newNode->payloads;
        tmp = ptemp->payload;
        btemp = boundingNode->payloads+index;
        ptemp->payload = btemp->payload;
        btemp->payload = tmp;

        ptemp->payloadTree = btemp->payloadTree;
        newNode->currSize = 1;

        boundingNode->left = newNode;
        boundingNode->height = max(1, boundingNode->height);
		
        correctHeights_short(boundingNode->parent);
        boundingNode->balance = height_short(boundingNode->right);
        *newLeaf = newNode;
    }
}

void insertToGreatestLowerLeaf_int(TNodeInt * boundingNode, TNodeInt ** newLeaf, BufferManager * buf_man, int index)
{
    int64_t key = boundingNode->keys[index];
    char *  tmp;

    TNodeInt * greatestLowerLeaf = findGreatestLowerLeaf_int( boundingNode );
    
    if( greatestLowerLeaf != NULL )
    {
        if( greatestLowerLeaf->currSize < INT_TNODE_SIZE )
        {
            PayloadWrapper * ptemp, * btemp;
            
            greatestLowerLeaf->keys[greatestLowerLeaf->currSize] = key;
            ptemp = greatestLowerLeaf->payloads+greatestLowerLeaf->currSize;
            tmp = ptemp->payload;
            btemp = boundingNode->payloads+index;
            ptemp->payload = btemp->payload;
            btemp->payload = tmp;
            ptemp->payloadTree = btemp->payloadTree;
            greatestLowerLeaf->maxKey = key;/*MAX_KEY*/
            greatestLowerLeaf->currSize++;
        }
        else
        {
            /* this min is greater than any element in greatestLowerLeaf
               so it will be added as a new right child of greatestLowerLeaf */
            TNodeInt * newNode = buf_man->makeTNode(buf_man);
            PayloadWrapper * btemp, * ptemp;
            
            newNode->parent = greatestLowerLeaf;
            newNode->keys[0] = key;
            newNode->maxKey = key;

            btemp = boundingNode->payloads+index;
            ptemp = newNode->payloads;
            tmp = ptemp->payload;
            ptemp->payload = btemp->payload;
            btemp->payload = tmp;
            
            ptemp->payloadTree = btemp->payloadTree;
            newNode->currSize = 1;

            greatestLowerLeaf->right = newNode;
            greatestLowerLeaf->height = max(1, greatestLowerLeaf->height);
		
            correctHeights_int(greatestLowerLeaf->parent);
            greatestLowerLeaf->balance = -height_int(greatestLowerLeaf->left);
            *newLeaf = newNode;
        }
                
    }
    else
    {
        /* a new leaf */
        TNodeInt * newNode = buf_man->makeTNode(buf_man);
        PayloadWrapper * btemp, * ptemp;
        
        newNode->parent = boundingNode;
        newNode->keys[0] = key;
        newNode->maxKey = key;

        ptemp = newNode->payloads;
        btemp = boundingNode->payloads+index;
        tmp = ptemp->payload;
        ptemp->payload = btemp->payload;
        btemp->payload = tmp;

        ptemp->payloadTree = btemp->payloadTree;
        newNode->currSize = 1;

        boundingNode->left = newNode;
        boundingNode->height = max(1, boundingNode->height);
		
        correctHeights_int(boundingNode->parent);
        boundingNode->balance = height_int(boundingNode->right);
        *newLeaf = newNode;
    }
}

TNode * rotateWithRightChild(TNode ** ptrToThisNode)
{
    TNode * thisNode     = *ptrToThisNode;
    TNode * thisRightSub = thisNode->right;
    int     hr, hl;
    
    thisNode->right = thisRightSub->left;
    if(thisRightSub->left)
        thisRightSub->left->parent = thisNode;
 
    thisRightSub->left = thisNode;
    hl = height(thisNode->left);
    hr = height(thisNode->right);
    thisNode->height = max( hl, hr ) + 1;

    thisNode->balance = hr-hl;/*NEW added*/
    
    hr = height(thisRightSub->right);
    thisRightSub->height = max( hr, thisNode->height) + 1;
    
    thisRightSub->balance = hr-thisNode->height;/*NEW added*/
    
    thisRightSub->parent = thisNode->parent;
    thisNode->parent = thisRightSub;
    
    *ptrToThisNode = thisNode = thisRightSub;
    
    return thisRightSub;
}

TNode * rotateWithLeftChild(TNode ** ptrToThisNode)
{
    TNode * thisNode    = *ptrToThisNode;
    TNode * thisLeftSub = thisNode->left;
    int     hr, hl;
    
    thisNode->left = thisLeftSub->right;
    if(thisLeftSub->right)
        thisLeftSub->right->parent = thisNode;
 
    thisLeftSub->right = thisNode;
    hl = height(thisNode->left);
    hr = height(thisNode->right);
    thisNode->height = max( hl, hr) + 1;
    
    thisNode->balance = hr-hl;/*NEW added*/
    
    hl = height(thisLeftSub->left);
    thisLeftSub->height = max( hl, thisNode->height) + 1;
    
    thisLeftSub->balance = thisNode->height-hl;/*NEW added*/
    
    thisLeftSub->parent = thisNode->parent;    
    thisNode->parent = thisLeftSub;
    
    *ptrToThisNode = thisNode = thisLeftSub;
    
    return thisLeftSub;
}

TNodeShort * rotateWithRightChild_short(TNodeShort ** ptrToThisNode)
{
    TNodeShort * thisNode     = *ptrToThisNode;
    TNodeShort * thisRightSub = thisNode->right;
    int          hr, hl;
    
    thisNode->right = thisRightSub->left;
    if(thisRightSub->left)
        thisRightSub->left->parent = thisNode;
 
    thisRightSub->left = thisNode;
    hl = height_short(thisNode->left);
    hr = height_short(thisNode->right);
    thisNode->height = max( hl, hr ) + 1;
    thisNode->balance = hr-hl;/*NEW added*/
    hr = height_short(thisRightSub->right);
    thisRightSub->height = max( hr, thisNode->height) + 1;
    thisRightSub->balance = hr-thisNode->height;/*NEW added*/
    thisRightSub->parent = thisNode->parent;
    thisNode->parent = thisRightSub;
    
    *ptrToThisNode = thisNode = thisRightSub;
    
    return thisRightSub;
}

TNodeInt * rotateWithRightChild_int(TNodeInt ** ptrToThisNode)
{
    TNodeInt * thisNode     = *ptrToThisNode;
    TNodeInt * thisRightSub = thisNode->right;
    int        hr, hl;
    
    thisNode->right = thisRightSub->left;
    if(thisRightSub->left)
        thisRightSub->left->parent = thisNode;
 
    thisRightSub->left = thisNode;
    hl = height_int(thisNode->left);
    hr = height_int(thisNode->right);
    thisNode->height = max( hl, hr ) + 1;
    
    thisNode->balance = hr-hl;/*NEW added*/
    hr = height_int(thisRightSub->right);
    thisRightSub->height = max( hr, thisNode->height) + 1; 
   
    thisRightSub->balance = hr-thisNode->height;/*NEW added*/
    thisRightSub->parent = thisNode->parent;
    thisNode->parent = thisRightSub;
    
    *ptrToThisNode = thisNode = thisRightSub;
    
    return thisRightSub;
}

TNodeShort * rotateWithLeftChild_short(TNodeShort ** ptrToThisNode)
{
    TNodeShort * thisNode    = *ptrToThisNode;
    TNodeShort * thisLeftSub = thisNode->left;
    int          hr, hl;
    
    thisNode->left = thisLeftSub->right;
    if(thisLeftSub->right)
        thisLeftSub->right->parent = thisNode;
 
    thisLeftSub->right = thisNode;
    hl = height_short(thisNode->left);
    hr = height_short(thisNode->right);
    thisNode->height = max( hl, hr) + 1;
    
    thisNode->balance = hr-hl;/*NEW added*/
    hl = height_short(thisLeftSub->left);
    thisLeftSub->height = max( hl, thisNode->height) + 1;
    
    thisLeftSub->balance = thisNode->height-hl;/*NEW added*/
    thisLeftSub->parent = thisNode->parent;    
    thisNode->parent = thisLeftSub;
    
    *ptrToThisNode = thisNode = thisLeftSub;
    
    return thisLeftSub;
}

TNodeInt * rotateWithLeftChild_int(TNodeInt ** ptrToThisNode)
{
    TNodeInt * thisNode    = *ptrToThisNode;
    TNodeInt * thisLeftSub = thisNode->left;
    int        hr, hl;
    
    thisNode->left = thisLeftSub->right;
    if(thisLeftSub->right)
        thisLeftSub->right->parent = thisNode;
 
    thisLeftSub->right = thisNode;
    hl = height_int(thisNode->left);
    hr = height_int(thisNode->right);
    thisNode->height = max( hl, hr) + 1;
    
    thisNode->balance = hr-hl;/*NEW added*/
    hl = height_int(thisLeftSub->left);
    thisLeftSub->height = max( hl, thisNode->height) + 1;
    
    thisLeftSub->balance = thisNode->height-hl;/*NEW added*/
    thisLeftSub->parent = thisNode->parent;    
    thisNode->parent = thisLeftSub;
    
    *ptrToThisNode = thisNode = thisLeftSub;
    
    return thisLeftSub;
}

void rollbackDB(IndexData * id, AbortRecord * list, int count, BufferManager * buf_man)
{
    RecordWrapper recwrap;
    AbortRecord * rec;
    register int  i;
    
    for(i=count-1; i >= 0; i--)
    {
        rec = list+i;
        recwrap.key = &rec->key;
        recwrap.payload = (const char *)rec->payload;
        recwrap.payloadTree = rec->payloadTree;
        
        if( rec->isDelete )
            id->insertItem(id, &recwrap, 1);
        else
            id->removeRecord(id, &recwrap, NULL);
    }
}

ErrCode insertItem_new(IndexData * id, RecordWrapper * recwrap, int isRollback)
{
    TNode *         boundingNode;
    TNode *         newLeaf = NULL;
    TNode *         root    = id->index;
    BufferManager * buf_man = id->buffer_manager;


    const KeyVal * key     = recwrap->key;
    const char *   payload = recwrap->payload;
    int            found   = 0;
    
    /* if(strlen(key->charkey)<64) counter++; */
    /* if(counter % 100==0) printf("COUNTER=%d\n",counter); */
    
    boundingNode = findBoundingNode( root, key->charkey, &found );

    if( found != -1 )
    {
        /*int found = binarySearch( boundingNode, key->charkey );*/
        if(isRollback)
        {
            MyRecord * foundRec = boundingNode->data+found;

            transferAllPayloads( &foundRec->payloadTree, recwrap );

            return SUCCESS;
        }
        else
        {
            MyRecord * foundRec     = boundingNode->data+found;
            int        entry_exists = 0;

            entry_exists = ( mystrcmp( foundRec->payload, payload ) == 0 );

            if( !entry_exists && foundRec->payloadTree )
                entry_exists = bst_search( foundRec->payloadTree, payload );

            if( entry_exists )
                return ENTRY_EXISTS;

            bst_add( &foundRec->payloadTree, payload );
            
            return SUCCESS;
        }
        
        if( boundingNode->currSize < TNODE_SIZE )
        {
            /* there is room to put the item in this node */
            insertIntoNode( boundingNode, key, payload );
        }
        else
        {
            /* there is no room: min element will be replaced with the item 
               min is boundingNode->data[0] */
            insertToGreatestLowerLeaf( boundingNode, boundingNode->data, &newLeaf, buf_man);
            
            boundingNode->data->key = *key;
            boundingNode->data->payloadTree = NULL;
            PAYLOAD_COPY(boundingNode->data->payload, payload);/*0*/
            tidyUpNode( boundingNode );
        }
    }
    else
    {
        /* TODO: does bounding node always have more than 0 item ?? */
        if( boundingNode->currSize > 0 )
        {
            /* no bounding node found, boundingNode is now the last node on the search path */
            if( boundingNode->currSize < TNODE_SIZE )
            {
                if(VARCHARKEY_CMP( key->charkey, boundingNode->data->key.charkey ) < 0 )
                {
                    /* item is now the new minumum for the node */
                    shiftUpAllItems( boundingNode, 1 );
                    boundingNode->data->key = *key;
                    boundingNode->data->payloadTree = NULL;
                    PAYLOAD_COPY(boundingNode->data->payload, payload);
                    boundingNode->currSize++;
                }
                /* item is now the new maximum for the node */                
                else
                {

                    boundingNode->data[boundingNode->currSize].key = *key;
                    boundingNode->data[boundingNode->currSize].payloadTree = NULL;
                    PAYLOAD_COPY(boundingNode->data[boundingNode->currSize].payload, payload);
                    boundingNode->currSize++;                    
                    }
                /*
                else
                    insertIntoNode( boundingNode, key, payload );
                */
            }

            else
            {
                if(VARCHARKEY_CMP( key->charkey, boundingNode->data->key.charkey ) < 0 )
                {
                    /* create a new leaf on left */
                    TNode * newNode = buf_man->makeTNode(buf_man);
                    
                    newNode->parent = boundingNode;
                    newNode->data->key = *key;
                    PAYLOAD_COPY(newNode->data->payload, payload);
                    newNode->currSize = 1;

                    boundingNode->left = newNode;
                    boundingNode->height = max(1, boundingNode->height);
                    correctHeights(boundingNode->parent);
                    boundingNode->balance = height(boundingNode->right);
                    newLeaf = newNode;
                }
                else
                {
                    /* create a new leaf on right */
                    /* TODO :  HIGH PRIORITY check whether this has to be a new leaf on right */
                    TNode * newNode = buf_man->makeTNode(buf_man);
                    
                    newNode->parent = boundingNode;
                    newNode->data->key = *key;
                    PAYLOAD_COPY(newNode->data->payload, payload);
                    newNode->currSize = 1;

                    boundingNode->height = max(1, boundingNode->height);
                    correctHeights(boundingNode->parent);
                    boundingNode->balance = -height(boundingNode->left);
                    boundingNode->right = newNode;
                    newLeaf = newNode;
                }
            }
        }
        else {
            /* empty tree case */
#if DEBUG            
            if(boundingNode->parent){
                printf("DEBUG: 2) does bounding node always have more than 0 item ?? ==> It is now 0\n");
                printTTree(root,0);
            }
            else
                printf("DEBUG: Insert to empty tree\n");
#endif            
            insertIntoNode(root, key, payload);
        }
    }


    if( newLeaf != NULL )
    {
        /* check for balance */
        TNode * check = newLeaf->parent->parent;
        int     ok    = 0;
		
        if( check )
        {
            check->balance = height(check->right) - height(check->left);

            while( abs( check->balance ) < 2 )
            {
                if( !check->parent )
                {
                    ok = 1;
                    break;
                }
                check = check->parent;
                check->balance = height(check->right) - height(check->left);
            }

            if( !ok )
            {
                TNode * par = check->parent;
                TNode * self;
                
                /* an imbalance situation at node check */
                if( check->balance == 2 )
                {
                    /* Right sub-tree outweighs */
                    if( check->right->balance != -1 )
                    {
                        /* RR single rotation */
                        if( !par )
                            rotateWithRightChild(&root);
                        else 
                            if( par->left == check )
                                self = rotateWithRightChild(&par->left);
                            else
                                self = rotateWithRightChild(&par->right);
                    }
                    else
                    {
                        /* RL double rotation */
                        /* Is it the special case?? */
                        if( isHalfLeaf(check) && isHalfLeaf(check->right) 
                           && isLeaf(check->right->left) && check->right->left->currSize == 1 )
                        {
                            /* move items from check->right to check->right->left */

                            TNode * c     = check->right->left;
                            TNode * b     = check->right;
                            char *  tmp_pay;                            
                            int     b_len = b->currSize-1;
                            register int b_idx;
                            
                            for(b_idx = 0; b_idx < b_len; b_idx++)
                            {
                                tmp_pay = c->data[b_idx+1].payload;
                                c->data[b_idx+1] = b->data[b_idx];
                                b->data[b_idx].payload = tmp_pay;
                            }
                            c->currSize = b_idx+1;
                            tmp_pay = b->data->payload;
                            b->data[0] = b->data[b_len];
                            b->data[b_len].payload = tmp_pay;
                            b->currSize = 1;
                        }
						
                        if( !par )
                        {
                            rotateWithLeftChild( &root->right );
                            rotateWithRightChild(&root);
                        }
                        else 
                            if( par->left == check )
                            {
                                rotateWithLeftChild( &par->left->right );
                                self = rotateWithRightChild( &par->left );
                            }
                            else
                            {
                                rotateWithLeftChild( &par->right->right );
                                self = rotateWithRightChild( &par->right );
                            }
                    }
                }
                else
                {
                    /*left sub-tree outweighs*/
                    if( check->left->balance == 1 )
                    {
                        /* LR double double rotation */
                        /* Is it the special case?? */
                        if( isHalfLeaf(check) && isHalfLeaf(check->left) 
                           && isLeaf(check->left->right) && check->left->right->currSize == 1 )
                        {
                            /* move items from check->left to check->left->right */
                            TNode *  c     = check->left->right;
                            TNode *  b     = check->left;
                            char *   tmp_pay;
                            MyRecord temp  = c->data[0];
                            register int      b_idx = 1;

                            
                            for(;b_idx < b->currSize; b_idx++)
                            {
                                tmp_pay = c->data[b_idx-1].payload;
                                c->data[b_idx-1] = b->data[b_idx];
                                b->data[b_idx].payload = tmp_pay;
                            }
                            tmp_pay = c->data[b_idx-1].payload;
                            c->data[b_idx-1] = temp;
                            b->data->payload = tmp_pay;
                            c->currSize = b_idx;
                            b->currSize = 1;
                        }
						
                        if( !par )
                        {
                            rotateWithRightChild(&root->left);
                            self = rotateWithLeftChild(&root);
                        }
                        else
                            if( par->left == check )
                            {
                                rotateWithRightChild(&par->left->left);
                                self = rotateWithLeftChild(&par->left);
                            }
                            else
                            {
                                rotateWithRightChild(&par->right->left);
                                self = rotateWithLeftChild(&par->right);
                            }
                    }
                    else
                    {
                        /* LL single rotation */
                        if( !par )
                            rotateWithLeftChild(&root);
                        else
                            if( par->left == check )
                                self = rotateWithLeftChild(&par->left);
                            else
                                self = rotateWithLeftChild(&par->right);
                    }
                }

                correctHeights(check->parent);
                
                if( !par )
                {
                    root->parent = NULL;
                    id->index = root;
                }	
                else
                    self->parent = par;
            }                    
        }
    }

    CHECK_VARCHAR(id->index);
    return SUCCESS;
}

ErrCode insertItem_short(IndexData * id, RecordWrapper * recwrap, int isRollback )
{
    TNodeShort *    boundingNode;
    TNodeShort *    newLeaf = NULL;
    TNodeShort *    root    = id->index;
    BufferManager * buf_man = id->buffer_manager;
    const char *    payload = recwrap->payload;
    
    int isFound = 0;
    int32_t key = recwrap->key->shortkey;


    boundingNode = findBoundingNode_short( root, recwrap->key, &isFound );

    if( isFound )
    {

        int found = binarySearch_short( boundingNode, key );
        
        if( found != -1 )
        {
            if(isRollback)
            {
                BSTNode * ptree = boundingNode->payloads[found].payloadTree;
                
                transferAllPayloads( &ptree, recwrap );

                return SUCCESS;
            }
            else
            {
                char *    foundPayload = boundingNode->payloads[found].payload;
                BSTNode * ptree        = boundingNode->payloads[found].payloadTree;
                int       entry_exists = 0;

                entry_exists = ( mystrcmp( foundPayload, payload ) == 0 );

                if( !entry_exists && ptree )
                    entry_exists = bst_search( ptree, payload );

                if( entry_exists )
                    return ENTRY_EXISTS;

                bst_add( &ptree, payload );
                return SUCCESS;
            }
        }
        
        if( boundingNode->currSize < SHORT_TNODE_SIZE )
        {
            /* there is room to put the item in this node */
            insertIntoNode_short( boundingNode, recwrap->key, payload );
        }
        else
        {
            /* there is no room: min element will be replaced with the item 
               min is boundingNode->data[0] */
            insertToGreatestLowerLeaf_short( boundingNode, &newLeaf, buf_man, 0);
            boundingNode->keys[0] = key;
            boundingNode->payloads->payloadTree = NULL;
            PAYLOAD_COPY(boundingNode->payloads->payload, payload);
            tidyUpNode_short( boundingNode );
        }
    }
    else
    {
        /* TODO: does bounding node always have more than 0 item ?? */
        if( boundingNode->currSize > 0 )
        {
            /* no bounding node found, boundingNode is now the last node on the search path */
            if( boundingNode->currSize < SHORT_TNODE_SIZE )
            {
                if( key < boundingNode->keys[0] )
                {
                    /* item is now the new minumum for the node */
                    shiftUpAllItems_short( boundingNode, 1 );
                    boundingNode->keys[0] = key;
                    boundingNode->payloads->payloadTree = NULL;
                    PAYLOAD_COPY(boundingNode->payloads->payload, payload);
                }
                else
                {
                    /* item is now the new maximum for the node */
                    boundingNode->maxKey = key;
                    boundingNode->keys[boundingNode->currSize] = key;
                    boundingNode->payloads[boundingNode->currSize].payloadTree = NULL;
                    PAYLOAD_COPY(boundingNode->payloads[boundingNode->currSize].payload, payload);
                }
                boundingNode->currSize++;
            }
            else
            {
                if( key < boundingNode->keys[0] )
                {
                    /* create a new leaf on left */
                    TNodeShort * newNode = buf_man->makeTNode(buf_man);
                    
                    newNode->parent = boundingNode;
                    newNode->maxKey = key;
                    newNode->keys[0] = key;
                    PAYLOAD_COPY(newNode->payloads->payload, payload);
                    newNode->currSize = 1;

                    boundingNode->left = newNode;
                    boundingNode->height = max(1, boundingNode->height);
                    correctHeights_short(boundingNode->parent);
                    boundingNode->balance = height_short(boundingNode->right);
                    newLeaf = newNode;
                }
                else
                {
                    /* create a new leaf on right */
                    /* TODO :  HIGH PRIORITY check whether this has to be a new leaf on right */
                    TNodeShort * newNode = buf_man->makeTNode(buf_man);
                    
                    newNode->parent = boundingNode;
                    newNode->maxKey = key;
                    newNode->keys[0] = key;
                    PAYLOAD_COPY(newNode->payloads->payload, payload);
                    newNode->currSize = 1;

                    boundingNode->height = max(1, boundingNode->height);
                    correctHeights_short(boundingNode->parent);
                    boundingNode->balance = -height_short(boundingNode->left);
                    boundingNode->right = newNode;
                    newLeaf = newNode;
                }
            }
        }
        else {
            /* empty tree case */
#if DEBUG            
            if(boundingNode->parent){
                printf("DEBUG: 2) does bounding node always have more than 0 item ?? ==> It is now 0\n");
                printTTree(root,0);
            }
            else
                printf("DEBUG: Insert to empty tree\n");
#endif            
            insertIntoNode_short(root, recwrap->key, payload);
        }
    }


    if( newLeaf != NULL )
    {
        /* check for balance */
        TNodeShort * check = newLeaf->parent->parent;
        int ok = 0;
		
        if( check )
        {
            check->balance = height_short(check->right) - height_short(check->left);
            while( abs( check->balance ) < 2 )
            {
                if( !check->parent )
                {
                    ok = 1;
                    break;
                }
                check = check->parent;
                check->balance = height_short(check->right) - height_short(check->left);
            }

            if( !ok )
            {
                TNodeShort * par = check->parent;
                TNodeShort * self;
                
                /* an imbalance situation at node check */
                if( check->balance == 2 )
                {
                    /* Right sub-tree outweighs */
                    if( check->right->balance != -1 )
                    {
                        /* RR single rotation */
                        if( !par )
                            rotateWithRightChild_short(&root);
                        else 
                            if( par->left == check )
                                self = rotateWithRightChild_short(&par->left);
                            else
                                self = rotateWithRightChild_short(&par->right);
                    }
                    else
                    {
                        /* RL double rotation */
                        /* Is it the special case?? */
                        if( isHalfLeaf_short(check) && isHalfLeaf_short(check->right) 
                           && isLeaf_short(check->right->left) && check->right->left->currSize == 1 )
                        {
                            /* move items from check->right to check->right->left */
                            PayloadWrapper tmp_pay;
                            TNodeShort *   c     = check->right->left;
                            TNodeShort *   b     = check->right;
                            int            b_len = b->currSize-1;
                            register int   b_idx;
                            
                            for(b_idx = 0; b_idx < b_len; b_idx++)
                            {
                                tmp_pay = c->payloads[b_idx+1];
                                
                                c->keys[b_idx+1] = b->keys[b_idx];
                                c->payloads[b_idx+1] = b->payloads[b_idx];

                                b->payloads[b_idx] = tmp_pay;
                            }
                            c->currSize = b_idx+1;
                            c->maxKey = c->keys[b_idx];
                            tmp_pay = b->payloads[0];
                            
                            b->keys[0] = b->keys[b_len];
                            b->maxKey = b->keys[0];
                            b->payloads[0] = b->payloads[b_len];

                            b->payloads[b_len] = tmp_pay;
                            b->currSize = 1;
                        }
						
                        if( !par )
                        {
                            rotateWithLeftChild_short( &root->right );
                            rotateWithRightChild_short(&root);
                        }
                        else 
                            if( par->left == check )
                            {
                                rotateWithLeftChild_short( &par->left->right );
                                self = rotateWithRightChild_short( &par->left );
                            }
                            else
                            {
                                rotateWithLeftChild_short( &par->right->right );
                                self = rotateWithRightChild_short( &par->right );
                            }
                    }
                }
                else
                {
                    /*left sub-tree outweighs*/
                    if( check->left->balance == 1 )
                    {
                        /* LR double double rotation */
                        /* Is it the special case?? */
                        if( isHalfLeaf_short(check) && isHalfLeaf_short(check->left) 
                           && isLeaf_short(check->left->right) && check->left->right->currSize == 1 )
                        {
                            /* move items from check->left to check->left->right */
                            TNodeShort *   c         = check->left->right;
                            TNodeShort *   b         = check->left;
                            int32_t        temp_key  = c->keys[0];
                            PayloadWrapper temp_data = c->payloads[0];
                            PayloadWrapper tmp_pay;                            
                            register int   b_idx     = 1;
	
                            
                            for(;b_idx < b->currSize; b_idx++)
                            {
                                tmp_pay = c->payloads[b_idx-1];
                                c->keys[b_idx-1] = b->keys[b_idx];
                                c->payloads[b_idx-1] = b->payloads[b_idx];
                                b->payloads[b_idx] = tmp_pay;
                            }
                            tmp_pay = c->payloads[b_idx-1];
                            c->keys[b_idx-1] = temp_key;
                            c->payloads[b_idx-1] = temp_data;
                            b->payloads[0] = tmp_pay;
                            c->currSize = b_idx;
                            c->maxKey = temp_key;
                            b->currSize = 1;
                            b->maxKey = b->keys[0];
                        }
						
                        if( !par )
                        {
                            rotateWithRightChild_short(&root->left);
                            self = rotateWithLeftChild_short(&root);
                        }
                        else
                            if( par->left == check )
                            {
                                rotateWithRightChild_short(&par->left->left);
                                self = rotateWithLeftChild_short(&par->left);
                            }
                            else
                            {
                                rotateWithRightChild_short(&par->right->left);
                                self = rotateWithLeftChild_short(&par->right);
                            }
                    }
                    else
                    {
                        /* LL single rotation */
                        if( !par )
                            rotateWithLeftChild_short(&root);
                        else
                            if( par->left == check )
                                self = rotateWithLeftChild_short(&par->left);
                            else
                                self = rotateWithLeftChild_short(&par->right);
                    }
                }

                correctHeights_short(check->parent);
                
                if( !par )
                {
                    root->parent = NULL;
                    id->index = root;
                }	
                else
                    self->parent = par;
            }                    
        }
    }

    CHECK_SHORT(id->index);
    return SUCCESS;
}

ErrCode insertItem_int(IndexData * id, RecordWrapper * recwrap, int isRollback )
{
    TNodeInt *      boundingNode;
    TNodeInt *      newLeaf = NULL;
    TNodeInt *      root    = id->index;
    BufferManager * buf_man = id->buffer_manager;
    const char *    payload = recwrap->payload;    

    int isFound = 0;
    int64_t key = recwrap->key->intkey;


    boundingNode = findBoundingNode_int( root, recwrap->key, &isFound );

    if( isFound )
    {

        int found = binarySearch_int( boundingNode, key );
        
        if( found != -1 )
        {
            if(isRollback)
            {
                BSTNode * ptree = boundingNode->payloads[found].payloadTree;
                
                transferAllPayloads( &ptree, recwrap );

                return SUCCESS;
            }
            else
            {
                char *    foundPayload = boundingNode->payloads[found].payload;
                BSTNode * ptree        = boundingNode->payloads[found].payloadTree;
                int       entry_exists = 0;

                entry_exists = ( mystrcmp( foundPayload, payload ) == 0 );

                if( !entry_exists && ptree )
                    entry_exists = bst_search( ptree, payload );

                if( entry_exists )
                    return ENTRY_EXISTS;

                bst_add( &ptree, payload );
                return SUCCESS;
            }
        }
        
        if( boundingNode->currSize < INT_TNODE_SIZE )
        {
            /* there is room to put the item in this node */
            insertIntoNode_int( boundingNode, recwrap->key, payload );
        }
        else
        {
            /* there is no room: min element will be replaced with the item 
               min is boundingNode->data[0] */
            insertToGreatestLowerLeaf_int( boundingNode, &newLeaf, buf_man, 0);
            boundingNode->keys[0] = key;
            boundingNode->payloads[0].payloadTree = NULL;
            PAYLOAD_COPY(boundingNode->payloads[0].payload, payload);
            tidyUpNode_int( boundingNode );
        }
    }
    else
    {
        /* TODO: does bounding node always have more than 0 item ?? */
        if( boundingNode->currSize > 0 )
        {
            /* no bounding node found, boundingNode is now the last node on the search path */
            if( boundingNode->currSize < INT_TNODE_SIZE )
            {
                if( key < boundingNode->keys[0] )
                {
                    /* item is now the new minumum for the node */
                    shiftUpAllItems_int( boundingNode, 1 );
                    boundingNode->keys[0] = key;
                    boundingNode->payloads[0].payloadTree = NULL;
                    PAYLOAD_COPY(boundingNode->payloads[0].payload, payload);
                }
                else
                {
                    /* item is now the new maximum for the node */
                    boundingNode->maxKey = key;
                    boundingNode->keys[boundingNode->currSize] = key;
                    boundingNode->payloads[boundingNode->currSize].payloadTree = NULL;
                    PAYLOAD_COPY(boundingNode->payloads[boundingNode->currSize].payload, payload);
                }
                boundingNode->currSize++;
            }
            else
            {
                if( key < boundingNode->keys[0] )
                {
                    /* create a new leaf on left */
                    TNodeInt * newNode = buf_man->makeTNode(buf_man);
                    
                    newNode->parent = boundingNode;
                    newNode->maxKey = key;
                    newNode->keys[0] = key;
                    PAYLOAD_COPY(newNode->payloads[0].payload, payload);
                    newNode->currSize = 1;

                    boundingNode->left = newNode;
                    boundingNode->height = max(1, boundingNode->height);
                    
                    correctHeights_int(boundingNode->parent);
                    boundingNode->balance = height_int(boundingNode->right);
                    newLeaf = newNode;
                }
                else
                {
                    /* create a new leaf on right */
                    /* TODO :  HIGH PRIORITY check whether this has to be a new leaf on right */
                    TNodeInt * newNode = buf_man->makeTNode(buf_man);
                    
                    newNode->parent = boundingNode;
                    newNode->maxKey = key;
                    newNode->keys[0] = key;
                    PAYLOAD_COPY(newNode->payloads[0].payload, payload);
                    newNode->currSize = 1;

                    boundingNode->height = max(1, boundingNode->height);
                    
                    correctHeights_int(boundingNode->parent);
                    boundingNode->balance = -height_int(boundingNode->left);
                    boundingNode->right = newNode;
                    newLeaf = newNode;
                }
            }
        }
        else {
            /* empty tree case */
#if DEBUG            
            if(boundingNode->parent){
                printf("DEBUG: 2) does bounding node always have more than 0 item ?? ==> It is now 0\n");
                printTTree(root,0);
            }
            else
                printf("DEBUG: Insert to empty tree\n");
#endif            
            insertIntoNode_int(root, recwrap->key, payload);
        }
    }


    if( newLeaf != NULL )
    {
        /* check for balance */
        TNodeInt * check = newLeaf->parent->parent;
        int ok = 0;
		
        if( check )
        {
            check->balance = height_int(check->right) - height_int(check->left);
            while( abs( check->balance ) < 2 )
            {
                if( !check->parent )
                {
                    ok = 1;
                    break;
                }
                check = check->parent;
                check->balance = height_int(check->right) - height_int(check->left);
            }

            if( !ok )
            {
                TNodeInt * par = check->parent;
                TNodeInt * self;
                
                /* an imbalance situation at node check */
                if( check->balance == 2 )
                {
                    /* Right sub-tree outweighs */
                    if( check->right->balance != -1 )
                    {
                        /* RR single rotation */
                        if( !par )
                            rotateWithRightChild_int(&root);
                        else 
                            if( par->left == check )
                                self = rotateWithRightChild_int(&par->left);
                            else
                                self = rotateWithRightChild_int(&par->right);
                    }
                    else
                    {
                        /* RL double rotation */
                        /* Is it the special case?? */
                        if( isHalfLeaf_int(check) && isHalfLeaf_int(check->right) 
                           && isLeaf_int(check->right->left) && check->right->left->currSize == 1 )
                        {
                            /* move items from check->right to check->right->left */
                            PayloadWrapper tmp_pay;
                            TNodeInt *     c     = check->right->left;
                            TNodeInt *     b     = check->right;
                            int            b_len = b->currSize-1;
                            register int   b_idx;
                            
                            for(b_idx = 0; b_idx < b_len; b_idx++)
                            {
                                tmp_pay = c->payloads[b_idx+1];
                                
                                c->keys[b_idx+1] = b->keys[b_idx];
                                c->payloads[b_idx+1] = b->payloads[b_idx];

                                b->payloads[b_idx] = tmp_pay;
                            }
                            c->currSize = b_idx+1;
                            c->maxKey = c->keys[b_idx];
                            tmp_pay = b->payloads[0];
                            
                            b->keys[0] = b->keys[b_len];
                            b->maxKey = b->keys[0];
                            b->payloads[0] = b->payloads[b_len];

                            b->payloads[b_len] = tmp_pay;
                            b->currSize = 1;
                        }
						
                        if( !par )
                        {
                            rotateWithLeftChild_int( &root->right );
                            rotateWithRightChild_int(&root);
                        }
                        else 
                            if( par->left == check )
                            {
                                rotateWithLeftChild_int( &par->left->right );
                                self = rotateWithRightChild_int( &par->left );
                            }
                            else
                            {
                                rotateWithLeftChild_int( &par->right->right );
                                self = rotateWithRightChild_int( &par->right );
                            }
                    }
                }
                else
                {
                    /*left sub-tree outweighs*/
                    if( check->left->balance == 1 )
                    {
                        /* LR double double rotation */
                        /* Is it the special case?? */
                        if( isHalfLeaf_int(check) && isHalfLeaf_int(check->left) 
                           && isLeaf_int(check->left->right) && check->left->right->currSize == 1 )
                        {
                            /* move items from check->left to check->left->right */
                            TNodeInt *     c         = check->left->right;
                            TNodeInt *     b         = check->left;
                            int64_t        temp_key  = c->keys[0];
                            PayloadWrapper temp_data = c->payloads[0];
							PayloadWrapper tmp_pay;                            
                            register int   b_idx     = 1;

                            
                            for(;b_idx < b->currSize; b_idx++)
                            {
                                tmp_pay = c->payloads[b_idx-1];
                                c->keys[b_idx-1] = b->keys[b_idx];
                                c->payloads[b_idx-1] = b->payloads[b_idx];
                                b->payloads[b_idx] = tmp_pay;
                            }
                            tmp_pay = c->payloads[b_idx-1];
                            c->keys[b_idx-1] = temp_key;
                            c->payloads[b_idx-1] = temp_data;
                            b->payloads[0] = tmp_pay;
                            c->currSize = b_idx;
                            c->maxKey = temp_key;
                            b->currSize = 1;
                            b->maxKey = b->keys[0];
                        }
						
                        if( !par )
                        {
                            rotateWithRightChild_int(&root->left);
                            self = rotateWithLeftChild_int(&root);
                        }
                        else
                            if( par->left == check )
                            {
                                rotateWithRightChild_int(&par->left->left);
                                self = rotateWithLeftChild_int(&par->left);
                            }
                            else
                            {
                                rotateWithRightChild_int(&par->right->left);
                                self = rotateWithLeftChild_int(&par->right);
                            }
                    }
                    else
                    {
                        /* LL single rotation */
                        if( !par )
                            rotateWithLeftChild_int(&root);
                        else
                            if( par->left == check )
                                self = rotateWithLeftChild_int(&par->left);
                            else
                                self = rotateWithLeftChild_int(&par->right);
                    }
                }

                correctHeights_int(check->parent);
                
                if( !par )
                {
                    root->parent = NULL;
                    id->index = root;
                }	
                else
                    self->parent = par;
            }                    
        }
    }

    CHECK_INT(id->index);
    return SUCCESS;
}


int _checkTTree(TNode * root, TNode * parent, int * height)
{
    int leftHeight, rightHeight;
    int correct = 1;

    
    if(root->left==NULL)
    {
        leftHeight = -1;
    }
    else
    {
        correct = correct && _checkTTree(root->left, root, &leftHeight);
    }

    if(root->right==NULL)
    {
        rightHeight = -1;
    }
    else
    {
        correct = correct && _checkTTree(root->right, root, &rightHeight);
    }

    correct = correct && (root->parent == parent) && (abs(rightHeight-leftHeight)<2)
              && (root->currSize<TNODE_SIZE+1);
    /*        && ((rightHeight!=-1&&leftHeight!=-1)?(root->currSize>MIN_TNODE_SIZE-1):1);*/
    

    *height = 1+max(leftHeight,rightHeight);
    correct = correct && (*height==root->height)
        && ((rightHeight-leftHeight)==root->balance);
    
    return correct;
}


void checkTTree(TNode * root)
{
    int height;
    
    if( !_checkTTree(root, NULL, &height))
        printf("********** ERROR: T-Tree(VARCHAR) has problems!!! **********\n");
    else
        printf("***** T-Tree(Varchar) is Correct *****\n");
    
}

int _checkTTree_short(TNodeShort * root, TNodeShort * parent, int * height)
{
    int leftHeight, rightHeight;
    int correct = 1;

    
    if(root->left==NULL)
    {
        leftHeight = -1;
    }
    else
    {
        correct = correct && _checkTTree_short(root->left, root, &leftHeight);
    }

    if(root->right==NULL)
    {
        rightHeight = -1;
    }
    else
    {
        correct = correct && _checkTTree_short(root->right, root, &rightHeight);
    }

    correct = correct && (root->parent == parent) && (abs(rightHeight-leftHeight)<2)
              && (root->currSize<SHORT_TNODE_SIZE+1);
    /*        && ((rightHeight!=-1&&leftHeight!=-1)?(root->currSize>SHORT_MIN_TNODE_SIZE-1):1);*/
    

    *height = 1+max(leftHeight,rightHeight);
    correct = correct && (*height==root->height)
        && ((rightHeight-leftHeight)==root->balance);
    
    return correct;
}


void checkTTree_short(TNodeShort * root)
{
    int height;
    
    if( !_checkTTree_short(root, NULL, &height))
        printf("********** ERROR: T-Tree(SHORT) has problems!!! **********\n");
    else
        printf("***** T-Tree(Short) is Correct *****\n");
}


int _checkTTree_int(TNodeInt * root, TNodeInt * parent, int * height)
{
    int leftHeight, rightHeight;
    int correct = 1;

    
    if(root->left==NULL)
    {
        leftHeight = -1;
    }
    else
    {
        correct = correct && _checkTTree_int(root->left, root, &leftHeight);
    }

    if(root->right==NULL)
    {
        rightHeight = -1;
    }
    else
    {
        correct = correct && _checkTTree_int(root->right, root, &rightHeight);
    }

    correct = correct && (root->parent == parent) && (abs(rightHeight-leftHeight)<2)
              && (root->currSize<INT_TNODE_SIZE+1);
    /*       && ((rightHeight!=-1&&leftHeight!=-1)?(root->currSize>INT_MIN_TNODE_SIZE-1):1);*/
    

    *height = 1+max(leftHeight,rightHeight);
    correct = correct && (*height==root->height)
        && ((rightHeight-leftHeight)==root->balance);
    
    return correct;
}

void checkTTree_int(TNodeInt * root)
{
    int height;
    
    if( !_checkTTree_int(root, NULL, &height))
        printf("********** ERROR: T-Tree(INT) has problems!!! **********\n");
    else
        printf("***** T-Tree(Int) is Correct *****\n");
    
}

void printTabs(int level)
{
	int i=0;
	while(i<level){
		printf("    ");
		i++;
	}
}

void printTTree(TNode * root, int level)
{
	if( root )
	{
		int i=0;
        if( root->currSize )
        {
            printf("+ [");
            for(;i<root->currSize-1;i++)
            {
                    printf( "%s; ", root->data[i].key.charkey );                    
            }

            printf( "%s]", root->data[i].key.charkey );                    
            
            printf( "(h=%d, b=%d) => \n", root->height, root->balance );
		}
        else
            printf( "[](h=0, b=0) => \n");
            
		printTabs(level+1);
		printTTree(root->left, level+1);
		printTabs(level+1);
		printTTree(root->right, level+1);
	}
	else{
		printf("- []\n");
	}
}

void printTTree_short(TNodeShort * root, int level)
{
	if( root )
	{
		int i=0;
        if( root->currSize )
        {
            printf("+ [");
            for(;i<root->currSize-1;i++)
            {
                printf( "%ld; ", (long int)root->keys[i] );
            }

            printf( "%ld]", (long int)root->keys[i] );
            
            printf( "(h=%d, b=%d) => \n", root->height, root->balance );
		}
        else
            printf( "[](h=0, b=0) => \n");
            
		printTabs(level+1);
		printTTree_short(root->left, level+1);
		printTabs(level+1);
		printTTree_short(root->right, level+1);
	}
	else{
		printf("- []\n");
	}
}

void freeTNode(TNode * node)
{
    register int i;

    for(i=TNODE_SIZE-1; i >= 0 ; i-- )
        if( node->data[i].payloadTree )
            bst_free( node->data[i].payloadTree );
}

void freeTNode_short(TNodeShort * node)
{
    register int i;

    for(i=SHORT_TNODE_SIZE-1; i >= 0 ; i-- )
        if( node->payloads[i].payloadTree )
            bst_free( node->payloads[i].payloadTree );
}

void freeTNode_int(TNodeInt * node)
{
    register int i;

    for(i=INT_TNODE_SIZE-1; i >= 0 ; i-- )
        if( node->payloads[i].payloadTree )
            bst_free( node->payloads[i].payloadTree );
}

void freeTree(TNode * root)
{	
	if( root )
	{
		if( root->left )
			freeTree( root->left );
		
		if( root->right )
			freeTree( root->right );
			
		freeTNode(root);
	}
}

void freeTree_short(TNodeShort * root)
{	
	if( root )
	{
		if( root->left )
			freeTree_short( root->left );
		
		if( root->right )
			freeTree_short( root->right );
			
		freeTNode_short(root);
	}
}

void freeTree_int(TNodeInt * root)
{	
	if( root )
	{
		if( root->left )
			freeTree_int( root->left );
		
		if( root->right )
			freeTree_int( root->right );
			
		freeTNode_int(root);
	}
}

/*************** Debugging related ******************/

void printSearch(KeyVal * key, TNode * node)
{
    int i;
    printf("==========DEBUG==========\n");
    printf("SEARCH KEY : %s\n",key->charkey);

    for(i=0; i < node->currSize; i++)
            printf("KEY %d => %s\n",i+1,node->data[i].key.charkey);
}

/*
static inline int mystrcmp(const char *x, const char *y)
{
    return (*x != *y ? (int)(*x-*y) : strcmp(x, y));
}
*/
/***************Debugging related end ***************/
