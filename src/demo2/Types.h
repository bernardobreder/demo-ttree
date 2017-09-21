/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#ifndef __MYTYPES__
#define __MYTYPES__

#include "server.h"
#include "Constants.h"

typedef struct TNode          TNode;
typedef struct MyRecord       MyRecord;
typedef union  KeyVal         KeyVal;
typedef struct AbortRecord    AbortRecord;
typedef struct RecordWrapper  RecordWrapper;
typedef struct PayloadWrapper PayloadWrapper;
typedef struct MyPayload      MyPayload;
typedef char                  KeyVarchar[MAX_VARCHAR_LEN+1];

typedef struct TNodeShort TNodeShort;
typedef struct TNodeInt     TNodeInt;
typedef struct TNodeVarchar TNodeVarchar;

struct TNodeShort
{
    struct PayloadWrapper * payloads;
    struct TNodeShort *     right;
    struct TNodeShort *     left;
    struct TNodeShort *     parent;
    short                   height;
    unsigned char           currSize;
    char                    balance;
    int32_t                 maxKey;
    int32_t                 keys[SHORT_TNODE_SIZE];
};

struct TNodeInt
{
    struct PayloadWrapper * payloads;
    struct TNodeInt *       right;
    struct TNodeInt *       left;
    struct TNodeInt *       parent;
    int                     height;
    short                   currSize;
    short                   balance;
    int64_t                 maxKey;
    int64_t                 keys[INT_TNODE_SIZE];    
};

struct TNodeVarchar
{
    struct PayloadWrapper * payloads;
    struct TNode * parent;
    struct TNode * left;
    struct TNode * right;
    int            height;
    short          currSize;
    short          balance;

    char keys[(MAX_VARCHAR_LEN+1)*TNODE_SIZE];
};

union KeyVal
{
    int32_t shortkey;
    int64_t intkey;
    char    charkey[MAX_VARCHAR_LEN + 1];
};

struct MyRecord
{
    union KeyVal     key;
    char *           payload; /*[MAX_PAYLOAD_LEN+1];*/
    struct BSTNode * payloadTree;
};
    
struct TNode
{
    /* contol data */
    short int currSize;
    short int balance;    
    int       height;
    
    struct TNode * left;
    struct TNode * right;
    struct TNode * parent;
    
    MyRecord data[TNODE_SIZE];
};

struct RecordWrapper
{
    const KeyVal *   key;
    const char *     payload;
    struct BSTNode * payloadTree;
};

struct MyPayload
{
    char *           payload;
    struct BSTNode * payloadTree;
    char             payload_space[112];
};

struct PayloadWrapper
{
    char * payload;
    struct BSTNode * payloadTree;
};

struct AbortRecord
{
    union KeyVal     key;
    char             payload[MAX_PAYLOAD_LEN+1];
    struct BSTNode * payloadTree;
    int              isDelete;
};

#endif
