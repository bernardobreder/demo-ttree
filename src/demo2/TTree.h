/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#ifndef _TTREE_H_
#define _TTREE_H_

#include "DBTypes.h"

/*typedef int (*Comparator)(const KeyVal *, const KeyVal *);*/

/*** DECLERATIONS ***/
void rollbackDB(IndexData * id, AbortRecord * list, int count, struct BufferManager * buf_man);
/*implemented in DBimpl.c*/
void addToPendingList(IndexData * id, XACTState * xactState, RecordWrapper * recwrap, int del);
void testCorrectness();


/* Debugging and Utility */
/*
void checkTTree(TNode * root);
void checkTTree_int(TNodeInt * root);
void checkTTree_short(TNodeShort * root);
int  _checkTTree(TNode * root, TNode * parent, int * height);
void checkTTree_short(TNodeShort * root);
int  _checkTTree_short(TNodeShort * root, TNodeShort * parent, int * height);
void checkTTree_int(TNodeInt * root);
int  _checkTTree_int(TNodeInt * root, TNodeInt * parent, int * height);

void printTTree(TNode * root, int level);
*/

/*VARCHAR TNODE*/
ErrCode        insertItem_new(IndexData * id, RecordWrapper * recwrap, int isRollback );
void           insertToGreatestLowerLeaf(TNode * boundingNode, MyRecord * min, TNode ** newLeaf, struct BufferManager * buf_man);
TNode *        findBoundingNode(TNode * root, const char * key, int * isFound);
int            binarySearch( const TNode * data, const char * key);
int            insertIntoNode(TNode * node, const KeyVal * key, const char * payload);
void           tidyUpNode( TNode * node );
void           shiftUpAllItems( TNode * node, int by );
void           shiftDownAllItems( TNode * node, int downto );
/*
inline int     isLeaf(const TNode * node);
inline int     isHalfLeaf(const TNode * node);
inline int     height( const TNode * tree );
*/
TNode *        rotateWithRightChild(TNode ** ptrToThisNode);
TNode *        rotateWithLeftChild(TNode ** ptrToThisNode);
void           correctHeights(TNode * node);
inline TNode * findGreatestLowerLeaf(const TNode * node);
inline TNode * findLeastUpperLeaf(const TNode * node);
ErrCode        findRecordAndTNode(IDXState * idxState, const KeyVal * key, Record * result);
ErrCode        getFirstRecordAndTNode(IDXState * idxState, Record * out);
ErrCode        getNextRecordAndTNode(struct IDXState * idx_state, Record * out);
TNode *        getNextTNode(TNode * curr);
TNode *        getPrevTNode(TNode * curr);
int            scanTNode(TNode * node, KeyVal * lastKey);
ErrCode        deleteOneOrAllRecords(IndexData * id, RecordWrapper * record, XACTState * xactState);
inline int     hasRecords(const void * node);
void           freeTree(TNode * root);

/*UNUSED*/
MyRecord * _findRecord(TNode * root, const KeyVal * key);
ErrCode    _findRecordWithPayload(TNode * root, MyRecord * rec, TNode ** itsNode, int *recIdx);
ErrCode    findRecordWithPayload(TNode * root, MyRecord * rec, TNode ** itsNode, int *recIdx, TNode ** boundingNode, int * isBNFound);
TNode *    deleteItem(TNode ** rootPtr, const KeyVal * item);

/*INT TNODE*/
ErrCode           insertItem_int(IndexData * id, RecordWrapper * recwrap, int isRollback );
void              insertToGreatestLowerLeaf_int(TNodeInt * boundingNode, TNodeInt ** newLeaf, struct BufferManager * buf_man, int index);
TNodeInt *        findBoundingNode_int(TNodeInt * _root, const KeyVal * key, int * isFound);
int               binarySearch_int( const TNodeInt * node, int64_t key);
int               insertIntoNode_int(TNodeInt * node, const KeyVal * key, const char * payload);
void              tidyUpNode_int( TNodeInt * node );
void              shiftUpAllItems_int( TNodeInt * node, int by );
void              shiftDownAllItems_int( TNodeInt * node, int downto);
/*
inline int        height_int( const TNodeInt * tree );
inline int        isLeaf_int(const TNodeInt * node);
inline int        isHalfLeaf_int(const TNodeInt * node);
*/
TNodeInt *        rotateWithRightChild_int(TNodeInt ** ptrToThisNode);
TNodeInt *        rotateWithLeftChild_int(TNodeInt ** ptrToThisNode);
void              correctHeights_int(TNodeInt * node);
inline TNodeInt * findGreatestLowerLeaf_int(const TNodeInt * node);
inline TNodeInt * findLeastUpperLeaf_int(const TNodeInt * node);
ErrCode           findRecordAndTNode_int(IDXState * idxState, const KeyVal * key, Record * result);
ErrCode           getFirstRecordAndTNode_int(IDXState * idxState, Record * out);
ErrCode           getNextRecordAndTNode_int(IDXState * idx_state, Record * out);
TNodeInt *        getNextTNode_int(TNodeInt * curr);
TNodeInt *        getPrevTNode_int(TNodeInt * curr);
int               scanTNode_int(TNodeInt * node, KeyVal * lastKey);
ErrCode           deleteOneOrAllRecords_int(IndexData * id, RecordWrapper * record, XACTState * xactState);
inline int        hasRecords_int(const void * node);
void              freeTree_int(TNodeInt * root);

/*SHORT TNODE*/
ErrCode             insertItem_short(IndexData * id, RecordWrapper * recwrap, int isRollback );
void                insertToGreatestLowerLeaf_short(TNodeShort * boundingNode, TNodeShort ** newLeaf, struct BufferManager * buf_man, int index);
TNodeShort *        findBoundingNode_short(TNodeShort * _root, const KeyVal * key, int * isFound);
int                 binarySearch_short( const TNodeShort * node, int32_t key);
int                 insertIntoNode_short(TNodeShort * node, const KeyVal * key, const char * payload);
void                tidyUpNode_short( TNodeShort * node );
void                shiftUpAllItems_short( TNodeShort * node, int by );
void                shiftDownAllItems_short( TNodeShort * node, int downto);
/*
inline short        height_short( const TNodeShort * tree );
inline int          isLeaf_short(const TNodeShort * node);
inline int          isHalfLeaf_short(const TNodeShort * node);
*/
TNodeShort *        rotateWithRightChild_short(TNodeShort ** ptrToThisNode);
TNodeShort *        rotateWithLeftChild_short(TNodeShort ** ptrToThisNode);
void                correctHeights_short(TNodeShort * node);
inline TNodeShort * findGreatestLowerLeaf_short(const TNodeShort * node);
inline TNodeShort * findLeastUpperLeaf_short(const TNodeShort * node);
ErrCode             findRecordAndTNode_short(IDXState * idxState, const KeyVal * key, Record * result);
ErrCode             getFirstRecordAndTNode_short(IDXState * idxState, Record * out);
ErrCode             getNextRecordAndTNode_short(IDXState * idx_state, Record * out);
TNodeShort *        getNextTNode_short(TNodeShort * curr);
TNodeShort *        getPrevTNode_short(TNodeShort * curr);
int                 scanTNode_short(TNodeShort * node, KeyVal * lastKey);
ErrCode             deleteOneOrAllRecords_short(IndexData * id, RecordWrapper * record, XACTState * xactState);
inline int          hasRecords_short(const void * node);
void                freeTree_short(TNodeShort * root);



/********** FUNCTION POINTERS **********/
ErrCode (*findRecord[3])(IDXState *, const KeyVal *, Record *);
ErrCode (*insertItem[3])(IndexData *, RecordWrapper *, int);
ErrCode (*getFirst[3])(IDXState *, Record *);
ErrCode (*getNextRecord[3])(IDXState *, Record *);
ErrCode (*removeRecord[3])(IndexData *, RecordWrapper *, XACTState *);
int (*hasRecordsFunc[3])(const void *);
int (*_keycmp[3])(const KeyVal *key1, const KeyVal *key2);

/********** END of FUNCTION POINTERS **********/

#endif
