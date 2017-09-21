/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#ifndef __CONSTANTS__
#define __CONSTANTS__

/*alignment of memory allocations*/
#define ALIGN_SIZE 64

#define TNODE_SIZE 1500
#define MIN_TNODE_SIZE 1490

#define SHORT_TNODE_SIZE 38
#define SHORT_MIN_TNODE_SIZE 34

#define INT_TNODE_SIZE 18
#define INT_MIN_TNODE_SIZE 16

/*How many number of TNode's in each buffer*/
#define NUM_TNODE_PER_BUF 2048

/*How many number of buffers do we have initially*/
#define INIT_NUM_BUFS 16

/*We allocate 2x of TNode's not to clash re-allocations to same time*/
#define INIT_NUM_PAYLOAD_BUFS INIT_NUM_BUFS

/*number of LCB buffers initially*/
#define INIT_NUM_LCB_BUFS 4

/*how many lcb's per buffer for pre-allocation*/
#define NUM_LCB_PER_BUF 100

/*Number of bytes for each payload*/
#define PAYLOAD_ALLOC_SIZE 128

/*One payload buffer size for pre-allocation - INT keys*/
#define PAYLOAD_INTBUF_SIZE NUM_TNODE_PER_BUF*((INT_TNODE_SIZE+1)*PAYLOAD_ALLOC_SIZE+INT_PAYLOAD_OFFSET)*2

/*One payload buffer size for pre-allocation - SHORT keys*/
#define PAYLOAD_SHORTBUF_SIZE NUM_TNODE_PER_BUF*((SHORT_TNODE_SIZE+1)*PAYLOAD_ALLOC_SIZE+SHORT_PAYLOAD_OFFSET)*2

/*One payload buffer size for pre-allocation -  VARCHAR keys */
#define PAYLOAD_BUF_SIZE NUM_TNODE_PER_BUF*TNODE_SIZE*PAYLOAD_ALLOC_SIZE*2

/*When changing the node size for TNodeShort and TNodeInt also change
  these numbers.
  1. Values for TNodeShort Sizes:
      6  --> 0
      22 --> 256
      38 --> 512
      54 --> 768
      118 --> 1792
  2. Values for TNodeInt Sizes:
      2  --> 0
      10 --> 64
      18 --> 192
      26 --> 320
      34 --> 448
      66 --> 17*64
      98 --> 23*64
*/
#define SHORT_PAYLOAD_OFFSET 512
#define INT_PAYLOAD_OFFSET 192

/*specifies how many XactPendingLists we have in one AbortPool*/
#define NUM_PENDING_LISTS 4

/*how many items we have in each pending list initially*/
#define INITIAL_PENDING_COUNT 128

/*specifies how many XactLCBs we have in one XactLCBPool*/
#define NUM_PENDING_LISTS_XACTLCB 1024

#endif
