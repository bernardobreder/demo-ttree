/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#ifndef __BINARY_SEARCH_TREE__
#define __BINARY_SEARCH_TREE__

/*#include "server.h" for MAX_PAYLOAD_LEN */
#define MAX_PAYLOAD_LEN 100

/**
 * Simple Binary Search Tree implementation.
 *
 * 1. begin with an empty BSTNode * tree = NULL
 * 2. then use bst_add(tree, "string"); to add items
 * 3. bst_search() for searching only returns whether items exists or not
 * 4. bst_delete() removes an item
 * 5. bst_free() to free the memory allocated for the tree
 *
 * NOTE: set MAX_PAYLOAD_LEN accordingly for the max string that can occur
 */
typedef struct BSTNode
{
    struct BSTNode * right;
    struct BSTNode * left;
    char data[MAX_PAYLOAD_LEN+1];
} BSTNode;
    
int  bst_search (BSTNode * tree, const char * data);
int  bst_delete (BSTNode ** tree, const char * data);
void bst_add    (BSTNode ** tree, const char * data);
void bst_free   (BSTNode * tree);
void bst_print  (BSTNode * tree);
const char * bst_first(BSTNode * tree);

#endif
