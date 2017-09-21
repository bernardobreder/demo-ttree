//
//  main.c
//  ttree
//
//  Created by Bernardo Breder on 10/02/16.
//  Copyright © 2016 breder. All rights reserved.
//

#include <stdlib.h>
#include <stdio.h>
#include "ttree.h"
#include "utest.h"
#include "test_utils.h"

struct item {
    int key;
};

static int __cmpfunc(void *key1, void *key2)
{
    return (*(int *)key1 - *(int *)key2);
}

static struct item *alloc_item(int val)
{
    struct item *item;

    item = (struct item*) malloc(sizeof(struct item));
    if (!item) {
        utest_error("Failed to allocate %zd bytes!", sizeof(*item));
    }

    item->key = val;
    return item;
}

#define CHECK_ITEM(item, exp)                                           \
    do {                                                                \
        if (!item) {                                                    \
            UTEST_FAILED("Failed to lookup item by key %d. But given key" \
                         " was inserted into the tree!", exp);          \
        }                                                               \
        if ((item)->key != exp) {                                       \
            UTEST_FAILED("ttree_lookup returned unexpected item with key " \
                         "%d. But key I wanted to find was %d!",        \
                         (item)->key, exp);                             \
        }                                                               \
    } while (0)

int main(int argc, const char * argv[]) {
    Ttree tree;
    TtreeNode *tnode;
    int ret, i;
    int num_keys = 4;
    int num_items = 16;
    struct balance_info binfo;
    struct item *item;
    
//    num_keys = utest_get_arg(args, 0, INT);
//    num_items = utest_get_arg(args, 1, INT);
//    UTEST_ASSERT(num_items >= 1);
    
    ret = ttree_init(&tree, num_keys, true, __cmpfunc, struct item, key);
    UTEST_ASSERT(ret >= 0);
    for (i = 0; i < (num_items / 2); i++) {
        item = alloc_item(i);
        UTEST_ASSERT(ttree_insert(&tree, item) == 0);
        item = alloc_item(num_items - i - 1);
        UTEST_ASSERT(ttree_insert(&tree, item) == 0);
    }
    
    check_tree_balance(&tree, &binfo);
    if (binfo.balance != TREE_BALANCED) {
        UTEST_FAILED("Tree is unbalanced on a node %p BFC = %d, %s\n",
                     binfo.tnode, binfo.tnode->bfc,
                     balance_name(binfo.balance));
    }
    
    /*
     * Just an example of how to browse the tree keys
     * in a sorted order: from the smallest one to the greatest one.
     * The following cycle runs from the smallest key to the greatest
     * one and checks that an item by given key can be successfully found.
     */
    tnode = ttree_node_leftmost(tree.root);
    while (tnode) {
        tnode_for_each_index(tnode, i) {
            ret = *(int *)tnode_key(tnode, i);
            item = (struct item *)ttree_lookup(&tree, &ret, NULL);
            CHECK_ITEM(item, ret);
        }
        
        tnode = tnode->successor;
    }
    
    /*
     * Here we run from the largest item to the smallest one
     * checking that ttree_lookup finds each item we ask.
     */
    for (i = num_keys - 1; i >= 0; i--) {
        item = (struct item *)ttree_lookup(&tree, &i, NULL);
        CHECK_ITEM(item, i);
    }
    return 0;
}
