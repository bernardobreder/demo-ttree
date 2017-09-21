/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#ifndef __MYSTACK__
#define __MYSTACK__


/**
 * Simple stack implementation for holding data that may fit in void *
 *
 * NOTE: for initializing:
 *    1. Stack * stack;
 *    2. stack_init(&stack);
 *    3. then use it.
 */
typedef struct StackItem
{
    struct StackItem * next;
    void *             data;
} StackItem;

typedef struct Stack
{
    StackItem * top_ptr;
    int         size;
} Stack;

void * stack_pop(Stack * stack);
inline void stack_push(Stack * stack, void * data);
inline void stack_init(Stack ** stack);
void        stack_free(Stack * stack);
int         stack_size(const Stack * stack);



#endif
