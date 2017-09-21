/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#include <stdlib.h>

#include "Stack.h"

void stack_init(Stack ** stack)
{
    Stack * new_stack;
    new_stack = (Stack*)malloc(sizeof(Stack));
    new_stack->size = 0;
    new_stack->top_ptr = NULL;
    *stack = new_stack;
}

void * stack_pop(Stack * stack)
{
    if(!stack || stack->size == 0 )
        return NULL;
    else
    {
        void * data;
        StackItem * top = stack->top_ptr;
        
        data = top->data;
        stack->top_ptr = top->next;
        --stack->size;
        free(top);
        return data;
    }
}

inline void   stack_push(Stack * stack, void * data)
{
    StackItem * new_item;

    new_item = (StackItem*)malloc(sizeof(StackItem));
    new_item->data = data;
    new_item->next = stack->top_ptr;
    stack->top_ptr = new_item;
    ++stack->size;
}

inline int    stack_size(const Stack * stack)
{
    return stack->size;
}

void stack_free(Stack * stack)
{
    StackItem * itms = stack->top_ptr, *tmp;

    while(itms)
    {
        tmp = itms->next;
        free(itms);
        itms = tmp;
    }

    free(stack);
}
