/*******************************************************************************
 *                                                                             *
 *    Author:    Cagri Balkesen                                                *
 *    E-mail:    bcagri@student.ethz.ch                                        *
 *    Web:       http://www.cagribalkesen.name.tr                              *
 *    Institute: Swiss Federal Institute of Technology, Zurich (ETH Zurich)    *
 *                                                                             *
 *******************************************************************************/

#include "BinarySearchTree.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>


const char * bst_first(BSTNode * tree)
{
    BSTNode * temp = tree;

    while( temp->left )
        temp = temp->left;

    return temp->data;
}

int bst_search(BSTNode * tree, const char * data)
{
    BSTNode * temp = tree;
    int cmp;

    while( temp )
    {
        cmp = strcmp( data, temp->data );

        if(cmp == 0)
            return 1;

        if( cmp < 0 )
            temp = temp->left;
        else
            temp = temp->right;
    }

    return 0;
}

void bst_add(BSTNode ** tree, const char * data)
{
    BSTNode * tr = *tree;
    BSTNode ** p_tr;
    
    if( tr == NULL )
    {
        tr = (BSTNode*)malloc(sizeof(BSTNode));
        tr->right = tr->left = NULL;        
        strcpy(tr->data, data);
        *tree = tr;
        return;
    }

    do
    {
        if( strcmp( data, tr->data ) < 0 )
            p_tr = &tr->left;
        else
            p_tr = &tr->right;

        if( *p_tr == NULL )
        {
            tr = (BSTNode*)malloc(sizeof(BSTNode));
            tr->right = tr->left = NULL;
            strcpy(tr->data, data);
            *p_tr = tr;
            return;
        }

        tr = *p_tr;
    }while( tr );
}

BSTNode * bst_inorder_next(BSTNode * node, BSTNode ** parent)
{
    BSTNode * par = node;
    BSTNode * tmp = par->right;

    while( tmp->left )
    {
        par = tmp;
        tmp = tmp->left;
    }

    *parent = par;
    return tmp;
}

int bst_delete(BSTNode ** tree, const char * data)
{
    BSTNode * temp = *tree, * del, * successor, * par = NULL;
    int cmp;

    while( temp )
    {
        cmp = strcmp( data, temp->data );

        if(cmp == 0)
        {
            if( temp->left == NULL )
            {
                del = temp;
                if( par == NULL )
                    *tree = temp->right;
                else if( par->left == temp )
                    par->left = temp->right;
                else
                    par->right = temp->right;
                
                free(del);
            }
            else if( temp->right == NULL )
            {
                del = temp;
                if( par == NULL )
                    *tree = temp->left;
                else if( par->left == temp )
                    par->left = temp->left;
                else
                    par->right = temp->left;
                
                free(del);
            }
            else
            {
                BSTNode * succ_par;
                successor = bst_inorder_next( temp, &succ_par );
                strcpy( temp->data, successor->data );

                temp = successor->right;
                if(succ_par->left == successor)
                {
                    free(successor);
                    succ_par->left = temp;
                }
                else
                {
                    free(successor);
                    succ_par->right = temp;
                }
                
            }
            return 1;
        }
        else if( cmp < 0 )
        {
            par = temp;
            temp = temp->left;
        }
        else
        {
            par = temp;
            temp = temp->right;
        }
    }

    return 0;
}

void bst_free(BSTNode * tree)
{
    if( tree == NULL )
        return;
    
    if( tree->left )
        bst_free( tree->left );

    if( tree->right )
        bst_free( tree->right );

    free( tree );
}

void bst_print(BSTNode * tree)
{
    if( tree == NULL ) return;
    
    bst_print(tree->left);

    printf("%s\n", tree->data);

    bst_print(tree->right);
}

