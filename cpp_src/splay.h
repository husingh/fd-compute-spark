
/*
           An implementation of top-down splaying with sizes
             D. Sleator <sleator@cs.cmu.edu>, January 1994.
*/
#ifndef _splay_h
#define _splay_h
#include <stdio.h>
#include <stdlib.h>
//#pragma warning(disable:593)
typedef struct tree_node Tree;
typedef long long T;
struct sizeObjsize {
    T size ;
    long long objsize ;
} ;

struct tree_node {
    Tree * left, * right;
    T key;
    T size;   /* maintained to be the number of nodes rooted here */
    long long objsize;
    long long cum_objsize;
};

#define compare(i,j) ((i)-(j))
/* This is the comparison.                                       */
/* Returns <0 if i<j, =0 if i=j, and >0 if i>j                   */
 
#define node_size(x) (((x)==NULL) ? 0 : ((x)->size))
/* This macro returns the size of a node.  Unlike "x->size",     */
/* it works even if x=NULL.  The test could be avoided by using  */
/* a special version of NULL which was a real node with size 0.  */
 
#define node_cum_objsize(x) (((x)==NULL) ? 0 : ((x)->cum_objsize))

Tree * splay (T i, Tree *t);
Tree * insert(T i, Tree * t, long long objsize); 
Tree * treedelete(T i, Tree *t, long long objsize); 
Tree *find_rank(T r, Tree *t); 
void printtree(Tree * t, int d); 
void freetree(Tree* t);
struct sizeObjsize getsizeObjsize(struct tree_node x) ;

#endif 

