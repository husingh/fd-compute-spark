#include "splay.h"
/*
           An implementation of top-down splaying with sizes
             D. Sleator <sleator@cs.cmu.edu>, January 1994.

  This extends top-down-splay.c to maintain a size field in each node.
  This is the number of nodes in the subtree rooted there.  This makes
  it possible to efficiently compute the rank of a key.  (The rank is
  the number of nodes to the left of the given key.)  It it also
  possible to quickly find the node of a given rank.  Both of these
  operations are illustrated in the code below.  The remainder of this
  introduction is taken from top-down-splay.c.

  "Splay trees", or "self-adjusting search trees" are a simple and
  efficient data structure for storing an ordered set.  The data
  structure consists of a binary tree, with no additional fields.  It
  allows searching, insertion, deletion, deletemin, deletemax,
  splitting, joining, and many other operations, all with amortized
  logarithmic performance.  Since the trees adapt to the sequence of
  requests, their performance on real access patterns is typically even
  better.  Splay trees are described in a number of texts and papers
  [1,2,3,4].

  The code here is adapted from simple top-down splay, at the bottom of
  page 669 of [2].  It can be obtained via anonymous ftp from
  spade.pc.cs.cmu.edu in directory /usr/sleator/public.

  The chief modification here is that the splay operation works even if the
  item being splayed is not in the tree, and even if the tree root of the
  tree is NULL.  So the line:

                              t = splay(i, t);

  causes it to search for item with key i in the tree rooted at t.  If it's
  there, it is splayed to the root.  If it isn't there, then the node put
  at the root is the last one before NULL that would have been reached in a
  normal binary search for i.  (It's a neighbor of i in the tree.)  This
  allows many other operations to be easily implemented, as shown below.

  [1] "Data Structures and Their Algorithms", Lewis and Denenberg,
       Harper Collins, 1991, pp 243-251.
  [2] "Self-adjusting Binary Search Trees" Sleator and Tarjan,
       JACM Volume 32, No 3, July 1985, pp 652-686.
  [3] "Data Structure and Algorithm Analysis", Mark Weiss,
       Benjamin Cummins, 1992, pp 119-130.
  [4] "Data Structures, Algorithms, and Performance", Derick Wood,
       Addison-Wesley, 1993, pp 367-375
*/

Tree * splay (T i, Tree *t)
/* Splay using the key i (which may or may not be in the tree.) */
/* The starting root is t, and the tree used is defined by rat  */
/* size fields are maintained */
{
    Tree N, *l, *r, *y;
    T comp,l_size, r_size;
    long long l_cum_objsize, r_cum_objsize;
    if (t == NULL) return t;
    N.left = N.right = NULL;
    l = r = &N;
    l_size = r_size = l_cum_objsize = r_cum_objsize = 0;
 
    for (;;) {
        comp = compare(i, t->key);
        if (comp < 0) {
            if (t->left == NULL) break;
            if (compare(i, t->left->key) < 0) {
                y = t->left;                           /* rotate right */
                t->left = y->right;
                y->right = t;
                t->size = node_size(t->left) + node_size(t->right) + 1;
		t->cum_objsize = node_cum_objsize(t->left) + node_cum_objsize(t->right) + t->objsize;
                t = y;
                if (t->left == NULL) break;
            }
            r->left = t;                               /* link right */
            r = t;
            r_size += 1+node_size(r->right);
	    r_cum_objsize += t->objsize+node_cum_objsize(r->right);
            t = t->left;
        } else if (comp > 0) {
            if (t->right == NULL) break;
            if (compare(i, t->right->key) > 0) {
                y = t->right;                          /* rotate left */
                t->right = y->left;
                y->left = t;
		t->size = node_size(t->left) + node_size(t->right) + 1;
		t->cum_objsize = node_cum_objsize(t->left) + node_cum_objsize(t->right) + t->objsize;
                t = y;
                if (t->right == NULL) break;
            }
            l->right = t;                              /* link left */
            l = t;
            l_size += 1+node_size(l->left);
	    l_cum_objsize += t->objsize+node_cum_objsize(l->left);
            t = t->right;
        } else {
            break;
        }
    }

    l_size += node_size(t->left);  /* Now l_size and r_size are the sizes of */
    r_size += node_size(t->right); /* the left and right trees we just built.*/
    l_cum_objsize += node_cum_objsize(t->left);
    r_cum_objsize += node_cum_objsize(t->right);
    t->size = l_size + r_size + 1;
    t->cum_objsize = l_cum_objsize + r_cum_objsize + t->objsize;

    l->right = r->left = NULL;

    /* The following two loops correct the size fields of the right path  */
    /* from the left child of the root and the right path from the left   */
    /* child of the root.                                                 */
    for (y = N.right; y != NULL; y = y->right) {
        y->size = l_size;
	y->cum_objsize = l_cum_objsize;
        l_size -= 1+node_size(y->left);
	l_cum_objsize -= y->objsize+node_cum_objsize(y->left);
    }
    for (y = N.left; y != NULL; y = y->left) {
        y->size = r_size;
        y->cum_objsize = r_cum_objsize;
        r_size -= 1+node_size(y->right);
	r_cum_objsize -= y->objsize+node_cum_objsize(y->right);
    }
 
    l->right = t->left;                                /* assemble */
    r->left = t->right;
    t->left = N.right;
    t->right = N.left;

    return t;
}

Tree * insert(T i, Tree * t, long long objsize) {
/* Insert key i into the tree t, if it is not already there. */
/* Return a pointer to the resulting tree.                   */
    Tree * newtree;

    if (t != NULL) {
        t = splay(i,t);
	if (compare(i, t->key)==0) {
	    return t;  /* it's already there */
	}
    }
    newtree = (Tree *) malloc (sizeof (Tree));
    if (newtree == NULL) {printf("Ran out of space\n"); exit(1);}
    if (t == NULL) {
	newtree->left = newtree->right = NULL;
    } else if (compare(i, t->key) < 0) {
	newtree->left = t->left;
	newtree->right = t;
	t->left = NULL;
	t->size = 1+node_size(t->right);
	t->cum_objsize = t->objsize+node_cum_objsize(t->right);
    } else {
	newtree->right = t->right;
	newtree->left = t;
	t->right = NULL;
	t->size = 1+node_size(t->left);
	t->cum_objsize = t->objsize+node_cum_objsize(t->left);
    }
    newtree->key = i;
    newtree->size = 1 + node_size(newtree->left) + node_size(newtree->right);
    newtree->objsize = objsize;
    newtree->cum_objsize = objsize + node_cum_objsize(newtree->left) + node_cum_objsize(newtree->right);
    return newtree;
}

Tree * treedelete(T i, Tree *t, long long objsize) {
/* Deletes i from the tree if it's there.               */
/* Return a pointer to the resulting tree.              */
    Tree * x;
    T tsize;
    long long t_cum_objsize;

    if (t==NULL) return NULL;
    tsize = t->size;
    t_cum_objsize = t->cum_objsize;
    t = splay(i,t);
    if (compare(i, t->key) == 0) {               /* found it */
	if (t->left == NULL) {
	    x = t->right;
	} else {
	    x = splay(i, t->left);
	    x->right = t->right;
	}
	free(t);
	if (x != NULL) {
	    x->size = tsize-1;
	    x->cum_objsize = t_cum_objsize-objsize;
	}
	return x;
    } else {
	return t;                         /* It wasn't there */
    }
}

Tree *find_rank(T r, Tree *t) {
/* Returns a pointer to the node in the tree with the given rank.  */
/* Returns NULL if there is no such node.                          */
/* Does not change the tree.  To guarantee logarithmic behavior,   */
/* the node found here should be splayed to the root.              */
    T lsize;
    if ((r < 0) || (r >= node_size(t))) return NULL;
    for (;;) {
	lsize = node_size(t->left);
	if (r < lsize) {
	    t = t->left;
	} else if (r > lsize) {
	    r = r - lsize -1;
	    t = t->right;
	} else {
	    return t;
	}
    }
}
void freetree(Tree* t)
{
    if(t==NULL) return;
    freetree(t->right);
    freetree(t->left);
    free(t);
}
void printtree(Tree * t, int d) {
    int i;
    if (t == NULL) return;
    printtree(t->right, d+1);
    for (i=0; i<d; i++) printf("  ");
    printf("%lld(%lld,%lld,%lld)\n", t->key, t->size, t->objsize, t->cum_objsize);
    printtree(t->left, d+1);
}

struct sizeObjsize getsizeObjsize(struct tree_node x) {
  struct sizeObjsize s ;
  s.size = x.size;
  s.objsize = x.cum_objsize;
  return s;
}

