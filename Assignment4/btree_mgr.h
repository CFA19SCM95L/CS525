#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"

////structure for accessing btrees
// typedef struct BTreeHandle {
//   DataType keyType;
//   char *idxId;
//   void *mgmtData;
// } BTreeHandle;

// typedef struct BT_ScanHandle {
//   BTreeHandle *tree;
//   void *mgmtData;
// } BT_ScanHandle;

typedef struct BTreeHandle {
    DataType keyType;
    char *idxId;
    void *mgmtData;
    SM_FileHandle *fh;
    BM_BufferPool *bp;
    int n;
    int nodeNum;
    int entryNum;
    int rootPage;
} BTreeHandle;

typedef union BT_Element {
  int node;
  RID id;
} BT_Element;

//show the property of the B+-tree's node
typedef struct BT_Node {
    int isLeafNode;//False=0, TRUE=1;
    int parentNode;
    int current_node;
    int isValid;
    int size;
    BT_Element *element;
} BT_Node;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
    int count;
    int currentNode;
    int currentPos;
} BT_ScanHandle;




// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);




// debug and test functions
extern char *printTree (BTreeHandle *tree);

#endif // BTREE_MGR_H
