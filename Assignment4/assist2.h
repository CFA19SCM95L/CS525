#include <stdlib.h>
#include <string.h>
#include "btree_mgr.h"


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