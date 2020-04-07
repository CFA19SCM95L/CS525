#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "btree_mgr.h"
#include "dberror.h"
#include "tables.h"
#include "expr.h"


RC RC_IM_NODE_NOT_EXIST = -1;
RC  RC_IM_TREE_NOT_EXIST = -1;

// init and shutdown index manager
RC initIndexManager (void *mgmtData){
    initStorageManager();
    printf("Initialized the Index Manager.\n");
    return RC_OK;
}

BTreeHandle *th = NULL;
RC shutdownIndexManager (){
    th = NULL;
    printf("Shutting the Index Manager Down...\n");
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n){
    if(idxId == NULL){
        printf("Fail to Create the B-tree.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    int nodeNum = 0;
    int entryNum = 0;
    int rootPage = 0;//These are the contents we added in the BTreeHandle.
    SM_FileHandle *fHandle; //The index should be backed up by a page file.
    char *metadata;
    
    fHandle = (SM_FileHandle *)calloc(1, sizeof(SM_FileHandle));
    metadata = (char *)calloc(1, PAGE_SIZE);
    
    createPageFile(idxId);
    openPageFile(idxId, fHandle);
    ensureCapacity(1, fHandle);

    //assign the messages to metadata
    memcpy(metadata, &keyType, sizeof(DataType)); //keyType
    memcpy(metadata + sizeof(int), &n, sizeof(int)); //int n
    memcpy(metadata + 2 * sizeof(int), &nodeNum, sizeof(int)); //nodeNum
    memcpy(metadata + 3 * sizeof(int), &entryNum, sizeof(int)); //entryNum
    memcpy(metadata + 4 * sizeof(int), &rootPage, sizeof(int)); //rootPage

    writeBlock(0, fHandle, metadata); //write the metadata to the block
    free(fHandle);
    free(metadata);
    printf("Created B-tree Successfully.\n");
    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId){
    if(idxId == NULL){
        printf("Fail to Open the B-tree.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    //list the content of BTreeHandle
    DataType keyType;
    char * metadata; //Change to metaData
    SM_FileHandle *fHandle;
    //pages of the index should be accessed through buffer manager
    BM_BufferPool  *bufferPool;
    int n;
    int nodeNum;
    int entryNum;
    int rootPage;
    
    metadata = (char *)calloc(PAGE_SIZE, sizeof(char));
    fHandle = (SM_FileHandle *)calloc(1, sizeof(SM_FileHandle));
    bufferPool = MAKE_POOL();
    
    openPageFile(idxId, fHandle);
    initBufferPool(bufferPool, idxId, 10, RS_LRU, NULL);
    readBlock(0, fHandle, metadata);
    
    //assign metadata to the BTreeHandle contents
    memcpy(&keyType, metadata, sizeof(DataType));
    memcpy(&n, metadata + sizeof(int), sizeof(int));
    memcpy(&nodeNum, metadata + 2 * sizeof(int), sizeof(int));
    memcpy(&entryNum, metadata + 3 * sizeof(int), sizeof(int));
    memcpy(&rootPage, metadata + 4 * sizeof(int), sizeof(int));

    //assign the BTreeHandle **tree
    *tree = (BTreeHandle *)calloc(1, sizeof(BTreeHandle));
    (*tree)->keyType = keyType;
    (*tree)->idxId = idxId;
    (*tree)->fh = fHandle;
    (*tree)->bp = bufferPool;
    (*tree)->n = n;
    (*tree)->nodeNum = nodeNum;
    (*tree)->entryNum = entryNum;
    (*tree)->rootPage = rootPage;
    
    free(metadata);
    printf("Opened B-tree sccessfully.\n");
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree){
    //it's OK if the tree is NUll
    printf("Closing the B-tree...\n");
    shutdownBufferPool(tree->bp);
    free(tree->bp);
    free(tree->fh);
    free(tree);
    printf("Closed the B-tree sccessfully.\n");
    return RC_OK;
}

RC deleteBtree (char *idxId){
    if(idxId == NULL){
        printf("The parameter is NULL, please check again.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    return destroyPageFile(idxId);
}
// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
    if(tree == NULL){
        printf("Fail to Get the Number of Nodes.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    *result=(*tree).nodeNum;
    printf("Got the Number of Nodes Sccessfully.\n");
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result)
{
    if(tree == NULL){
        printf("Fail to Get the Number of Entries.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    *result=(*tree).entryNum;
    printf("Got the Number of Entries Sccessfully.\n");
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
    if(tree == NULL){
        printf("Fail to Get the KeyType.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    *result=(*tree).keyType;
    printf("Got the KeyType Sccessfully.\n");
    return RC_OK;
}
// index access
/*These functions are used to find, insert, and delete keys in/from a given B+-tree.*/

/*We added a method to help us get the tree node which we want to make some change to it.
  We also added a struct: BT_Node to show the property of the B+-tree's node */
RC getTreeNode (BTreeHandle *tree, int nodeNum, BT_Node **node)
{
    if(tree == NULL){
        printf("Fail to Find the Key.\n");
        return RC_ILLEGAL_PARAMETER;
    }

    BM_PageHandle *ph = (BM_PageHandle *)calloc(1, sizeof(BM_PageHandle));
    bool isNullNode = false;
    RC isSuccess = RC_OK;
    RC result = RC_OK;

    //if *node is NULL, we intialize the node.
    if(*node == NULL){
        isNullNode = true;
        *node = (BT_Node *)calloc(1, sizeof(BT_Node));
    }

    isSuccess = pinPage(tree->bp, ph, nodeNum);
    if(isSuccess == RC_OK){
        //pinPage is successful
        //check the node is vaild or not
        memcpy(&(*node)->isValid, ph->data, sizeof(int));
        if((*node)->isValid == 0){ //the node is not valid
            unpinPage(tree->bp, ph);
            result = RC_IM_NODE_NOT_EXIST; //可替换
        }else{
            //when failed to pin page
            memcpy(&(*node)->parentNode, ph->data + sizeof(int), sizeof(int));
            memcpy(&(*node)->current_node, ph->data + 2 * sizeof(int) , sizeof(int));
            memcpy(&(*node)->size, ph->data + 3 * sizeof(int), sizeof(int));
            memcpy(&(*node)->isLeafNode, ph->data + 4 * sizeof(int), sizeof(int));
            if(isNullNode || (*node)->element == NULL){
                (*node)->element = (BT_Element *)malloc(sizeof(BT_Element) * (tree->n+2) * 2);
            }
            memcpy((*node)->element, ph->data + 5*sizeof(int),
                   sizeof(BT_Element) * (*node)->size);
            result = unpinPage(tree->bp, ph);
        }
    }
    free(ph);
    printf("getTreeNode() Sccessfully.\n");
    return result;
}
//we added the freeTreeNode() method to concise the code
RC freeTreeNode (BT_Node *node){
    if(node == NULL){
        printf("The node is null already.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    
    if(node->element != NULL){//we need to free the element space
        free(node->element);
        node->element = NULL;
    }
    free(node);
    node = NULL;
    printf("The node is free now.\n");
    return RC_OK;
}

//findKey return the RID for the entry with the search key in the b-tree.
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    if(tree == NULL || key == NULL){
        printf("Fail to Find the Key.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    
    BT_Node *node = NULL; //we need to set new node = null
    /*Pointers to intermediate nodes should be \
     represented by the page number of the page \
     the node is stored in. */
    int num = tree->rootPage;
    while(true)
    {
        getTreeNode (tree,num, &node);
        //when node is a non-leaf node.
        if((node->isLeafNode) == 0){
            int i=1;
            if((node->element + 1)->node > key->v.intV)
            {
                num = node->element->node;
            }
            else
            {
                while( i < node->size )
                {
                    if((node->element + i)->node <= key->v.intV)
                    {
                        num = (node->element + i + 1)->node;
                    }
                    i += 2;
                }
            }
        }
        //when node is a leaf node
        if((node->isLeafNode) == 1)
        {
            int i = 1;
            while(i < node->size)
            {
                if((node->element + i)->node == key->v.intV)
                {
                    result->slot=(node->element + i-1)->id.slot;
                    result->page=(node->element + i-1)->id.page;
                    
                    freeTreeNode(node);
                    printf("findKey() Sccessfully.\n");
                    return RC_OK;
                }
                i += 2;
            }
            
            freeTreeNode(node);
            return RC_IM_KEY_NOT_FOUND;
        }
         
    }
}

//We added a method to set node into tree
RC setTreeNode (BTreeHandle *tree, int nodeNum, BT_Node *node){
    if(tree == NULL || nodeNum <= 0){
        printf("Fail to Find the Key.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    if(ensureCapacity(nodeNum+1, tree->fh) == RC_OK
        && pinPage(tree->bp, ph, nodeNum) == RC_OK){
        memcpy(ph->data, &node->isValid, sizeof(int));
        memcpy(ph->data + sizeof(int), &node->parentNode, sizeof(int));
        memcpy(ph->data + 2*sizeof(int), &node->current_node, sizeof(int));
        memcpy(ph->data + 3*sizeof(int), &node->size, sizeof(int));
        memcpy(ph->data + 4*sizeof(int), &node->isLeafNode, sizeof(int));
        memcpy(ph->data + 5*sizeof(int), node->element, sizeof(BT_Element) * node->size);
        markDirty(tree->bp, ph);
        unpinPage(tree->bp, ph);
    }
    free(ph);
    printf("setTreeNode() Sccessfully.\n");
    return RC_OK;
}

//insertKey inserts a new key and record pointer pair into the index.
RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    BT_Element *element = (BT_Element *)malloc(2 * (tree->n+2) * sizeof(BT_Element));
    BT_Node *node = NULL;
    BT_Node *newNode = NULL, *rootNode = NULL;
    // If the B-tree is null, we need to create a root key.
    if(tree->entryNum==0 || tree->rootPage==-1 ||
       getTreeNode(tree, tree->rootPage, &node) != RC_OK){
        node = (BT_Node *)calloc(1, sizeof(BT_Node));
        node->isLeafNode = 1;
        node->size = 2;
        node->isValid = 1;
        node->parentNode = -1;
        node->current_node = tree->fh->totalNumPages;
        appendEmptyBlock(tree->fh);
        node->element = (BT_Element *)malloc(sizeof(BT_Element) * 2 * (tree->n+2));
        (*(node->element)).id = rid;
        (*(node->element+1)).node = key->v.intV;
        tree->nodeNum++;
        tree->rootPage = node->current_node;
        setTreeNode(tree, node->current_node, node);
    } else {
        // Find the target node to insert
        int j = 0;
        while(node->isLeafNode == 0){//non-leaf node
            for(int i=1; ; i+=2){
                if(i>=node->size || (*(node->element+i)).node > key->v.intV){
                    j = (*(node->element+i-1)).node;
                    getTreeNode(tree, j, &node);
                    break;
                }
            }
        }

        int intKey = key->v.intV;
        int left, right;
        while(true) {
            // Insert rid and key into node
            for(int i=1; i < node->size+4; i+=2){
                //It should return error code RC_IM_KEY_ALREADY_EXISTS
                //if this key is already stored in the b-tree.
                if((*(node->element+i)).node > intKey || i>=node->size){
                    if(i > 1 && (*(node->element+i-2)).node == intKey){
                        freeTreeNode(node);
                        return RC_IM_KEY_ALREADY_EXISTS;
                    }
                    if(node->isLeafNode == 1){ //leaf node
                        memcpy(element, node->element, sizeof(BT_Element) * (i-1));
                        (*(element+i-1)).id = rid;
                        (*(element+i)).node = intKey;
                        memcpy(element + i+1, node->element+ i-1, sizeof(BT_Element) * (node->size+1-i));
                        
                    } else {//non-leaf node
                        memcpy(element, node->element, sizeof(BT_Element) * (i-1));
                        (*(element+i)).node = intKey;
                        (*(element+i-1)).node = left;
                        (*(element+i+1)).node = right;
                        memcpy(element + i+2, node->element+ i, sizeof(BT_Element) * (node->size-i));
                    }
                    node->size+=2;
                    memcpy(node->element, element, sizeof(BT_Element) * node->size);
                    break;
                }
            }

            if(node->size < 2 * (tree->n+1)){
                setTreeNode(tree, node->current_node, node);
                break;
            }
            else {/*when node overflow, we need to follow the conventions*/
                if(newNode== NULL){
                    newNode = (BT_Node *)malloc(sizeof(BT_Node));
                    newNode->element = (BT_Element *)calloc(2*(tree->n+2), sizeof(BT_Element));
                }
                
                newNode->isValid = 1;
                newNode->isLeafNode = node->isLeafNode;
                newNode->size = node->size/4*2 + node->size%2;
                newNode->current_node = tree->fh->totalNumPages;
                newNode->parentNode = node->parentNode;
                appendEmptyBlock(tree->fh);
                memcpy(newNode->element, node->element + node->size - newNode->size,
                    sizeof(BT_Element)*newNode->size);
                node->size = (node->size/2 - node->size/4)*2 + 1;
                (*(node->element+node->size-1)).node = newNode->current_node;

                tree->nodeNum++;
                if(node->parentNode == -1){
                    left = node->current_node;
                    if(rootNode == NULL){//create a parent node
                        rootNode = (BT_Node *)malloc(sizeof(BT_Node));
                        rootNode->isLeafNode = 0;
                        rootNode->isValid = 1;
                        rootNode->parentNode = -1;
                        rootNode->size = 3;
                        rootNode->element = (BT_Element *)malloc(sizeof(BT_Element) * 3);
                    }

                    rootNode->current_node = tree->fh->totalNumPages;
                    appendEmptyBlock(tree->fh);
                    
                    (*(rootNode->element)).node = left;
                    (*(rootNode->element+1)).node = (*(newNode->element+1)).node;
                    (*(rootNode->element+2)).node = newNode->current_node;

                    tree->rootPage = rootNode->current_node;
                    node->parentNode = rootNode->current_node;
                    tree->nodeNum++;
                    newNode->parentNode = rootNode->current_node;
                    
                    setTreeNode(tree, node->current_node, node);
                    setTreeNode(tree, newNode->current_node, newNode);
                    setTreeNode(tree, rootNode->current_node, rootNode);
                    break;
                }
                else { //node has parent node
                    setTreeNode(tree, node->current_node, node);
                    setTreeNode(tree, newNode->current_node, newNode);
                    intKey = (*(newNode->element+1)).node;
                    left = node->current_node;
                    right = newNode->current_node;
                    getTreeNode(tree, node->parentNode, &node);
                }
            }
        }
    }
    tree->entryNum++;
    free(element);
    freeTreeNode(node);
    freeTreeNode(newNode);
    freeTreeNode(rootNode);
    printf("Insert Key Done.\n");
    return RC_OK;
}

//deleteKey removes a key (and corresponding record pointer) from the index.
//For deletion it is up to the client whether this is handled as an error.
RC deleteKey (BTreeHandle *tree, Value *key){
    //If we cannot find the key in tree or the tree is null
    BT_Node *node = NULL;
    if(tree->rootPage == -1 || getTreeNode(tree, tree->rootPage, &node) != RC_OK){
        freeTreeNode(node);
        printf("The Key Is Not Found.\n");
        return RC_IM_KEY_NOT_FOUND;
    }
    // Step 1: Find the key node
    int i;
    while(node->isLeafNode == 0) //non-leaf node
    {
        for(i=1;;i+=2)
        {
            if(i>=node->size || (*(node->element+i)).node > key->v.intV)
                {
                getTreeNode(tree, (*(node->element+i-1)).node, &node);
                break;
                }
        }
    }
    // Step 2: Find the key
    for(i=1; i<node->size; i+=2)
    {
        if((*(node->element + i)).node > key->v.intV)
        {
            break;//cannot find the key
        }
        else
        {
                printf("Error");
        }
    }
    i -= 2;
    
    if((*(node->element+i)).node != key->v.intV)
    {       // If the key to be found does not exist
        freeTreeNode(node);
        printf("The Key Is Not Found.\n");
        return RC_IM_KEY_NOT_FOUND;
    }

    // Step 3: Delete the key
    BT_Node *childNode = NULL, *siblingNode = NULL;
    while(i<node->size){
        (*(node->element+i-1)).id = (*(node->element+i+1)).id;
        (*(node->element+i)).node = (*(node->element+i+2)).node;
        i += 2;
    }
    node->size -= 2;
    tree->entryNum--;
    //If the key is exist in the parent node, we need to remove it.
    while(node->size<2 && node->current_node!=0)
        {
        getTreeNode(tree, node->current_node, &childNode);
        childNode->isValid = 0;
        getTreeNode(tree, node->parentNode, &node);
        for(i=0; i<node->size; i+=2)
            {
                if((*(node->element+i)).node == childNode->current_node)
                {
                    break;
                }
            }
        if(i == 0)
        {
            for(;i+2<node->size;i++)
            {
                (*(node->element+i)).node = (*(node->element+i+2)).node;
            }
        }
        else
            {
            i--;
            if(childNode->size == 1 && childNode->isLeafNode == 1)
            {       // Sibling point to next
                getTreeNode(tree, (*(node->element+i-1)).node, &siblingNode);
                (*(node->element+siblingNode->size-1)).node = (*childNode->element).node;
                setTreeNode(tree, siblingNode->current_node, siblingNode);
            }
            for(;i+2<node->size;i++)
            {
                (*(node->element+i)).node = (*(node->element+i+2)).node;
            }
        }
        tree->nodeNum--;
        node->size -= 2;
        setTreeNode(tree, childNode->current_node, childNode);
    }
    setTreeNode(tree, node->current_node, node);
    freeTreeNode(node);
    freeTreeNode(childNode);
    freeTreeNode(siblingNode);
    printf("Deleted Key Sccessfully.\n");
    return RC_OK;
}

/*clients can scan through all entries of a BTree \
 *in sort order using the openTreeScan, nextEntry, and closeTreeScan methods. */
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
    if(tree == NULL){
        printf("Fail to Open Tree Scan.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    
    *handle = (BT_ScanHandle*)calloc(1, sizeof(BT_ScanHandle));
    (*handle)->tree = tree;
    int pageNum = tree->rootPage;
    BT_Node *node = NULL;
    getTreeNode (tree, pageNum, &node);
    //when the node is non-leaf node
    while(node->isLeafNode == 0){
         pageNum = node->element->node;
         getTreeNode(tree, pageNum, &node);
     }
    //when the node is leaf node
    (*handle)->currentNode = node->current_node;
    (*handle)->currentPos = 0;
    
    getNumEntries(tree, &pageNum);
    (*handle)->count = pageNum;
     
    freeTreeNode(node);
    printf("openTreeScan() Sccessfully.\n");
    return RC_OK;
}

/*The nextEntry method should return RC_IM_NO_MORE_ENTRIES
 *if there are no more entries to be returned
 (the scan has gone beyond the last entry of the B+-tree). */
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    if(handle == NULL){
        printf("Fail to Open Tree Scan.\n");
        return RC_ILLEGAL_PARAMETER;
    }
    BT_Node *node=NULL;
    //there are no more entries to be returned
    if(handle->count <= 0){
        freeTreeNode(node);
        printf("There are no more entries to be returned.\n");
        return RC_IM_NO_MORE_ENTRIES;
    }
    
    getTreeNode (handle->tree, handle->currentNode, &node);
    result->slot = (node->element+handle->currentPos)->id.slot;
    result->page = (node->element+handle->currentPos)->id.page;
    handle->currentPos = handle->currentPos + 2;
    handle->count--;
    
    if(handle->currentPos == node->size - 1 && handle->count != 0)
    {
        handle->currentNode = (node->element+handle->currentPos)->node;
        handle->currentPos=0;
    }

    freeTreeNode(node);
    printf("nextEntry() Sccessfully.\n");
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle)
{
    if(handle == NULL){
        printf("The handle is already null.\n");
        return RC_OK;
    }
    
    free(handle);
    printf("The closeTreeScan() Sccessfully.\n");
    return RC_OK;
}

// debug and test functions
//printTree is used to create a string representation of a b-tree.
char *printTree (BTreeHandle *tree){
    if(tree == NULL){
           printf("The tree is null.\n");
           return RC_ILLEGAL_PARAMETER;
    }
    
    BT_Node *node = NULL;
    int print[100];
    
    if((*tree).rootPage == -1)
    {
        printf("The Tree is Null.\n");
        return RC_IM_TREE_NOT_EXIST;
    }
    
    printf("");
    int num = 0, size = 1;
    print[0] = (*tree).rootPage;
    while (num != size)
    {
        getTreeNode(tree, print[num], &node);
        printf("(%d)[", print[num]);
        if ((*node).isLeafNode == 1) //leaf node
        {
            for (int i=0; i<(*node).size; i++){
                if (i%2 == 0 && i != node->size - 1)//n is even
                    {
                        printf("%d.%d ", (*((*node).element + i)).id.page,
                               (*((*node).element + i)).id.slot);
                    } else {
                        printf("%d ", (*((*node).element+i)).node);
                    }
            }
        }
        //non-leaf node
        else{
           for (int i=0; i<(*node).size; i++){
               printf("%d ", (*((*node).element + i)).node);
               if ( i%2 == 0 )
               {
                   print[size] = (*((*node).element + i)).node;
                   size++;
               }
           }
        }
        num++;
        printf("]\n");
    }
    freeTreeNode(node);
    printf("---------------- DONE ----------------");
}



