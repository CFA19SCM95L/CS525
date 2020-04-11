#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "btree_mgr.h"
#include "dberror.h"
#include "assist2.h"


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
    shutdownBufferPool((*tree).bp);
    free((*tree).bp);
    free((*tree).fh);
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

//////////////////////////////////////////////////////


// access b-tree
RC getNumNodes (BTreeHandle *tree, int *result) {
    *result=(*tree).nodeNum;
    printf("GetNumNodes: Successed...\n");
    return RC_OK;
}


RC getNumEntries (BTreeHandle *tree, int *result) {
    *result=(*tree).entryNum;
    printf("GetNumEntries: Successed...\n");
    return RC_OK;
}


RC getKeyType (BTreeHandle *tree, DataType *result) {
    *result=(*tree).keyType;
    printf("GetKeyType: Successed...\n");
    return RC_OK;
}
// index access
RC getTreeNode (BTreeHandle *tree, int nodeNum, BT_Node **node) {
    BM_PageHandle *ph = malloc(32);
    if(*node == NULL){                   
        *node = malloc(32);
    }
    int isNullNode = *node == NULL?1:0;
    if(pinPage((*tree).bp, ph, nodeNum) == RC_OK){          //check vaild
        memcpy(&(*(*node)).isValid, (*ph).data, 4);
        if((*(*node)).isValid == 0){                        //not valid
            if (unpinPage((*tree).bp, ph) != RC_OK) {
                return -1;
            }
        }else{                                                  //pin page
            memcpy(&(*(*node)).parentNode, (*ph).data + 4, 4);
            memcpy(&(*(*node)).current_node, (*ph).data + 8 , 4);
            memcpy(&(*(*node)).size, (*ph).data + 12, 4);
            memcpy(&(*(*node)).isLeafNode, (*ph).data + 16, 4);
            if(isNullNode == 1 || (*(*node)).element == NULL){
                (*(*node)).element = calloc(8 , ((*tree).n+2) * 2);
            }
            memcpy((*(*node)).element, (*ph).data + 20, 8 * (*(*node)).size);
            if (unpinPage((*tree).bp, ph) != RC_OK) {
                return -1;
            }
        }
    }
    return RC_OK;
}
//return RID for the entry.
RC findKey (BTreeHandle *tree, Value *key, RID *result) {  
    BT_Node *node = NULL; 
    int num = (*tree).rootPage;
    while(true) {
        if (getTreeNode (tree, num, &node)!= RC_OK) {
            return -1;
        }
        if(((*node).isLeafNode) != 0){   //leaf node     
            int i = 1;
            int flag = i < (*node).size ? 1:0;
            while (flag) {
                if((*((*node).element + i)).node == (*key).v.intV) {
                    (*result).slot=(*((*node).element + i-1)).id.slot;
                    (*result).page=(*((*node).element + i-1)).id.page;
                    printf("FindKey: Sccessed...\n");
                    return RC_OK;
                }
                i+=2;
                if (i >= (*node).size) {
                    break;
                }
            }
            return RC_IM_KEY_NOT_FOUND;  
        } else {                    //non-leaf node
            num = (*((*node).element + 1)).node > (*key).v.intV?(*(*node).element).node: num;
            if ((*((*node).element + 1)).node <= (*key).v.intV) {
                int i = 1;
                int flag =  i < (*node).size ? 1:0;
                while (flag) {
                    if((*((*node).element + i)).node <= (*key).v.intV) {
                        num = (*((*node).element + i + 1)).node;
                    }
                    i+=2;
                    if (i >= (*node).size) {
                        break;
                    }
                }
            }          
        }        
    }
}


RC setTreeNode (BTreeHandle *tree, int nodeNum, BT_Node *node){
    BM_PageHandle *ph = calloc(32, 1);
    int con;
    int con2;
    if (con = ensureCapacity(nodeNum+1, (*tree).fh)!=RC_OK) {
        return -1;
    }    
    if (con2 =pinPage((*tree).bp, ph, nodeNum)!=RC_OK) {
        return -1;
    }
    if (con && con2) {
        return -1;
    }
    memcpy((*ph).data, &(*node).isValid, 4);
    memcpy((*ph).data + 4, &(*node).parentNode, 4);
    memcpy((*ph).data + 8, &(*node).current_node, 4);
    memcpy((*ph).data + 12, &(*node).size, 4);
    memcpy((*ph).data + 16, &(*node).isLeafNode, 4);
    memcpy((*ph).data + 20, (*node).element, 8 * (*node).size);
    if (markDirty((*tree).bp, ph)!= RC_OK) {
        return -1;
    }
    if (unpinPage((*tree).bp, ph) != RC_OK) {
        return -1;
    }
    printf("SetTreeNode: Successed...\n");
    return RC_OK;
}

//insertKey seperate into 3 methods since it's too long.
int insertKey3(BTreeHandle *tree, Value *key, RID rid,BT_Node *node,BT_Element *element) {
    int intKey = (*key).v.intV;
    int left;
    int right;
    while(true) {
        // Insert rid and key into node
        int i = 1;
        int flag = i < (*node).size+4 ? 1: 0; 
        while (flag) {
            if((*((*node).element+i)).node > intKey || i>=(*node).size){
                if(i > 1 && (*((*node).element+i-2)).node == intKey){
                    return RC_IM_KEY_ALREADY_EXISTS;
                }
                if((*node).isLeafNode != 1){ //non-leaf
                    memcpy(element, (*node).element, 8 * (i-1));
                    (*(element+i)).node = intKey;
                    (*(element+i+1)).node = right;
                    (*(element+i-1)).node = left;
                    memcpy(element + i+2, (*node).element+ i, 8 * ((*node).size-i));         
                } else {                    //leaf
                    memcpy(element, (*node).element, 8 * (i-1));
                    (*(element+i-1)).id = rid;
                    (*(element+i)).node = intKey;
                    memcpy(element + i+1, (*node).element+ i-1, 8 * ((*node).size+1-i));
                }
                (*node).size+=2;
                memcpy((*node).element, element, 8 * (*node).size);
                break;
            }    
            i+=2; 
            if (i >= (*node).size+4) {
                break;
            }           
        }
        if((*node).size < 2 * ((*tree).n+1)){
            if (setTreeNode(tree, (*node).current_node, node)!=RC_OK) {
                return -1;
            }
            break;
        } else {            //overflow
            BT_Node *newNode = NULL;
            if(newNode== NULL){
                newNode = calloc(32, 1);
                (*newNode).element = malloc(2*((*tree).n+2) * 8);
            }                
            (*newNode).isValid = 1;
            (*newNode).isLeafNode = (*node).isLeafNode;
            (*newNode).size = (*node).size/4*2 + (*node).size%2;
            (*newNode).current_node = (*(*tree).fh).totalNumPages;
            (*newNode).parentNode = (*node).parentNode;
            if (appendEmptyBlock((*tree).fh) != RC_OK) {
                return -1;
            }
            memcpy((*newNode).element, (*node).element + (*node).size - (*newNode).size, 8*(*newNode).size);
            (*tree).nodeNum++;
            (*node).size = ((*node).size/2 - (*node).size/4)*2 + 1;
            (*((*node).element+(*node).size-1)).node = (*newNode).current_node;
            if((*node).parentNode == -1){
                BT_Node *rootNode = NULL;
                left = (*node).current_node;
                if(rootNode == NULL){       //create a parent node
                    rootNode = calloc(32, 1);
                    (*rootNode).parentNode = -1;
                    (*rootNode).isLeafNode = 0;
                    (*rootNode).isValid = 1;
                    (*rootNode).size = 3;
                    (*rootNode).element = calloc(8 , 3);
                }
                (*rootNode).current_node = (*(*tree).fh).totalNumPages;
                if (appendEmptyBlock((*tree).fh)!= RC_OK) {
                    return -1;
                }                    
                (*((*rootNode).element)).node = left;
                (*((*rootNode).element+1)).node = (*((*newNode).element+1)).node;
                (*((*rootNode).element+2)).node = (*newNode).current_node;
                (*tree).rootPage = (*rootNode).current_node;
                (*node).parentNode = (*rootNode).current_node;
                (*tree).nodeNum++;
                (*newNode).parentNode = (*rootNode).current_node;  
                if (setTreeNode(tree, (*node).current_node, node)!=RC_OK) {
                    return -1;
                }
                if (setTreeNode(tree, (*newNode).current_node, newNode)!=RC_OK) {
                    return -1;
                }
                if (setTreeNode(tree, (*rootNode).current_node, rootNode)!=RC_OK) {
                    return -1;
                }
                break;
            } else { //node has parent node
                if (setTreeNode(tree, (*node).current_node, node) != RC_OK) {
                    return -1;
                }
                if (setTreeNode(tree, (*newNode).current_node, newNode) != RC_OK) {
                    return -1;
                }
                intKey = (*((*newNode).element+1)).node;
                right = (*newNode).current_node;
                left = (*node).current_node;
                if (getTreeNode(tree, (*node).parentNode, &node)!= RC_OK) {
                    return -1;
                }
            }
        }
    }
    return 1;
}
int insertKey2(BTreeHandle *tree, Value *key, RID rid) {
    BT_Element *element = calloc(2 * ((*tree).n+2) , 8);
    BT_Node *node = NULL;
    if (getTreeNode(tree, (*tree).rootPage, &node)!= RC_OK) {
        return -1;
    }
    // Find the target node to insert
    int j = 0;
    int flag = (*node).isLeafNode == 0? 1:0;
    while(flag){//non-leaf node
        int i = 1;
        while (true) {
            if(i>=(*node).size || (*((*node).element+i)).node > (*key).v.intV){
                j = (*((*node).element+i-1)).node;
                if (getTreeNode(tree, j, &node)!= RC_OK) {
                    return -1;
                }
                break;
            }
            i+=2;
        }
        if ((*node).isLeafNode != 0) {
            break;
        }
    }
    if (insertKey3(tree,key,rid,node,element) != 1) {
        return -1;
    }
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    BT_Element *element = calloc(2 * ((*tree).n+2) , 8);
    BT_Node *node = NULL;
    if((*tree).entryNum==0 || (*tree).rootPage==-1 ){    //create root.
        node = malloc(32);
        (*node).current_node = (*(*tree).fh).totalNumPages;
        (*node).size = 2; 
        (*node).isLeafNode = 1;
        (*node).isValid = 1;
        (*node).parentNode = -1;
        if (appendEmptyBlock((*tree).fh) != RC_OK) {
            return -1;
        }
        (*node).element = calloc(8 , 2 * ((*tree).n+2));
        (*((*node).element)).id = rid;
        (*((*node).element+1)).node = (*key).v.intV;
        (*tree).nodeNum++;
        (*tree).rootPage = (*node).current_node;
        if (setTreeNode(tree, (*node).current_node, node) != RC_OK) {
            return -1;
        }
    } else {
        if (insertKey2(tree, key, rid) != 1) {
            return -1;
        }
    }
    (*tree).entryNum++;
    printf("InsertKey: Successed...\n");
    return RC_OK;
}

//deleteKey seperate into two methods
int removekey(int i , BT_Node *node, BTreeHandle *tree) {
    BT_Node *childNode = NULL;
    int f = i<(*node).size? 1:0;
    while(f){
        (*((*node).element+i-1)).id = (*((*node).element+i+1)).id;
        (*((*node).element+i++)).node = (*((*node).element+i+2)).node;
        if (++i>=(*node).size) {
            break;
        }
    }
    (*node).size -= 2;
    (*tree).entryNum--;
    while((*node).size<2 && (*node).current_node!=0) {    //Exist in the parent node
        if (getTreeNode(tree, (*node).current_node, &childNode)!= RC_OK) {
            return -1;
        }
        (*childNode).isValid = 0;
        if (getTreeNode(tree, (*node).parentNode, &node)!= RC_OK) {
            return -1;
        }
        int k = 0;
        int cond = k<(*node).size? 1:0; 
        while (cond) {
            if((*((*node).element+k++)).node == (*childNode).current_node){
                break;
            }
            if (++k>=(*node).size) {
                break;
            }
        }
        if(k != 0) {
            k--;
            int cond1 = (*childNode).size == 1?1:0;
            int cond2 = (*childNode).isLeafNode == 1?1:0;
            BT_Node *siblingNode = NULL;
            if(cond1 && cond2) {       // Sibling point to next
                if (getTreeNode(tree, (*((*node).element+k-1)).node, &siblingNode)!= RC_OK) {
                    return -1;
                }
                (*((*node).element+(*siblingNode).size-1)).node = (*(*childNode).element).node;
                if (setTreeNode(tree, (*siblingNode).current_node, siblingNode) != RC_OK) {
                    return -1;
                }
            }
            int f = k+2<(*node).size? 1:0;
            while (f) {
                (*((*node).element+k)).node = (*((*node).element+k+2)).node;
                if (++k+2>=(*node).size) {
                    break;
                }
            }
        } else {
            int f = k+2<(*node).size? 1:0;
            while (f) {
                (*((*node).element+k)).node = (*((*node).element+k+2)).node;
                if (++k+2>=(*node).size) {
                    break;
                }
            }
        }
        (*tree).nodeNum--;
        (*node).size -= 2;
        if (setTreeNode(tree, (*childNode).current_node, childNode) != RC_OK) {
            return -1;
        }
    }
    if (setTreeNode(tree, (*node).current_node, node)!= RC_OK) {
        return -1;
    }
}


RC deleteKey (BTreeHandle *tree, Value *key){
    if((*tree).rootPage == -1 ){
        return RC_IM_KEY_NOT_FOUND;
    }
    BT_Node *node = NULL;
    if (getTreeNode(tree, (*tree).rootPage, &node)!= RC_OK) {
        return -1;
    }
    //Find node
    int j = 1;
    int flag = (*node).isLeafNode == 0 ? 1:0; 
    while(flag) { //non-leaf node
        if(j>=(*node).size || (*((*node).element+j)).node > (*key).v.intV){
            if (getTreeNode(tree, (*((*node).element+j-1)).node, &node)!= RC_OK) {
                return -1;
            }
            break;
        } 
        if ((*node).isLeafNode != 0) {
            break;
        }   
        j+=2;    
    }
    // Find key
    int i = 1;
    flag = i<(*node).size? 1:0; 
    while (flag) {
        if((*((*node).element + i)).node > (*key).v.intV){
            break;//cannot find the key
        }
        i+=2;
        if (i>=(*node).size) {
            break;
        }
    }
    i -= 2;
    
    if((*((*node).element+i)).node != (*key).v.intV) {       //not exist
        return RC_IM_KEY_NOT_FOUND;
    }
    removekey(i, node, tree);
    printf("DeleteKey: Successed...\n");
    return RC_OK;
}

// scan by using nextEntry, and closeTreeScan method
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){ 
    *handle = malloc( 32);
    (*(*handle)).tree = tree;
    BT_Node *node = NULL;
    int isLeaf = 0;
    if (getTreeNode (tree, (*tree).rootPage, &node)!= RC_OK) {
        return -1;
    }
    int flag = (*node).isLeafNode == 0 ? 1:0; 
    while(flag){    //nonleaf node
        if (getTreeNode(tree,  (*(*node).element).node, &node)!= RC_OK) {
            return -1;
        }
        if ((*node).isLeafNode != 0) {
            isLeaf = 1;
            break;
        }
    }
    if (isLeaf) {    //leaf node
        (*(*handle)).currentPos = 0;
        (*(*handle)).currentNode = (*node).current_node;
        if (getNumEntries(tree, & (*(*node).element).node) != RC_OK) {
            return -1;
        }
        (*(*handle)).count =  (*(*node).element).node;
    } else {
        return -1;
    }
    printf("OpenTreeScan: Successed.\n");
    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result) {
    if((*handle).count <= 0){           
        return RC_IM_NO_MORE_ENTRIES;
    }
    BT_Node *node=NULL;
    if (getTreeNode ((*handle).tree, (*handle).currentNode, &node)!= RC_OK) {
        return -1;
    }
    (*handle).count--;
    (*result).page = (*((*node).element+(*handle).currentPos)).id.page;
    (*result).slot = (*((*node).element+(*handle).currentPos)).id.slot;
    (*handle).currentPos += 2;
    int cond = (*handle).currentPos == (*node).size - 1 && (*handle).count ? 1:0;
    (*handle).currentNode = cond?(*((*node).element+(*handle).currentPos)).node:(*handle).currentNode;
    (*handle).currentPos = cond ?  0: (*handle).currentPos;
    printf("NextEntry: Successed...\n");
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle) {
    handle = NULL;
    printf("CloseTreeScan: Successed...\n");
    return RC_OK;
}


char *printTree (BTreeHandle *tree){
    int print[100];
    if((*tree).rootPage == -1) {
        return -1;
    }
    int size = 1;
    print[0] = (*tree).rootPage;
    for (int num = 0; num != size; num++) {
        BT_Node *node = NULL;
        if (getTreeNode(tree, print[num], &node)!= RC_OK) {
            return -1;
        }
        if ((*node).isLeafNode != 1) {
           for (int i=0; i<(*node).size; i++){
               if ( i%2 == 0 ) {
                   print[size++] = (*((*node).element + i)).node;
               }
           }
        }
    }
}



