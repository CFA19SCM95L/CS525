/*
    A20435695 Chen En Lee
    A20437470 Jingeun Jung
*/

#include "stdio.h"
#include "stdlib.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dt.h"
#include "dberror.h"
#include "limits.h"
#define RC_BUFFER_ERROR 5




/*
	initBufferPool creates a new buffer pool with numPages page frames using the page replacement strategy. 
	The pool is used to cache pages from the page file with name pageFileName. Initially, all page frames should be empty. 
	The page file should already exist, i.e., this method should not generate a new page file. 
	stratData can be used to pass parameters for the page replacement strategy. For example, for LRU-k this could be the parameter k.
*/
void initPage (BM_PageHandle *const page) {
    (*page).data = NULL;
    (*page).pageNum = INT_MIN;
    (*page).dirty = false;
    (*page).fixCounts = 0;
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData){
    (*bm).pageFile = (char *)pageFileName;
    (*bm).numPages = numPages;
    (*bm).strategy = strategy;
    (*bm).mgmtData = (BM_PageHandle *)calloc(numPages, sizeof(BM_PageHandle));
    int i = 0;
    while (i < numPages) initPage((*bm).mgmtData + i++);    
    (*bm).readNum = 0;
    (*bm).writeNum = 0;
    (*bm).count = 0;
    printf("InitBuffer: Successful.\n");
    return RC_OK;
}

/*  shutdownBufferPool:destroys a buffer pool. This method should free up
 *  all resources associated with buffer pool. For example, it should free
 *  the memory allocated for page frames. If the buffer pool contains any
 *  dirty pages, then these pages should be written back to disk before
 *  destroying the pool. It is an error to shutdown a buffer pool that has
 *  pinned pages.
 */

RC shutdownBufferPool(BM_BufferPool *const bm){
    if(bm == NULL) return RC_BUFFER_ERROR;
    int *fixCounts = getFixCounts(bm);
    forceFlushPool(bm);
    free(fixCounts);
    free((*bm).mgmtData);
    printf("Shut down Buffer Pool: Successfull.\n");
    return RC_OK;
 }

/*
 * forceFlushPool causes all dirty pages (with fixcounts 0)
 * from the buffer pool to be written to disk.
 */

RC forceFlushPool(BM_BufferPool *const bm){   
    if(bm == NULL) return RC_BUFFER_ERROR;
    bool *dirty = getDirtyFlags(bm);
    for(int i=0; i< (*bm).numPages; i++){      
        if(*(dirty + i) == false) continue;
        if(*(getFixCounts(bm) + i) == false) forcePage(bm, ((*bm).mgmtData)+i);
        (*(((*bm).mgmtData) + i)).dirty = false;
    }
    free(dirty);
    free(getFixCounts(bm));
    printf("ForceFlush: Successful.");
    return RC_OK;
}

//markDirty marks a page as dirty.
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    for(int i = 0; i < (*bm).numPages; i++) {
        if ((*((*bm).mgmtData + i)).pageNum == (*page).pageNum) {
            (*((*bm).mgmtData + i)).dirty = true;
            (*page).dirty = true;
            break;
        }
    }
    printf("MarkDirty: Successful.\n");
    return RC_OK;
}

/*
    unpinPage unpins the page page.
    The pageNum field of page should be used to figure out which page to unpin.
*/
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    for(int i = 0; i < (*bm).numPages; i++) {
        if ((*((*bm).mgmtData + i)).pageNum == (*page).pageNum) {
            (*((*bm).mgmtData + i)).fixCounts--;
            break;
        }
    }
    printf("UnpinPage: Successful.\n");
    return RC_OK;
}

//forcePage should write the current content of the page back to the page file on disk.
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    FILE *curFile = fopen((*bm).pageFile, "rb+");
    int pageNum = (*page).pageNum;
    fseek(curFile, pageNum*PAGE_SIZE, SEEK_SET);
    fwrite((*page).data, PAGE_SIZE, 1, curFile);
    fclose(curFile);
    (*bm).writeNum++;
    for(int i = 0; i < (*bm).numPages; i++) {       
        if ((*((*bm).mgmtData + i)).pageNum != pageNum) continue;
        (*((*bm).mgmtData + i)).dirty = false;
    }
    printf("ForcePage: Successful.\n");
    return RC_OK;
}

RC freshStrategy(BM_BufferPool *bm, BM_PageHandle *pageHandle) {
    if ((*bm).strategy != RS_FIFO && (*bm).strategy != RS_LRU) {
        return RC_BUFFER_ERROR;
    }
    if ((*pageHandle).strategyRecords == NULL) {
        if ((*bm).strategy == RS_FIFO || (*bm).strategy == RS_LRU) (*pageHandle).strategyRecords = calloc(1, sizeof(int));
    }   
    int *sNum = (int *)(*pageHandle).strategyRecords;
    *sNum = ((*bm).count++);
    return RC_OK;
}


void modify(BM_BufferPool *const bm, bool isExist, BM_PageHandle *const page, const PageNumber pageNum, int p) {
    BM_PageHandle *pg = ((*bm).mgmtData + p);
    if (isExist) {
        (*page).data = (*pg).data;
        (*page).pageNum = pageNum;
        (*pg).fixCounts++;
        (*page).fixCounts = (*pg).fixCounts;
        (*page).dirty = (*pg).dirty;
        (*page).strategyRecords = (*pg).strategyRecords;
    } else {       
        FILE* file = fopen((*bm).pageFile, "r");
        fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
        fread((*pg).data, sizeof(char), PAGE_SIZE, file);
        (*page).data = (*pg).data;
        (*pg).pageNum = pageNum;
        (*page).pageNum = pageNum;
        (*pg).fixCounts++;
        (*page).fixCounts = (*pg).fixCounts;
        (*page).dirty = (*pg).dirty;
        (*page).strategyRecords = (*pg).strategyRecords;
        (*bm).readNum++;
        freshStrategy(bm, pg);
        fclose(file);   
    }
}

struct Result
{
    int p;
    bool flag;
};

struct Result check (int val, BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    int p = val;
    bool isExist;
    for (int check = 0; check < (*bm).numPages; check++) {

        if ((*((*bm).mgmtData + check)).pageNum == pageNum) {   //in the buffer pool
            p = check;
            isExist = TRUE;
            if ((*bm).strategy == RS_LRU){ 
                freshStrategy(bm, (*bm).mgmtData + p); 
            }
            break;
        }
        if ((*((*bm).mgmtData + check)).pageNum < 0) {      //empty page
            (*((*bm).mgmtData + check)).data = (char*)malloc(PAGE_SIZE * sizeof(char));
            p = check;
            isExist = FALSE;
            break;
        }

        if (check == (*bm).numPages - 1) {      //full
            isExist = FALSE;          
            if ((*bm).strategy != RS_FIFO && (*bm).strategy != RS_LRU) {
                continue;
            }
            int *strategyNum = (int *)calloc((*bm).numPages, sizeof((*((*bm).mgmtData)).strategyRecords));
            int least = (*bm).count;
            int evictPageNum = INT_MIN;

            for (int i = 0; i < (*bm).numPages; i++) {
                int *flag = strategyNum + i;
                *flag = *((*((*bm).mgmtData + i)).strategyRecords);
                if (*(getFixCounts(bm) + i) != 0) {
                    continue;
                }
                evictPageNum = least >= *(strategyNum + i)? i: evictPageNum;
                least = least >= *(strategyNum + i) ? *(strategyNum + i): least;
            }
            p = evictPageNum;

            if ((*((*bm).mgmtData + p)).dirty) { 
                forcePage(bm, (*bm).mgmtData + p); 
            }            
        }
    }
    struct Result res = {p, isExist};
    
    return res;
}

/*
   pinPage pins the page with page number pageNum.
   The buffer manager is responsible to set the pageNum field of the page handle
   passed to the method. Similarly, the data field should point to the page frame
   the page is stored in (the area in memory storing the content of the page).
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    struct Result res = check(-1, bm, page, pageNum);
    modify(bm, res.flag, page, pageNum, res.p);    
    printf("PinPage: Successful.\n");
    return RC_OK;
}


/*
    The getFrameContents function returns an array of PageNumbers
    where the ith element is the number of the page stored in the ith page frame.
*/
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    PageNumber *frameContents = (PageNumber*)malloc(sizeof(PageNumber) * ((*bm).numPages));
    for(int i = 0; i < (*bm).numPages; i++) {
        frameContents[i] = (*((*bm).mgmtData + i)).data != NULL ? (*((*bm).mgmtData + i)).pageNum : NO_PAGE;
    }
    printf("GetFrame: Successful.\n");
    return frameContents;
}


/*
    The getDirtyFlags function returns an array of bools (of size numPages)
    where the ith element is TRUE if the page stored in the ith page frame is dirty. 
*/
bool *getDirtyFlags (BM_BufferPool *const bm) {
    bool *dirtyFlags = malloc(sizeof(bool) * ((*bm).numPages));
    for(int i = 0; i < (*bm).numPages; i++) dirtyFlags[i] = (*((*bm).mgmtData + i)).dirty;
    printf("GetDirty: Successful.\n");
    return dirtyFlags;
}


/*
    The getFixCounts function returns an array of ints (of size numPages)
    where the ith element is the fix count of the page stored
    in the ith page frame. Return 0 for empty page frames.
*/
int *getFixCounts (BM_BufferPool *const bm) {
    int *fixCounts = malloc(sizeof(int) * ((*bm).numPages));
    for(int i = 0; i < (*bm).numPages; i++) fixCounts[i] = (*((*bm).mgmtData + i)).fixCounts;
    printf("GetFixCounts: Successful.\n");
    return fixCounts;
}

//The getNumReadIO function returns the number of pages that have been read from disk since a buffer pool has been initialized.
int getNumReadIO (BM_BufferPool *const bm) {
    printf("GetNumReadIO: Successful.\n");
    return (*bm).readNum;
}
//getNumWriteIO returns the number of pages written to the page file since the buffer pool has been initialized.
int getNumWriteIO (BM_BufferPool *const bm) {
    printf("GetNumWriteIO: Successful.\n");
    return (*bm).writeNum;
}
