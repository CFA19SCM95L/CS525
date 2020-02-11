#include "stdio.h"
#include "stdlib.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dt.h"
#include "dberror.h"




//Buffer Pool Management Functions
// RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData){
//     // if(bm == NULL) {
//     //     printf("The buffer pool is illegal.");
//     //     return RC_READ_NON_EXISTING_BUFFERPOOL;
//     // }
//     FILE *myFile;

//     //check the pageFileName is valid or not
//     myFile = fopen(pageFileName, "r");
//     if(myFile != NULL){
//         fclose(myFile);
//     }else{
//         RC_message = "Can not find the file.";
//         return RC_FILE_NOT_FOUND;
//     }

//     //initializing the buffer pool
//     bm->pageFile = (char *)pageFileName;
//     bm->numPages = numPages;
//     bm->strategy = strategy;
//     BM_PageHandle *buffer = (BM_PageHandle *)calloc(numPages, sizeof(BM_PageHandle));
//     bm->mgmtData = buffer;
//     int i;
//     for (i = 0; i < numPages; i++)
//     {
//         (bm->mgmtData + i)->data = NULL;
//         (bm->mgmtData + i)->dirty = 0;
//         (bm->mgmtData + i)->pageNum = -1;
//         (bm->mgmtData + i)->fixCounts = 0;
//     }
//     //Strategy Variable
//     bm->readNum = 0;
//     bm->writeNum = 0;
//     bm->count = 0;

//     printf("Initialized a new Buffer Pool.\n");
//     return RC_OK;
// }

/*
	initBufferPool creates a new buffer pool with numPages page frames using the page replacement strategy. 
	The pool is used to cache pages from the page file with name pageFileName. Initially, all page frames should be empty. 
	The page file should already exist, i.e., this method should not generate a new page file. 
	stratData can be used to pass parameters for the page replacement strategy. For example, for LRU-k this could be the parameter k.
*/
void init (BM_PageHandle *const page) {
    (*page).data = NULL;
    (*page).dirty = 0;
    (*page).pageNum = -1;
    (*page).fixCounts = 0;
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData){
    if(bm == NULL) return RC_BUFFER_ERROR;
    if(fopen(pageFileName, "r") == NULL) {
        RC_message = "Can not find the file.";
        return RC_FILE_NOT_FOUND;
    }
    //initializing the buffer pool
    (*bm).pageFile = (char *)pageFileName;
    (*bm).numPages = numPages;
    (*bm).strategy = strategy;
    (*bm).mgmtData = (BM_PageHandle *)calloc(numPages, sizeof(BM_PageHandle));
    for (int i = 0; i < numPages; i++) init((*bm).mgmtData + i);
    //Strategy Variable
    (*bm).readNum = 0;
    (*bm).writeNum = 0;
    (*bm).count = 0;
    printf("Initialized a new Buffer Pool.\n");
    return RC_OK;
}


//  RC shutdownBufferPool(BM_BufferPool *const bm){
//     //  if(bm == NULL)
//     //  {
//     //      printf("The buffer pool is illegal.");
//     //      return RC_READ_NON_EXISTING_BUFFERPOOL;
//     //  }

//      //check if there has any pinned page;
//      int *fixCounts;
//      fixCounts = getFixCounts(bm);
//      for(int i=0; i< bm->numPages; i++){
//          if(*(fixCounts + i)){ //pinned pages
//              free(fixCounts);
//             //  RC_message = "The Buffer Pool still has pinned page.";
//             //  return RC_SHUTDOWN_POOL_WITH_PINNED_PAGES;
//          }
//      }

//      RC result;
//      result = forceFlushPool(bm);
//      if (result != RC_OK) {
//          free(fixCounts);
//          return result;
//      }

//      for (int i = 0; i < bm->numPages; i++) {
//          free((bm->mgmtData + i)->data);
//          free((bm->mgmtData + i)->strategyRecords);
//      }
//      free(fixCounts);
//      free(bm->mgmtData);
//      printf("Shut down the Buffer Pool successfully.\n");
//      return RC_OK;
//  }

/*  shutdownBufferPool:destroys a buffer pool. This method should free up
 *  all resources associated with buffer pool. For example, it should free
 *  the memory allocated for page frames. If the buffer pool contains any
 *  dirty pages, then these pages should be written back to disk before
 *  destroying the pool. It is an error to shutdown a buffer pool that has
 *  pinned pages.
 */

RC shutdownBufferPool(BM_BufferPool *const bm){
    if(bm == NULL) return RC_BUFFER_ERROR;
    //check if there has any pinned page;
    int *fixCounts = getFixCounts(bm);
    for(int i=0; i< (*bm).numPages; i++){
        if(*(fixCounts + i)){ //pinned pages
            free(fixCounts);
            RC_message = "The Buffer Pool still has pinned page.";
            return RC_BUFFER_ERROR;
        }
    }
    forceFlushPool(bm);
    for (int i = 0; i < (*bm).numPages; i++) {
        free((*((*bm).mgmtData + i)).data);
        free((*((*bm).mgmtData + i)).strategyRecords);
    }
    free(fixCounts);
    free((*bm).mgmtData);
    printf("Shut down the Buffer Pool successfully.\n");
    return RC_OK;
 }


/*
 * forceFlushPool causes all dirty pages (with fixcounts 0)
 * from the buffer pool to be written to disk.
 */

// RC forceFlushPool(BM_BufferPool *const bm){
//     // if(bm == NULL)
//     // {
//     //     printf("The buffer pool is illegal.");
//     //     return RC_READ_NON_EXISTING_BUFFERPOOL;
//     // }

//     int *fixCounts;
//     bool *dirty;
//     BM_PageHandle *dirtyPages;

//     fixCounts = getFixCounts(bm);
//     dirty = getDirtyFlags(bm);

//     for(int i=0; i< bm->numPages; i++){
//         if(*(dirty + i)){
//             if(*(fixCounts + i)){  //fixcounts is not equal to 0;
//                 //someone still need them
//                 continue;
//             }
//             else{
//                 dirtyPages = ((bm->mgmtData)+i);
//                 RC result = forcePage(bm, dirtyPages);
//                 if (result != RC_OK) {
//                     free(dirty);
//                     free(fixCounts);
//                     return result;
//                 }
//             }
//         }
//     }
//     for (int i = 0; i < bm->numPages; ++i)
//     {
//         if (*(dirty + i))
//             ((bm->mgmtData) + i)->dirty = 0;
//     }
//     free(dirty);
//     free(fixCounts);
//     printf("Already Flushed the Buffer Pool.\n");
//     return RC_OK;
// }

/*
 * forceFlushPool causes all dirty pages (with fixcounts 0)
 * from the buffer pool to be written to disk.
 */

RC forceFlushPool(BM_BufferPool *const bm){
    if(bm == NULL) return RC_BUFFER_ERROR;
    int *fixCounts = getFixCounts(bm);
    bool *dirty = getDirtyFlags(bm);
    for(int i=0; i< (*bm).numPages; i++){      
        if(*(dirty + i) == false){
            continue;
        }
        if(*(fixCounts + i) == false) forcePage(bm, ((*bm).mgmtData)+i);
        (*(((*bm).mgmtData) + i)).dirty = 0;
    }
    free(dirty);
    free(fixCounts);
    printf("Already Flushed the Buffer Pool.\n");
    return RC_OK;
}

//we can use this method to fresh the page's strategy.
//RC freshStrategy(BM_BufferPool *bm, BM_PageHandle *pageHandle) {
//    if (pageHandle->strategyRecords == NULL) {
//        if ((*bm).strategy == RS_FIFO || (*bm).strategy == RS_LRU) {
//            pageHandle->strategyRecords = calloc(1, sizeof(int));
//        }
//    }
//
//    if ((*bm).strategy == RS_FIFO || (*bm).strategy == RS_LRU) {
//        int *sNum;
//        sNum = (int *)pageHandle->strategyRecords;
//        *sNum = ((*bm).count);
//        ((*bm).count)++;
//        return RC_OK;
//    }
//    return RC_BUFFER_ERROR;
//}

RC freshStrategy(BM_BufferPool *bm, BM_PageHandle *pageHandle) {
    if (pageHandle->strategyRecords == NULL)
        if ((*bm).strategy == RS_FIFO || (*bm).strategy == RS_LRU) pageHandle->strategyRecords = calloc(1, sizeof(int));

    if ((*bm).strategy == RS_FIFO || (*bm).strategy == RS_LRU) {
        int *sNum;
        sNum = (int *)pageHandle->strategyRecords;
        *sNum = ((*bm).count);
        ((*bm).count)++;
        return RC_OK;
    }
    return RC_BUFFER_ERROR;
}

int FIFOandLRU(BM_BufferPool *bm) {
    // if(bm == NULL)
    // {
    //     printf("The buffer pool is illegal.");
    //     return RC_READ_NON_EXISTING_BUFFERPOOL;
    // }

    int *fixCounts = getFixCounts(bm);
    int *strategyNum;
    int *flag;
    int least = (*bm).count;
    int evictPageNum = -1;

    strategyNum = (int *)calloc((*bm).numPages, sizeof(((*bm).mgmtData)->strategyRecords));
    for (int i = 0; i < (*bm).numPages; i++) {
        flag = strategyNum + i;
        *flag = *(((*bm).mgmtData + i)->strategyRecords);

        if (*(fixCounts + i) != 0){continue;}
        if (least >= (*(strategyNum + i))) {
            evictPageNum = i;
            least = (*(strategyNum + i));
        }
    }
    return evictPageNum;
}

//Page Management Functions
/*
 * pinPage pins the page with page number pageNum.
 * The buffer manager is responsible to set the pageNum field of the page handle
 *  passed to the method. Similarly, the data field should point to the page frame
 *  the page is stored in (the area in memory storing the content of the page).
 */

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    // if(bm == NULL || page == NULL || pageNum < 0)
    //    {
    //        return printf("The buffer pool is illegal.");
    //        return RC_READ_NON_EXISTING_BUFFERPOOL;;
    //    }

    int p = -1;
    bool isExist;
    int check;
    for (check = 0; check < (*bm).numPages; check++)
    {
        BM_PageHandle *curPage = ((*bm).mgmtData + check);
         //there has empty page in buffer pool
        if ((*curPage).pageNum <0)
        {
            (*curPage).data = (char*)calloc(PAGE_SIZE, sizeof(char));
            //initilialize the empty page
            p = check;
            isExist = FALSE;
            break;
        }
        //the page is already in the buffer pool
        if ((*curPage).pageNum == pageNum)
        {
            p = check;
            isExist = TRUE;
            if ((*bm).strategy == RS_LRU){ freshStrategy(bm, (*bm).mgmtData + p); }
            break;
        }
        //the buffer pool is already full
        if (check == (*bm).numPages - 1)
        {

            isExist = FALSE;
            //find which page should be evicted.
            if ((*bm).strategy == RS_FIFO || (*bm).strategy == RS_LRU)
            {
                p = FIFOandLRU(bm);
                if (((*bm).mgmtData + p)->dirty){ forcePage(bm, (*bm).mgmtData + p); }
            }
        }
    }

    BM_PageHandle *modPage = ((*bm).mgmtData + p);
    if (isExist)
    {
        page->data = modPage->data;
        page->pageNum = pageNum;
        (modPage->fixCounts)++;
        page->fixCounts = modPage->fixCounts;
        page->dirty = modPage->dirty;
        page->strategyRecords = modPage->strategyRecords;

    }
    if (!isExist)
    {
       FILE* file;
       file = fopen((*bm).pageFile, "r");
       fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
       fread(modPage->data, sizeof(char), PAGE_SIZE, file);
       page->data = modPage->data;
       modPage->pageNum = pageNum;
       page->pageNum = pageNum;
       (modPage->fixCounts)++;
       page->fixCounts = modPage->fixCounts;
       page->dirty = modPage->dirty;
       page->strategyRecords = modPage->strategyRecords;
       (*bm).readNum++;
       freshStrategy(bm, modPage);
       fclose(file);
    }
    printf("Page has been pinned.\n");
    return RC_OK;
}




////unpinPage unpins the page page.
////The pageNum field of page should be used to figure out which page to unpin.
//RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
//{
//    // if(bm == NULL || page == NULL){
//    //     printf("The buffer pool is illegal.");
//    //     return RC_READ_NON_EXISTING_BUFFERPOOL;
//    // }
//
//    for (int check = 0; check < (*bm).numPages; check++)
//    {
//        BM_PageHandle *curPage = ((*bm).mgmtData + check);
//        if ((*curPage).pageNum == page->pageNum)
//        {
//            (*curPage).fixCounts--;
//            break;
//        }
//    }
//    printf("The Page has been unpinned.\n");
//    return RC_OK;
//}

//unpinPage unpins the page page.
//The pageNum field of page should be used to figure out which page to unpin.
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    for(int i = 0; i < (*bm).numPages; i++) {
        if ((*((*bm).mgmtData + i)).pageNum == (*page).pageNum) {
            (*((*bm).mgmtData + i)).fixCounts--;
            break;
        }
    }
    printf("The Page has been unpinned.\n");
    return RC_OK;
}

////markDirty marks a page as dirty.
//RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
//{
//    // if(bm == NULL || page == NULL){
//    //     printf("The buffer pool is illegal.");
//    //     return RC_READ_NON_EXISTING_BUFFERPOOL;
//    // }
//
//    int check;
//    for (check = 0; check < ((*bm).numPages); check++)
//    {
//        BM_PageHandle *curPage = ((*bm).mgmtData + check);
//        if ((*curPage).pageNum == page->pageNum)
//        {
//            (*curPage).dirty = 1;
//            page->dirty = 1;
//            break;
//       }
//    }
//    printf("The dirty page has been marked.\n");
//    return RC_OK;
//}

//markDirty marks a page as dirty.
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    for(int i = 0; i < (*bm).numPages; i++) {
        if ((*((*bm).mgmtData + i)).pageNum == (*page).pageNum) {
            (*((*bm).mgmtData + i)).dirty = 1;
            (*page).dirty = 1;
            break;
        }
    }
    printf("The dirty page has been marked.\n");
    return RC_OK;
}

////forcePage should write the current content of the page back to the page file on disk.
//RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
//{
//    FILE *curFile = fopen((*bm).pageFile, "rb+");
//    int pageNum = page->pageNum;
//    fseek(curFile, pageNum*PAGE_SIZE, SEEK_SET);
//    fwrite(page->data, PAGE_SIZE, 1, curFile);
//    ((*bm).writeNum)++;
//    fclose(curFile);
//
//    int check;
//    for (check = 0; check < ((*bm).numPages); check++)
//    {
//        BM_PageHandle *curPage = ((*bm).mgmtData + check);
//        if ((*curPage).pageNum == pageNum)
//        {
//            (*curPage).dirty = 0;
//            break;
//        }
//    }
//    page->dirty = 0;
//    printf("Current content of the page written back to the disk successfully.\n");
//    return RC_OK;
//}

//forcePage should write the current content of the page back to the page file on disk.
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    FILE *curFile = fopen((*bm).pageFile, "rb+");
    int pageNum = (*page).pageNum;
    fseek(curFile, pageNum*PAGE_SIZE, SEEK_SET);
    fwrite((*page).data, PAGE_SIZE, 1, curFile);
    ((*bm).writeNum)++;
    fclose(curFile);

    for(int i = 0; i < (*bm).numPages; i++) {
        if ((*((*bm).mgmtData + i)).pageNum == pageNum) (*((*bm).mgmtData + i)).dirty = 0;
    }
    (*page).dirty = 0;
    printf("Current content of the page written back to the disk successfully.\n");
    return RC_OK;
}

///*The getFrameContents function returns an array of PageNumbers
//where the ith element is the number of the page stored in the ith page frame.*/
//PageNumber *getFrameContents (BM_BufferPool *const bm) {
//    BM_PageHandle *frame = (*bm).mgmtData;
//    PageNumber *frameContents = (PageNumber*)malloc(sizeof(PageNumber)
//                                                    * ((*bm).numPages));
//    int i = 0;
//    while(i < (*bm).numPages) {
//        if ((frame + i)->data != NULL) {
//            frameContents[i] = (frame + i)->pageNum;
//        } else {
//            frameContents[i] = NO_PAGE;
//        }
//        i++;
//    }
//    return frameContents;
//}

/*The getFrameContents function returns an array of PageNumbers
where the ith element is the number of the page stored in the ith page frame.*/
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    PageNumber *frameContents = (PageNumber*)malloc(sizeof(PageNumber) * ((*bm).numPages));
    for(int i = 0; i < (*bm).numPages; i++)
        if (((*bm).mgmtData + i)->data != NULL) frameContents[i] = (*((*bm).mgmtData + i)).pageNum;
        else frameContents[i] = NO_PAGE;
    return frameContents;
}

///*The getDirtyFlags function returns an array of bools (of size numPages)
//where the ith element is TRUE if the page stored in the ith page frame is dirty. */
//bool *getDirtyFlags (BM_BufferPool *const bm) {
//    BM_PageHandle *frame= (*bm).mgmtData;
//    bool *dirtyFlags = malloc(sizeof(bool) * ((*bm).numPages));
//    int i = 0;

//    while(i < (*bm).numPages) {
//        dirtyFlags[i] = (frame + i)->dirty;
//        i++;
//    }
//    return dirtyFlags;
//}

/*The getDirtyFlags function returns an array of bools (of size numPages)
where the ith element is TRUE if the page stored in the ith page frame is dirty. */
bool *getDirtyFlags (BM_BufferPool *const bm) {
    bool *dirtyFlags = malloc(sizeof(bool) * ((*bm).numPages));
    for(int i = 0; i < (*bm).numPages; i++) dirtyFlags[i] = (*((*bm).mgmtData + i)).dirty;
    return dirtyFlags;
}


///*The getFixCounts function returns an array of ints (of size numPages)
// *where the ith element is the fix count of the page stored
// *in the ith page frame. Return 0 for empty page frames.*/
//int *getFixCounts (BM_BufferPool *const bm) {
//    // if(bm == NULL){
//    //     printf("The buffer pool is illegal.");
//    //     return RC_READ_NON_EXISTING_BUFFERPOOL;
//    // }
//    BM_PageHandle *frame = (*bm).mgmtData;
//    int *fixCounts = malloc(sizeof(int) * ((*bm).numPages));
//
//    int i = 0;
//    while(i < (*bm).numPages) {
//        fixCounts[i] = (frame + i)->fixCounts;
//        i++;
//    }
//    return fixCounts;
//}

int *getFixCounts (BM_BufferPool *const bm) {
    int *fixCounts = malloc(sizeof(int) * ((*bm).numPages));
    for(int i = 0; i < (*bm).numPages; i++) fixCounts[i] = (*((*bm).mgmtData + i)).fixCounts;

    return fixCounts;
}

//The getNumReadIO function returns the number of pages that have been read from disk since a buffer pool has been initialized.
int getNumReadIO (BM_BufferPool *const bm) {
    return (*bm).readNum;
}
//getNumWriteIO returns the number of pages written to the page file since the buffer pool has been initialized.
int getNumWriteIO (BM_BufferPool *const bm) {
    return (*bm).writeNum;
}
