Project Name : Storage Manager
-Chenen Lee(A20435685()
-Jingeun Jung(A20437470)
-Zhongqiu Peng(


Run:
1. Open terminal at project root

2. Type make launch to create a bin file: test_assign1 

3. Type ./test_assign1

4. Type make clean to clean




//////////////////////////part1/////////////////
Functions:
1. initStorageManager() : Initiates the storage manager



Manipulating page files:


2. createPageFile (char *fileName)
		overwrite the file is existed , or if the file does not exist, the file stream is opened in write mode
        and a new page is allocated.


3. openPageFile (char *fileName, SM_FileHandle *fHandle)
        File is opened in read mode. The fields of the file handle are updated with the nformation about the opened file; fields like fileSize and numOfPages. RC_OK is returned.
        If the file does not exist, the file is closed and RC_FILE_NOT-FOUND is returned.


4. closePageFile (SM_FileHandle *fHandle)
        First, it is checked whether the file handle is void or not. 
        If it is, error RC_FILE_NOT_FOUND is returned.
        If it is not void, file is closed using fclose();


5. destroyPageFile (char *fileName)
        First, it is checked whether fileName is void or not.
        If it is, then error RC_FILE_NOT_FOUND is returned.
        If it is not, then remove() is implemented, which returns value when succeeded.
        If remove() is not successful, RC_FILE_NOT_FOUND is returned.
        If remove() is successful, then the page is deleted, RC_OK is returned.


/////////////////////////part2/////////////////
Reading blocks from disk:


6. readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
        First, we check the validity of the page number, which should be greater than 0 and less than total no. of pages.
        If any of the above cases is true, page number is invalid and RC_READ_NON_EXISTING_PAGE is returned
        Also, if fHandle, myFile or memPage are void (do not exist) then RC_FILE_NOT_FOUND is returned.
        If the page number is valid, then the block can be read.
        The position is found, pointer is set to the position and the block is read by fread(), current page position is updated after the read. RC_OK is returned.


7. getBlockPos (SM_FileHandle *fHandle)
        First, we check whether the file handle or the file itself is void or not.
        If either is void, RC_FILE_NOT_FOUND is returned.
        If they are not void, the current page position is returned.


8. readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
        Implements readBlock() with pagenum=0, and the specified file and page handle.


9. readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
        The position of the block to be read is one less than the current position.
            The function readBlock() is implemented from the new block position (newpos), and the specified file and page handle.


10. readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
        The position of the current position is set to curPagePos.
           The function readBlock() is implemented from the new block position (curpos), and the specified file and page handle.


11. readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
        The position of the new block is one more than the current position.
            The function readBlock() is implemented from the new block position (nextpos), and the specified file and page handle.


12. readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
        The position of the last block is set to the value of total pages.
            The function readBlock() is implemented from the new block position (lastpos), and the specified file and page handle.


//////////////part3///////////////////
Writing blocks:


13. writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
        First, we check the validity of the parameter pageNum. the page number should be greater than 0, and less than the total no. of pages.
        If either of the above conditions are true, RC_READ_NON_EXISTING_PAGE is returned.
        The file is opened in read and write mode, since both operations are to be performed.
        Then, it is checked whether the fHAndle, myFile(the file itself), or memPage is null.
        If any of the above are null, RC_FILE_NOT_FOUND is returned.
        If everything is valid, the block can be written.
        Pointer is moved to the position where the writing is to be started. The block is wrtten using fwrite() and RC_OK is returned.


14. writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
        The functions writes a page to disk using the current position
        The writeBlock() function is called, starting from the current position with the specified file and page handle


15. appendEmptyBlock (SM_FileHandle *fHandle)
        The function will add an empty block to the end of the block. Increases the number of pages in the file by one.
        The new last page should be filled with zero bytes.
        Checking whether the fHandle and the file itself is void or not in which case an empty block will not be appended. RC_FILE_NOT_FOUND is returned.
        A new page is created.
        Pointer is moved to the end if the file.
        The empty block is written.
        Total no. of pages and current position is updated.
        Page is deallocated using free(); 


16. ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
        First, we check whether the fHandle and the file itself is void or not.
        If it void, RC_FILE_NOT_FOUND is returned.
        If totalNumPages < numberOfPages, appendEmptyBlock() is called. RC_OK is returned.
