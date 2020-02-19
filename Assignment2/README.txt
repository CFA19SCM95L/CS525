Project Name : BUFFER Manager

-Chenen Lee(A20435685)
-Jingeun Jung(A20437470)
-Zhongqiu Peng(A20427765)


Run:
1. Open terminal at project root

2. Type make to create a bin file: test1

3. Type ./test1

4. Type make clean to clean




Functions:
Buffer Manager Interface Pool Handling:


1. initBufferPool: 
	Initiate the buffer pool manager. Contain function initPage().
	


2. shutdownBufferPool:
    Close the buffer. Write all the dirty pages back to disk.


3. forceFlushPool:
	Write the dirty pages in the buffer pool back to disk.

      
Buffer Manager Interface Access Pages:



4. markDirty:
	Find the target page and mark it dirty.
	

5. unpinPage:
	Find the target page and unpinned it, and decrease the fixCounts.
	


6. forcePage:
	Write the content of page to the disk.

7. pinPage:
	If there is empty page in buffer pool, initialize the the empty page. 
	If the buffer pool is full, use FIFOandLRU to find which page should be kicked.
	Contain function: check(), modify(), freshStrategy()


Statistics Interface:

8. getFrameContents:
	Get the contents in the frame. Return an array of page numbers.

9. getDirtyFlags:
	Get the info that which pages are marked dirty. Returns an array of boolean data.

10. getFixCounts:
	Get the fix counts.

11. getNumReadIO:
	Get the number of pages read from the disk.

12. getNumWriteIO:
	Write the number of pages written to the disk.