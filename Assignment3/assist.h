#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct TableMgmt_info{
    int sizeOfRec;
    int totalRecordInTable;
    int blkFctr;
    RID firstFreeLoc;
    RM_TableData *rm_tbl_data;
    BM_PageHandle pageHandle;
    BM_BufferPool bufferPool;
}TableMgmt_info;

typedef struct RM_SCAN_MGMT{
    RID recID;
    Expr *cond;
    int count;  // no of records scaned
    RM_TableData *rm_tbl_data;
    BM_PageHandle rm_pageHandle;
    BM_BufferPool rm_bufferPool;
}RM_SCAN_MGMT;

RC RC_PIN_PAGE_FAILED = 111;
RC RC_UNPIN_PAGE_FAILED = 112;
RC RC_MARK_DIRTY_FAILED = 113;
RC RC_BUFFER_SHUTDOWN_FAILED = 114;
RC RC_NULL_IP_PARAM = 115;
RC RC_IVALID_PAGE_SLOT_NUM = 116;
RC RC_FILE_DESTROY_FAILED = 117;
RC RC_SCHEMA_NOT_INIT = 118;
RC RC_MELLOC_MEM_ALLOC_FAILED = 119;
RC RC_ERROR = -1;

void schemaReadFromFile();
char * readSchemaName();
char * readAttributeMeataData();
int readTotalKeyAttr();
char * readAttributeKeyData();
char * getSingleAtrData();
char ** getAtrributesNames();
char * extractName();
int extractDataType();
int * getAtributesDtType();
int extractTypeLength();
int * getAtributeSize();
int * extractKeyDt();
int * extractFirstFreePageSlot();
char * readFreePageSlotData();
int getAtrOffsetInRec();
void strRepInt();
int readTotalAttributes();
int extractTotalRecordsTab();