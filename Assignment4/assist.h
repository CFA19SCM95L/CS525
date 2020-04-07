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


RC RC_ERROR = -1;
// RC RC_ILLEGAL_PARAMETER = -1;
// RC RC_IM_NODE_NOT_EXIST = -1;
// RC RC_IM_TREE_NOT_EXIST = -1;

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