#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "record_mgr.h"
#include "tables.h"
#include "expr.h"

RC RC_ILLEGAL_PARAMETER = 100;
RC RC_RM_RECORD_NOT_EXIST = 101;

/*******************************************table and manager****************************************************/
RC initRecordManager (void *mgmtData){
    printf("Initial Record Manager Successfully.\n");
        return RC_OK;
}

RC shutdownRecordManager (){
    printf("Shutdown Record Manager Successfully.\n");
    return RC_OK;
}

extern RC createTable (char *name, Schema *schema){
    if(name == NULL || schema == NULL){
        printf("Faild to Create Table.");
        return RC_ILLEGAL_PARAMETER;
    }
    
    SM_FileHandle fh;
    SM_PageHandle ph;

    createPageFile(name);
    openPageFile(name, &fh);
    
    int metadataSize = strlen(serializeSchema(schema))
                        + 4 * sizeof(int);// get file's metadata size
    
    if (metadataSize % PAGE_SIZE != 0) { //file do not need extra page
        metadataSize = metadataSize / PAGE_SIZE + 1;
    }
    else {
        metadataSize = metadataSize / PAGE_SIZE;
    }

    int slotSize = 512;
    int recordSize = (getRecordSize(schema) / (slotSize));
    int recordNum = 0;
    char *c = (char *)calloc(PAGE_SIZE, sizeof(char));
    //memcpy() is used to copy a block of memory from a location to another.
    memcpy(c, &metadataSize, sizeof(int));
    memcpy(c + sizeof(int), &recordSize, sizeof(int));
    memcpy(c + 2 * sizeof(int), &slotSize, sizeof(int));
    memcpy(c + 3 * sizeof(int), &recordNum, sizeof(int));

    char *charSchema = serializeSchema(schema);
    if (ensureCapacity(metadataSize, &fh) != RC_OK) {
        return closePageFile(&fh);
    }

    if (strlen(charSchema) < PAGE_SIZE - 4 * sizeof(int)) {
        memcpy(c + 4 * sizeof(int), charSchema, strlen(charSchema));
        if (writeBlock(0, &fh, c) != RC_OK) {
            return closePageFile(&fh);
        }
        free(c);
    } else {
        memcpy(c + 4 * sizeof(int), charSchema, PAGE_SIZE - 4 * sizeof(int));
        if (writeBlock(0, &fh, c) != RC_OK) {
            return closePageFile(&fh);
        }
        free(c);
        for (int i = 1; i < metadataSize; i++) {
            c = (char *)calloc(PAGE_SIZE, sizeof(char));
            if (i == metadataSize - 1) {
                memcpy(c, charSchema + i * PAGE_SIZE, strlen(charSchema + i * PAGE_SIZE));
            } else {
                memcpy(c, charSchema + i * PAGE_SIZE, PAGE_SIZE);
            }
            if (writeBlock(0, &fh, c) != RC_OK) {
                return closePageFile(&fh);
            }
            free(c);
        }
    }

    if(addMetaDataBlock(&fh) != RC_OK) {
        return closePageFile(&fh);
    }
    return closePageFile(&fh);
}
//typedef struct RM_TableData
//{
//    char *name;

//    Schema *schema;
//    BM_BufferPool *bm; //
//    SM_FileHandle *fh; //
//    void *mgmtData;
//} RM_TableData;
RC openTable (RM_TableData *rel, char *name){
    if(name == NULL){
        return RC_ILLEGAL_PARAMETER;
    }
    //#define MAKE_POOL()                    \
    ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))
    //from buffer_mgr.h
    BM_BufferPool *bp = MAKE_POOL();
    //#define MAKE_PAGE_HANDLE()                \
    ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))
    // from buffer_mgr.h
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    SM_FileHandle *fh = (SM_FileHandle*)calloc(1, sizeof(SM_FileHandle));
    int i;
    int j;
    int metadataSize;

    openPageFile(name, fh); // open the name file
    initBufferPool(bp, name, 10, RS_LRU, NULL); //Initial the buffer pool
    metadataSize = getFileMetaDataSize(bp);// get the files meta data size
    for (i = 0; i < metadataSize; i++) {
        pinPage(bp, ph, i);
        unpinPage(bp, ph);
    }
    char * newSchema;
    newSchema = ph->data + 4 * sizeof(int);

    char * check;
    char * checkSign;
    char * str;
    //char *strchr(const char *str, int c) searches for the first occurrence
    //of the character c (an unsigned char) in the string pointed to
    //by the argument str.
    check = strchr(newSchema, '<');
    check++;
    str = (char *)calloc(40, sizeof(char));
    for (i = 0; 1; i++) {
        if (*(check + i) == '>') {
            break;
        }
        str[i] = check[i];
    }
    //int atoi(const char *str) converts the string   \
    argument str to an integer (type int)
    int strInt = atoi(str);
    free(str);
    
    check = strchr(check, '(');
    check++;
    // figure out the Data Type, Length of Data Type and attrNames \
       in typedef struct Schema
    DataType * dataType;
    int *dataTypeLen, *keyAttrs, keySize;
    char **attrNames;
    
    attrNames = (char **)calloc(strInt, sizeof(char *));
    dataType = (DataType *)calloc(strInt, sizeof(DataType));
    dataTypeLen = (int *)calloc(strInt, sizeof(int));

    for (i = 0; i < strInt; i++) {
        for (j = 0; 1; j++) {
            if (*(check + j) == ':') {
                attrNames[i] = (char *)calloc(j, sizeof(char));
                memcpy(attrNames[i], check, j);
                char c = *(check + j + 2);
                switch (c) {
                    case 'S':
                        dataType[i] = DT_STRING;

                        str = (char *)calloc(40, sizeof(char));
                        for (int a = 0; 1; a++) {
                            if (*(check+j+a+9) == ']') {
                                break;
                            }
                            str[a] = check[j+a+9];
                        }
                        dataTypeLen[i] = atoi(str);
                        free(str);
                        break;
                    case 'I':
                        dataType[i] = DT_INT;
                        dataTypeLen[i] = 0;
                    case 'B':
                        dataType[i] = DT_BOOL;
                        dataTypeLen[i] = 0;
                    case 'F':
                        dataType[i] = DT_FLOAT;
                        dataTypeLen[i] = 0;
                    default:
                        return RC_RM_UNKOWN_DATATYPE;
                }
                if (i == strInt - 1) {
                    break;
                }
                check = strchr(check, ',') + 2;
                break;
            }
        }
    }
    check = strchr(check, '(') + 1;
    checkSign = check;
    for (i = 0; true; i++) {
        checkSign = strchr(checkSign, ',');
        if (checkSign == NULL){
            break;
        }
        checkSign++;
    }
    //Assign the keySize and keyAttrs in typedef struct Schema
    keySize = i + 1;
    keyAttrs = (int *)calloc(keySize, sizeof(int));
    for (i = 0; i < keySize; i++) {
        for (j = 0; true; j++) {
            if ((*(check + j) == ',') || (*(check + j) == ')')) {
                str = (char *)calloc(100, sizeof(char));
                memcpy(str, check, j);
                for (int a = 0; a < strInt; a++) {
                    if (strcmp(str, attrNames[a]) == 0) {
                        keyAttrs[i] = a;
                        free(str);
                        break;
                    }
                }
                if (*(check + j) == ',') {
                    check += (j + 2);
                }
                else {
                    check += j;
                }
                break;
            }
        }
    }
    //Create schema and assign to the rel
    Schema *schema;
    schema = createSchema(strInt, attrNames, dataType, dataTypeLen, keySize, keyAttrs);
    rel->name = name;
    rel->schema = schema;
    rel->bm = bp;
    rel->fh = fh;
    
    printf("Opened Table Successfully.");
    return RC_OK;
}

RC closeTable (RM_TableData *rel){
    if(rel == NULL){
        printf("Failed to Close Table.");
        return RC_ILLEGAL_PARAMETER;
    }
    freeSchema(rel->schema);
    shutdownBufferPool(rel->bm);
    free(rel->bm);
    free(rel->fh);
    printf("Closed Table Successfully.");
    return RC_OK;
}

RC deleteTable (char *name){
    if(name == NULL){
        printf("Failed to Delete Table.");
        return RC_ILLEGAL_PARAMETER;
    }
    return destroyPageFile(name);
}

int getNumTuples (RM_TableData *rel){
    if(rel == NULL){
        printf("Failed to get the number of tuples.");
        return RC_ILLEGAL_PARAMETER;
    }
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    int numTuples;
    pinPage(rel->bm, ph, 0);
    memcpy(&numTuples, ph->data + 3 * sizeof(int), sizeof(int));
    unpinPage(rel->bm, ph);
    free(ph);
    printf("Got the number of tuples Successfully.");
    return numTuples;
}

int getFileMetaDataSize(BM_BufferPool *bm) {
    if(bm == NULL){
        printf("Failed at getFileMetaDataSize()");
        return RC_ILLEGAL_PARAMETER;
    }
    int metadataSize;
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    pinPage(bm, h, 0);
    memcpy(&metadataSize, h->data, sizeof(int));
    unpinPage(bm, h);
    free(h);
    return metadataSize;
}

RC addMetaDataBlock(SM_FileHandle *fh) {
    if(fh == NULL){
        printf("Failed at addMetaDataBlock()");
        return RC_ILLEGAL_PARAMETER;
    }
    appendEmptyBlock(fh);
    int metadataNum = PAGE_SIZE / (sizeof(int) * 2);
    char * pageMetadataInput = (char *)calloc(PAGE_SIZE, sizeof(char));
    int pageNum = fh->totalNumPages;
    int capacity = -1;

    for (int i = 0; i < metadataNum; i++){
        memcpy(pageMetadataInput + i *sizeof(int) * 2, &pageNum, sizeof(int));
        memcpy(pageMetadataInput + i * sizeof(int) * 2 + sizeof(int),
               &capacity, sizeof(int));
        pageNum++;
        if (i == metadataNum - 1) {
            pageNum = fh->totalNumPages - 1;
        }
    }
    writeBlock(fh->totalNumPages - 1, fh, pageMetadataInput);
    free(pageMetadataInput);
    return RC_OK;
}

/*******************************************handling records in a table****************************************************/
RC insertRecord (RM_TableData *rel, Record *record)
{
    if(rel == NULL || record == NULL){
        printf("Failed to Insert Record");
        return RC_ILLEGAL_PARAMETER;
    }
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    int recsize = getRecordSize(rel->schema);  // GETTING THE RECORD SIZE
    int p_meta = getFileMetaDataSize(rel->bm); //GETTING METADATA STORED IN THE FILE
    int slotnum = (recsize + sizeof(bool)) / 512 + 1;

    // Find out the target page and slot at the end to insert the new record
    while(true)
    {
        pinPage(rel->bm, ph, p_meta); // It will pin the page with page number p_meta
        memcpy(&p_meta, ph->data + PAGE_SIZE - sizeof(int), sizeof(int)); //COPIES THE COUNT BYTES (total number = sizeof(int) FROM THE SOURCE POINTED TO BY data to p_data.
        if(p_meta != -1) // If count bytes are not -1, we unpin the page.
        {
            unpinPage(rel->bm, ph); //call unpinPage from Buffer Manager and unpin the page.
        }
        else
        {
            break;
        }
    }

    int offset = 0, cur_num, tuples;
    bool stat = true;
    
    while (cur_num == PAGE_SIZE / 512)
    {
        memcpy(&cur_num, ph->data + sizeof(int) + offset, sizeof(int));
        //getting the current position
        offset += sizeof(int) * 2; //updating the offset
    }
    
    if(cur_num == -1)//We need to add new page
        {
        if(offset == PAGE_SIZE)
        {
            memcpy(ph->data + PAGE_SIZE - sizeof(int), &rel->fh->totalNumPages,
                   sizeof(int));
            addMetaDataBlock(rel->fh);
            markDirty(rel->bm, ph);
            unpinPage(rel->bm, ph);
            pinPage(rel->bm, ph, rel->fh->totalNumPages-1);
            offset = sizeof(int) * 2;
        }
        // set page number
        memcpy(ph->data + offset - sizeof(int) * 2, &rel->fh->totalNumPages,
               sizeof(int));
        appendEmptyBlock(rel->fh);
        cur_num = 0;
    }

    // Set record->id page number.
    memcpy(&record->id.page, ph->data + offset - sizeof(int) * 2, sizeof(int));
    // Set record->id slot.
    record->id.slot = cur_num * slotnum;
    cur_num++;
    memcpy(ph->data + offset - sizeof(int), &cur_num, sizeof(int));
    markDirty(rel->bm, ph);
    unpinPage(rel->bm, ph);

    // Insert header and data into page.
    pinPage(rel->bm, ph, record->id.page);
    memcpy(ph->data + 512*record->id.slot, &stat, sizeof(bool));
    memcpy(ph->data + 512*record->id.slot + sizeof(bool), record->data,
           recsize);
    markDirty(rel->bm, ph);
    unpinPage(rel->bm, ph);
    
    // Tuple number add 1.
    pinPage(rel->bm, ph, 0);
    memcpy(&tuples, ph->data + 3 * sizeof(int), sizeof(int));
    tuples++;
    memcpy(ph->data + 3 * sizeof(int), &tuples, sizeof(int));
    markDirty(rel->bm, ph);
    unpinPage(rel->bm, ph);
    free(ph);
    printf("Insert Record Successfully.");
    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id)
{
    if(rel == NULL){
        printf("Failed to Delete Record");
        return RC_ILLEGAL_PARAMETER;
    }
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    int recsize = getRecordSize(rel->schema); //obtaining the record size
    
    pinPage(rel->bm, ph, id.page); //pin the page with page number id.page
    char* recDeleted = (char *)calloc(sizeof(bool) + recsize,
                                      sizeof(char));
    memcpy(ph->data + 512*id.slot, recDeleted,
           sizeof(bool) + recsize);
    markDirty(rel->bm, ph);
    unpinPage(rel->bm, ph);
    
    int tuples;
    pinPage(rel->bm, ph, 0);
    memcpy(&tuples, ph->data + 3 * sizeof(int), sizeof(int));
    tuples--;
    
    memcpy(ph->data + 3 * sizeof(int), &tuples, sizeof(int));
    markDirty(rel->bm, ph);
    unpinPage(rel->bm, ph);
    free(ph);
    free(recDeleted);
    
    printf("Deleted Record Successfully.");
    return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {
    if(rel == NULL || record == NULL){
        printf("Failed to Update Record");
        return RC_ILLEGAL_PARAMETER;
    }
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    int recsize = getRecordSize(rel->schema);
    
    pinPage(rel->bm, ph, record->id.page);
    memcpy(ph->data + 512*record->id.slot + sizeof(bool),
           record->data, recsize);
    markDirty(rel->bm, ph);
    unpinPage(rel->bm, ph);
    free(ph);
    
    printf("Updated Record Successfully.");
    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record) {
    if(rel == NULL || record == NULL){
        printf("Failed to Get Record");
        return RC_ILLEGAL_PARAMETER;
    }
    
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    record->id = id;
    pinPage(rel->bm, ph, id.page);
    
    bool stat;
    int recsize = getRecordSize(rel->schema);
    memcpy(&stat, ph->data + 512*id.slot, sizeof(bool));
    if(stat == true){
        record->data = (char*) malloc(recsize);
        memcpy(record->data, ph->data + 512*id.slot + sizeof(bool), recsize);
        unpinPage(rel->bm, ph);
        free(ph);
        
        printf("Get Record Successfully.");
        return RC_OK;
    } else {
        free(ph);
        return RC_RM_RECORD_NOT_EXIST;
    }
}

/***********************************************************scans**************************************************************************/
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if(rel == NULL || scan == NULL || cond == NULL){
        printf("Failed to Start Scan");
        return RC_ILLEGAL_PARAMETER;
    }
    scan->rel = rel;
    scan->currentPage = 0;
    scan->currentSlot = 0;
    scan->expr = cond;
    printf("Started Scan Successfully.");
    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record)
{
    if(scan == NULL || record == NULL){
        printf("Failed to Update Record");
        return RC_ILLEGAL_PARAMETER;
    }
    
    int ind, r_slot;
    BM_BufferPool *tmp_bp = scan->rel->bm;
    BM_PageHandle *ph = (BM_PageHandle*)calloc(1,sizeof(BM_PageHandle));
    

    r_slot = (getRecordSize (scan->rel->schema)+sizeof(bool))/512 + 1;
    ind = getFileMetaDataSize(tmp_bp);
    pinPage(tmp_bp, ph, ind);

    RID rid;
    Value *result = (Value *)calloc(1,sizeof(Value));
    Record *tmp_Record = (Record *)calloc(1,sizeof(Record));
    int mslot, r_page, i;
    do
    {
        memcpy(&r_page,ph->data+(scan->currentPage) * sizeof(int) * 2, sizeof(int));
        memcpy(&mslot, ph->data + ((scan->currentPage) *2 + 1) * sizeof(int), sizeof(int));
        if(mslot!=-1)
        {
            i=scan->currentSlot;
            while(i<mslot)
            {
                rid.page = r_page;
                rid.slot = i * r_slot;
                if(getRecord(scan->rel,rid,tmp_Record) == RC_OK)
                {
                    evalExpr (tmp_Record, scan->rel->schema, scan->expr,&result);
                    if(result->v.boolV)
                    {
                        record->id.slot=rid.slot;
                        record->id.page=rid.page;
                        record->data=tmp_Record->data;
                        
                        if(i != mslot - 1)
                        {
                           scan->currentSlot = i + 1;
                        }
                        else if(i == mslot - 1)
                        {
                            scan->currentSlot = 0;
                            scan->currentPage++;
                        }
                        unpinPage (tmp_bp, ph);
                        free(ph);
                        free(result);
                        free(tmp_Record);

                        return RC_OK;
                    }
                }
                i++;
            }
        }
        else
        {
            unpinPage(tmp_bp,ph);
            free(ph);
            return RC_RM_NO_MORE_TUPLES;
        }
        scan->currentPage++;
    }while(scan->currentPage!=ind);
    
    unpinPage (tmp_bp, ph);
    free(ph);
    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan (RM_ScanHandle *scan)
{
    if(scan == NULL){
        printf("Failed to Close Scan");
        return RC_ILLEGAL_PARAMETER;
    }
    printf("Close Scan Successfully.");
    return RC_OK;
}

/*******************************************dealing with schemas****************************************************/
int getRecordSize (Schema *schema)
{
    if(schema == NULL){
        printf("Failed to Get the Record Size.");
        return RC_ILLEGAL_PARAMETER;
    }
    
    int size = 0;
    for (int i=0; i < (*schema).numAttr; i++)
    {
        if ((*schema).dataTypes[i] == DT_INT)
        {
            size = size + sizeof(int);
        }
        else if ((*schema).dataTypes[i] == DT_FLOAT)
        {
            size = size + sizeof(float);
        }
        else if ((*schema).dataTypes[i] == DT_BOOL)
        {
            size = size + sizeof(bool);
        }
        else if ((*schema).dataTypes[i] == DT_STRING)
        {
            size = size + (*schema).typeLength[i];
        }
    }
    return size;
}

Schema *createSchema (int numAttr, char **attrNames,
                      DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *new_schema = (Schema*)malloc(sizeof(Schema));
    (*new_schema).numAttr = numAttr;
    (*new_schema).attrNames = attrNames;
    (*new_schema).dataTypes = dataTypes;
    (*new_schema).typeLength = typeLength;
    (*new_schema).keySize = keySize;
    (*new_schema).keyAttrs = keys;
    return new_schema;
}

RC freeSchema (Schema *schema)
{
    if(schema == NULL){
        printf("Failed to Get the Record Size.");
        return RC_ILLEGAL_PARAMETER;
    }
    free((*schema).keyAttrs);
    free((*schema).typeLength);
    free((*schema).dataTypes);
    
    for (int i=0; i< (*schema).numAttr; i++)
    {
        free((*schema).attrNames[i]);
    }
    free((*schema).attrNames);
    free(schema);
    
    printf("Free Schema Successfully.");
    return RC_OK;
}

/***************************************dealing with records and attribute values**********************************************/
RC createRecord (Record **record, Schema *schema)
{
    if(record == NULL || schema == NULL){
        printf("Failed to Create Record.");
        return RC_ILLEGAL_PARAMETER;
    }
    
    *record = (struct Record *)calloc(1, sizeof(struct Record));
    (*record)->data = (char*)calloc(getRecordSize(schema), sizeof(char));
    
    printf("Create Record Successfully.");
    return RC_OK;
}

RC freeRecord (Record *record)
{
    if(record == NULL){
        printf("Failed to Free Record.");
        return RC_ILLEGAL_PARAMETER;
    }
    free((*record).data);
    free(record);
    
    printf("Free Record Successfully.");
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    if(schema == NULL || attrNum < 0){
        printf("Failed at getAttr()");
        return RC_ILLEGAL_PARAMETER;
    }

    int ofs = 0;
    for (int i=0; i < attrNum; i++)
    {
        if ((*schema).dataTypes[i] == DT_INT)
        {
            ofs = ofs + sizeof(int);
        }
        else if ((*schema).dataTypes[i] == DT_FLOAT)
        {
            ofs = ofs + sizeof(float);
        }
        else if ((*schema).dataTypes[i] == DT_BOOL)
        {
            ofs = ofs + sizeof(bool);
        }
        else if ((*schema).dataTypes[i] == DT_STRING)
        {
            ofs = ofs + (*schema).typeLength[i];
        }
    }
    
    // Get value from record.
    char end = '\0';
    *value = (Value *)malloc(sizeof(Value));
    (*value)->dt = (*schema).dataTypes[attrNum];
    if ((*schema).dataTypes[attrNum] == DT_INT)
    {
         memcpy(&((*value)->v.intV), (*record).data + ofs, sizeof(int));
    }
    else if ((*schema).dataTypes[attrNum] == DT_FLOAT)
    {
        memcpy(&((*value)->v.intV), (*record).data + ofs, sizeof(float));
    }
    else if ((*schema).dataTypes[attrNum] == DT_BOOL)
    {
        memcpy(&((*value)->v.intV), (*record).data + ofs, sizeof(bool));
    }
    else if ((*schema).dataTypes[attrNum] == DT_STRING)
    {
        (*value)->v.stringV = (char*)malloc((*schema).typeLength[attrNum] + 1);
        memcpy((*value)->v.stringV, (*record).data + ofs,
               (*schema).typeLength[attrNum]);
        memcpy((*value)->v.stringV + (*schema).typeLength[attrNum], &end, 1);
    }
    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    if(record == NULL || schema == NULL || attrNum < 0){
        printf("Failed at getAttr()");
        return RC_ILLEGAL_PARAMETER;
    }
    
    int ofs=0;
    for (int i=0; i < attrNum; i++)
    {
        if ((*schema).dataTypes[i] == DT_INT)
        {
            ofs = ofs + sizeof(int);
        }
        else if ((*schema).dataTypes[i] == DT_FLOAT)
        {
            ofs = ofs + sizeof(float);
        }
        else if ((*schema).dataTypes[i] == DT_BOOL)
        {
            ofs = ofs + sizeof(bool);
        }
        else if ((*schema).dataTypes[i] == DT_STRING)
        {
            ofs = ofs + (*schema).typeLength[i];
        }
    }

    if ((*schema).dataTypes[attrNum] == DT_INT)
    {
        memcpy((*record).data + ofs, &((*value).v.intV), sizeof(int));
    }
    else if ((*schema).dataTypes[attrNum] == DT_FLOAT)
    {
        memcpy((*record).data + ofs, &((*value).v.intV), sizeof(float));
    }
    else if ((*schema).dataTypes[attrNum] == DT_BOOL)
    {
        memcpy((*record).data + ofs, &((*value).v.intV), sizeof(bool));
    }
    else if ((*schema).dataTypes[attrNum] == DT_STRING)
    {
        if (strlen((*value).v.stringV) >= (*schema).typeLength[attrNum])
        {
            memcpy((*record).data + ofs, (*value).v.stringV, (*schema).typeLength[attrNum]);
        }
        else
        {
            strcpy((*record).data + ofs, (*value).v.stringV);
        }
    }
    return RC_OK;
}

