/***
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    // Add your code below. Please do not remove this line.

	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	// indexName is the name of the index file
	std::string indexName = idxStr.str();

	outIndexName = indexName; // return name of index file

	// set private variables to the correct values
	bufMgr = bufMgrIn; // set private BufMgr instance
	leafOccupancy = INTARRAYLEAFSIZE;
	nodeOccupancy = INTARRAYNONLEAFSIZE;
	attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	scanExecuting = false;

	bool fileExisted = false;

	try {
		file = new BlobFile(outIndexName, true);
	} catch(FileExistsException &e) {
		file = new BlobFile(outIndexName, false);
		fileExisted = true;
	}

	//if(fileExisted) {
	//	*file->open(outIndexName); // open file
	//}

	if(!fileExisted) {
		// TODO filescan stuff
	} else { // file exists
		headerPageNum = file->getFirstPageNo();
		Page *page = new Page();
		bufMgr->readPage(file, headerPageNum, page);
		IndexMetaInfo* idxMeta = new IndexMetaInfo();
		idxMeta = (IndexMetaInfo*)page;
		rootPageNum = idxMeta->rootPageNo;

		if(strcmp(idxMeta->relationName, relationName.c_str()) != 0) {
			throw BadIndexInfoException(outIndexName);
		}
		if(idxMeta->attrByteOffset != attrByteOffset) {
			throw BadIndexInfoException(outIndexName);
		}
		if(idxMeta->attrType != attrType) {
			throw BadIndexInfoException(outIndexName);
		}
	}

	

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    // Add your code below. Please do not remove this line.
	

	// Integer range values for scan
	lowValInt = *(int *)lowValParm;
	highValInt = *(int *)highValParm;

	// Double range values for scan
	lowValDouble = *(double *)lowValParm;
	highValDouble = *(double *)highValParm;

	//lowValString = *(std::string *)lowValParm;
	//highValString = *(std::string *)highValParm;
	
	
	if(lowValInt > highValInt) {
		throw BadScanrangeException();
	}

	if(lowOpParm != GT || lowOpParm != GTE) {
		throw BadOpcodesException();
	}

	if(highOpParm != LT || highOpParm != LTE) {
		throw BadOpcodesException();
	}
	
	// check if scan is executing, and if it is, end it
	if(scanExecuting) {
		endScan();
	}

	// set private Operator variables
	lowOp = lowOpParm;
	highOp = highOpParm;

	currentPageNum = rootPageNum;

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.

	if(!scanExecuting) {
		throw ScanNotInitializedException();
	}

	scanExecuting = false;

	// unpin any pinned pages
	// TODO

	

}

}
