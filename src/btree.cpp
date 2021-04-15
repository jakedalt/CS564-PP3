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
	idxStr << relationName << ’.’ << attrByteOffset;
	// indexName is the name of the index file
	std::string indexName = idxStr.str();

	outIndexName = indexName; // return name of index file
	bufMgr = bufMgrIn; // set private BufMgr instance

	attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	scanExecuting = false;

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
