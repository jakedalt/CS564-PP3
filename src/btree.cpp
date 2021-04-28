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
#include "exceptions/page_pinned_exception.h"


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
		file = new BlobFile(outIndexName, true); // check if file exists and open file if it does
	} catch(FileExistsException &e) {
		file = new BlobFile(outIndexName, false); // create and open file
		fileExisted = true;
	}

	// index file does not exist
	if(!fileExisted) {
		Page *headerPage;
		PageId *pageNum;
		bufMgr->allocPage(file, *pageNum, headerPage);
		bufMgr->unPinPage(file, *pageNum, false);
		headerPageNum = *pageNum; // set headerPageNum
		Page *rootPage;
		PageId *rPageNum;
		bufMgr->allocPage(file, *rPageNum, rootPage);
		bufMgr->unPinPage(file, *rPageNum, false);
		rootPageNum = *rPageNum; // set rootPageNum

		// setting indexMetaInfo variables
		IndexMetaInfo* idxMeta = new IndexMetaInfo();
		idxMeta = (IndexMetaInfo*)headerPage;
		idxMeta->rootPageNo = rootPageNum;
		idxMeta->attrByteOffset = attrByteOffset;
		idxMeta->attrType = attrType;
		strcpy(idxMeta->relationName, relationName.c_str());

		LeafNodeInt* leafInt = (LeafNodeInt*)rootPage; // creating leafNodeObject, not sure
		leafInt->rightSibPageNo = 0; // root node does not have right sibling yet		
		rootIsLeaf = 1;

		// setting up variables needed for file scanning
 		FileScan* fileScan = new FileScan(relationName, bufMgr);
		RecordId rid;
		std::string recordPointer;

		bool readingFile = true; // keeps track of if end of file hasn't been reached

		// inserting entries for every tuple in base relation
		while(readingFile) {
			try {
				fileScan->scanNext(rid);
				recordPointer = fileScan->getRecord();
				insertEntry((recordPointer.c_str()) + attrByteOffset, rid); 
			} catch(EndOfFileException &e) {
				readingFile = false;
				fileScan->~FileScan();
			}
		}		

	} else { // file exists
		headerPageNum = file->getFirstPageNo();
		Page *page = new Page();
		bufMgr->readPage(file, headerPageNum, page);
		bufMgr->unPinPage(file, headerPageNum, false);
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

	try {
		endScan();
	} catch(ScanNotInitializedException &e) {

	}
		
	try {
		bufMgr->flushFile(file);
	} catch(PagePinnedException &e) { // unpinned pages if this is caught
		std::cout << "Unpinned pages present.\n";
	}
	
	if(file != NULL) {
		delete file;
	}

	file = NULL;

}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.

	//TODO We created a private int variable called rootIsLeaf that should
	// be changed to 0 when root is no longer a leaf
	//bool rootIsLeaf = true;

	if (rootPageNum == NULL) {
		//create a new page into the buffer manager and add to the b+ tree root leaf
        LeafNodeInt *root = new LeafNodeInt();
        //root->keyArray[0] = key;
        root->ridArray[0] = rid;
        root->rightSibPageNo = NULL;
    }else{
    	//traverse through the tree and find where the key,id pair should be inserted

		// need to compare the new key id pair starting from the root and moving along the leaves.

		Page *RootPage;
		Page *HeaderPage; // use header page for index
		bufMgr -> readPage(file,headerPageNum,HeaderPage);
		bufMgr->unPinPage(file, headerPageNum, false);
		IndexMetaInfo* idxMeta = new IndexMetaInfo();
		idxMeta = (IndexMetaInfo*)HeaderPage;
		bufMgr -> readPage(file,idxMeta->rootPageNo,RootPage);
		bufMgr->unPinPage(file, idxMeta->rootPageNo, false);

		bool isLeaf = false;
		RIDKeyPair<Datatype> *pair;
		pair->set(rid,key);

		RecordId place_rec_id;
		Page *place_page;
		NonLeafNodeInt *cursor = (NonLeafNodeInt*)RootPage; //get the current root page using pageID

		while(!isLeaf){

				for(int i =0; i<nodeOccupancy; i++){
					Page *nextPage;
	               	bufMgr -> readPage(file,cursor->pageNoArray[i],nextPage);
					bufMgr->unPinPage(file, cursor->pageNoArray[i], false);
	              	
	               	for (PageIterator iter = nextPage->begin();
				        iter != nextPage->end();
				        ++iter) {
				     	RIDKeyPair curLeafPair;
				     	curLeafPair->set(iter.getCurrentRecord(),cursor->keyArray[i]);
						if(pair < curLeafPair){
							//go to the left side of the tree
							place_rec_id = iter.getCurrentRecord();
							place_page = nextPage;
							break;
						}
						
					}

					if (i == leafOccupancy - 1) {
			                //reached the end of the current leaf need to go to the next leaf node
			                break;
		              	}
	                	
				}
		}
		//LeafNodeInt curLeafNode;
		//using the while loop constantly move through the children from the root while comparing the each the entries in the btree to find the leafe node to place the key value pair
		
		LeafNodeInt *found = (LeafNodeInt*)place_page;
		//found->ridArray.length 
		//need to figure out how to get the length of the keys in the current leaf node
		if(0 < leafOccupancy){
			//the leafe we found to insert the record id key is not over flowing and we shoudl travers through it till the key fits
			int i = 0;// found the index where the current pair is greater than the left less than the right
            while (pair > found->ridArray[i]
                   && i < leafOccupancy) {
                i++;
            }

            for (int j = leafOccupancy;
                 j > i; j--) {
                found->ridArray[j]
                    = found->ridArray[j - 1];
            }
  			
  			//we found space in the leaf node to set the rid and key
            found->ridArray[i] = rid;
            //found->keyArray[i] = key;
		}else{
			// handle the case here the current leafe node is overfilling

		}
	}
}

void BTreeIndex::insertHelper(){

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

	scanExecuting = true;

	// set private Operator variables
	lowOp = lowOpParm;
	highOp = highOpParm;

	currentPageNum = rootPageNum;

	bufMgr->readPage(file, currentPageNum, currentPageData);

	LeafNodeInt* root = (LeafNodeInt*)currentPageData;

	if(!rootIsLeaf) {
		NonLeafNodeInt* currPage = (NonLeafNodeInt*)currentPageData;
		bool leafFound = false;

		// searching for leaves
		while(!leafFound) {
			currPage = (NonLeafNodeInt*)currentPageData;
			bool pageFound = false;
			if(currPage->level == 1) {
				leafFound = true;
			}
			for(int i = 0; i < nodeOccupancy; i++) {
				if(lowValInt < currPage->keyArray[i]) {
					pageFound = true;
					bufMgr->unPinPage(file, currentPageNum, false);
					// set current page to pageNoArray index i when 
					// lowValInt is smaller than the key to the right of it
					currentPageNum = currPage->pageNoArray[i];
					bufMgr->readPage(file, currentPageNum, currentPageData);
				}
			}
			if(!pageFound) {
				bufMgr->unPinPage(file, currentPageNum, false);
				// set current page to final node pointer in pageNoArray
				currentPageNum = currPage->pageNoArray[nodeOccupancy];
				bufMgr->readPage(file, currentPageNum, currentPageData);
			}
		}
		// we have found the correct leaf node
		
		LeafNodeInt* leafPage = (LeafNodeInt*)currentPageData;
		// used for comparison of key and bound parameters
		bool usesGt = false;
		bool usesLt = false;
		if(lowOpParm == GT) {
			usesGt = true;
		}
		if(highOpParm == LT) {
			usesLt = true;
		}
		
		bool searching = true;
		int keyIndex;
		// searching for value that satisfies scan criteria
		while(searching) {
			leafPage = (LeafNodeInt*)currentPageData;
			for(keyIndex = 0; keyIndex < leafOccupancy; keyIndex++) {
				if(usesGt) {
					if(lowValInt >= leafPage->keyArray[keyIndex]) {
						continue;
					}
				} else {
					if(lowValInt > leafPage->keyArray[keyIndex]) {
						continue;
					} 
				}
				if(usesLt) {
					if(highValInt <= leafPage->keyArray[keyIndex]) {
						throw NoSuchKeyFoundException();
					}
				} else {
					if(highValInt < leafPage->keyArray[keyIndex]) {
						throw NoSuchKeyFoundException();
					}
				}
				searching = false;
				break;
			}
			// if we've reached final leaf node without any key matching scan criteria
			if(leafPage->rightSibPageNo == 0 && searching) {
				throw NoSuchKeyFoundException();
			}
			// search continues to leaf node to the right of the current one
			if(searching) {
				bufMgr->unPinPage(file, currentPageNum, false);
				currentPageNum = leafPage->rightSibPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
			}
		}
		nextEntry = keyIndex + 1;
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.

	// if no scan has been initialized, throw error
	if(!scanExecuting) {
		throw ScanNotInitializedException();
	}
	
	LeafNodeInt* leafPage = (LeafNodeInt*)currentPageData;

	bool usesGt = false;
	bool usesLt = false;
	if(lowOp == GT) {
		usesGt = true;
	}
	if(highOp == LT) {
		usesLt = true;
	}
		
	bool searching = true;
	int keyIndex;
	
	// searching for value that satisfies scan criteria
	while(searching) {
		leafPage = (LeafNodeInt*)currentPageData;
		for(keyIndex = nextEntry; keyIndex < leafOccupancy; keyIndex++) {
			if(usesGt) {
				if(lowValInt >= leafPage->keyArray[keyIndex]) {
					continue;
				}
			} else {
				if(lowValInt > leafPage->keyArray[keyIndex]) {
					continue;
				} 
			}
			if(usesLt) {
				if(highValInt <= leafPage->keyArray[keyIndex]) {
					throw IndexScanCompletedException();
				}
			} else {
				if(highValInt < leafPage->keyArray[keyIndex]) {
					throw IndexScanCompletedException();
				}
			}
			searching = false;
			break;
		}
		nextEntry = keyIndex;
		// if we've reached final leaf node without any key matching scan criteria
		if(leafPage->rightSibPageNo == 0 && searching) {
			throw IndexScanCompletedException();
		}
		// search continues to leaf node to the right of the current one
		if(searching) {
			nextEntry = 0; // start on first index of next page
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = leafPage->rightSibPageNo;
			bufMgr->readPage(file, currentPageNum, currentPageData);
		}
		outRid = leafPage->ridArray[nextEntry];
	}	

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.

	// if no scan has been initialized, throw error
	if(!scanExecuting) {
		throw ScanNotInitializedException();
	}

	scanExecuting = false;

	// unpin any pinned pages
	bufMgr->unPinPage(file, currentPageNum, false);
	
	// reset scan specific variables
	scanExecuting = false;
	nextEntry = -1;
	currentPageNum = -1;	
	currentPageData = NULL;
		

}

}
