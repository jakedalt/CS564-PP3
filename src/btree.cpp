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

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

/**
   * BTreeIndex Constructor. 
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in
     metapage(relationName, attribute byte offset, attribute type etc.) 
     do not match with values received through constructor parameters.
   */
    BTreeIndex::BTreeIndex(const std::string &relationName,
                           std::string &outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType) {
        // Add your code below. Please do not remove this line.
    	std::cout << "starting\n";
        std::ostringstream idxStr;
        idxStr << relationName << '.' << attrByteOffset;
        // indexName is the name of the index file
        outIndexName = idxStr.str();

        //outIndexName =; // return name of index file

        // set private variables to the correct values
        bufMgr = bufMgrIn; // set private BufMgr instance
        leafOccupancy = INTARRAYLEAFSIZE;
        nodeOccupancy = INTARRAYNONLEAFSIZE;
        attributeType = attrType;
        this->attrByteOffset = attrByteOffset;
        scanExecuting = false;

        //bool fileExisted = false;

        //try {
        //	file = new BlobFile(outIndexName, true); // check if file exists and open file if it does
        //} catch(FileExistsException &e) {
        //	file = new BlobFile(outIndexName, false); // create and open file
        //	fileExisted = true;
        //}

        // index file does not exist
        try {
        	std::cout << "opening file\n";
            file = new BlobFile(outIndexName, true);
            
            Page *headerPage;
            PageId *pageNum = nullptr;
            std::cout << "creating header\n";
            bufMgr->allocPage(file, *pageNum, headerPage);
            std::cout << "allocated header\n";
            bufMgr->unPinPage(file, *pageNum, true);
            headerPageNum = *pageNum; // set headerPageNum
            Page *rootPage;
            PageId *rPageNum;
            std::cout << "creating root\n";
            bufMgr->allocPage(file, *rPageNum, rootPage);
            std::cout << "alloced index\n";
            bufMgr->unPinPage(file, *rPageNum, true);
            rootPageNum = *rPageNum; // set rootPageNum

            std::cout << "creating index\n";
            // setting indexMetaInfo variables
            IndexMetaInfo *idxMeta = (IndexMetaInfo *) headerPage;
            //idxMeta = (IndexMetaInfo*)headerPage;
            idxMeta->rootPageNo = rootPageNum;
            idxMeta->attrByteOffset = attrByteOffset;
            idxMeta->attrType = attrType;
            strncpy((char *) (&(idxMeta->relationName)), relationName.c_str(), 20);
            idxMeta->relationName[19] = 0;

            std::cout << "creating root leaf\n";
            LeafNodeInt *leafInt = (LeafNodeInt *) rootPage; // creating leafNodeObject, not sure
            leafInt->rightSibPageNo = 0; // root node does not have right sibling yet
            rootIsLeaf = 1;

            std::cout << "scanning file\n";
            // setting up variables needed for file scanning
            FileScan fileScan(relationName, bufMgr);
            RecordId rid;
            std::string recordPointer;

            bool readingFile = true; // keeps track of if end of file hasn't been reached
            std::cout << "reading file\n";
            // inserting entries for every tuple in base relation
            while (readingFile) {
                try {
                    fileScan.scanNext(rid);
                    recordPointer = fileScan.getRecord();
                    insertEntry((recordPointer.c_str()) + attrByteOffset, rid);
                } catch (EndOfFileException &e) {
                    readingFile = false;
                    fileScan.~FileScan();
                }
            }

        } catch (FileExistsException &e) { // file exists
            file = new BlobFile(outIndexName, false);
            headerPageNum = file->getFirstPageNo();
            Page *page;
            bufMgr->readPage(file, headerPageNum, page);
            bufMgr->unPinPage(file, headerPageNum, false);
            IndexMetaInfo *idxMeta = (IndexMetaInfo *) page;
            //idxMeta = (IndexMetaInfo*)page;
            rootPageNum = idxMeta->rootPageNo;

            if (idxMeta->relationName != relationName) {
                //std::cout << idxMeta->relationName << "a\n";
                //std::cout << relationName << "b\n";
                //throw BadIndexInfoException(outIndexName);
            }
            if (idxMeta->attrByteOffset != attrByteOffset) {
                throw BadIndexInfoException(outIndexName);
            }
            if (idxMeta->attrType != attrType) {
                throw BadIndexInfoException(outIndexName);
            }
        }

    }


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
/**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
*/
    BTreeIndex::~BTreeIndex() {
        // Add your code below. Please do not remove this line.

        try {
            endScan();
        } catch (ScanNotInitializedException &e) {

        }

        try {
            bufMgr->flushFile(file);
        } catch (PagePinnedException &e) { // unpinned pages if this is caught
            std::cout << "Unpinned pages present.\n";
        }

        if (file != NULL) {
            delete file;
        }

        file = NULL;

    }

/**
* Insert a key and a record ID to the appropriate position and array of the given leaf node.
*
* @param key   key to be inserted to keyArray
* @param rid   rid to be inserted to ridArray
* @param node  corresponding leaf node to be modified
* @param index index of a particular key and rid to be inserted
*/
void BTreeIndex::insertLeafNode(int key,
				    RecordId rid,
				    LeafNodeInt* node,
					int index) {
	// Shift to the right key after index and insert key at the appropriate position
	memmove(&node->keyArray[index + 1], &node->keyArray[index], sizeof(int) * (INTARRAYLEAFSIZE - index - 1));
	node->keyArray[index] = key;
	
	// Do the same for rid
	memmove(&node->ridArray[index + 1], &node->ridArray[index], sizeof(RecordId) * (INTARRAYLEAFSIZE - index - 1));
	node->ridArray[index] = rid;

	// Increment size
	node->size += 1;
}

/**
* Insert a key and a page ID to the appropriate position and array of the given non-leaf node.
*
* @param key   key to be inserted to keyArray
* @param childPageId   pageid to be inserted to pageNoArray
* @param node  corresponding non-leaf node to be modified
* @param index index of a particular key and pageid to be inserted
*/
void BTreeIndex::insertNonLeafNode(int key,
				    PageId childPageId,
					NonLeafNodeInt* node,
					int index) {
	// Shift to the right key after index and insert key at the appropriate position
	memmove(&node->keyArray[index + 1], &node->keyArray[index], sizeof(int) * (INTARRAYNONLEAFSIZE - index - 1));
	node->keyArray[index] = key;
	
	// Do the same for pid inside pageNoArray
	memmove(&node->pageNoArray[index + 2], &node->pageNoArray[index + 1], sizeof(PageId) * (INTARRAYNONLEAFSIZE - index - 1));
	node->pageNoArray[index + 1] = childPageId;

	// Increment size
	node->size += 1;
}

/**
* Helper function to do the recurion of inserting a key and a rid to the tree.
*
* @param key   key to be inserted
* @param rid   rid to be inserted
* @param currPageId  the current page which holds the node
* @param middleValueFromChild a callback parameter to pass back the middle value to its parent
* @param newlyCreatedPageId a callback parameter to pass back the splitted node's pageID to its parent
* @param isLeafBool whether this node is a leaf or not
*/
void BTreeIndex::insertEntryHelper(const int key,
					const RecordId rid,
					PageId currPageId,
					int* middleValueFromChild,
					PageId* newlyCreatedPageId,
					bool isLeafBool) 
{
	// Read page, which is a node
	Page *currNode;
  	bufMgr->readPage(file, currPageId, currNode);

  	// Base Case: current node is a leaf node
  	if (isLeafBool) {
  		LeafNodeInt* currLeafNode = (LeafNodeInt *)currNode;

  		// Index of the new key to be inserted
  		int index = currLeafNode->size;
  		for(int i = 0; i < currLeafNode->size; i++) {
  			if (currLeafNode->keyArray[i] > key) {
  				index = i;
  				break;
  			}
  		}

  		// Check if leaf is not full, if true directly insert to the leaf
  		if (currLeafNode->ridArray[INTARRAYLEAFSIZE - 1].page_number == Page::INVALID_NUMBER) {
  		//if (currLeafNode->size < INTARRAYLEAFSIZE) {	
			insertLeafNode(key, rid, currLeafNode, index);
  			bufMgr->unPinPage(file, currPageId, true);
  			*middleValueFromChild = 0;
  			*newlyCreatedPageId = 0;
  		} 
  		// Otherwise, split node into 2 and pass back the middle value
  		else {
  			// Allocate a new page for the right split
  			PageId newPageId;
  			Page* newNode;
  			bufMgr->allocPage(file, newPageId, newNode);
  			memset(newNode, 0, Page::SIZE);
  			LeafNodeInt* newLeafNode = (LeafNodeInt *)newNode;

  			// Split and add new value appropriately depending on the position of the index
  			int mid = INTARRAYLEAFSIZE / 2;
  			if (INTARRAYLEAFSIZE % 2 == 1 && index > mid) {
				mid = mid + 1;
			}

			for(int i = mid; i < INTARRAYLEAFSIZE; i++) {
				newLeafNode->keyArray[i-mid] = currLeafNode->keyArray[i];
				newLeafNode->ridArray[i-mid] = currLeafNode->ridArray[i];
				currLeafNode->keyArray[i] = 0;
				currLeafNode->ridArray[i].page_number = Page::INVALID_NUMBER;
			}

  			// Set size appropriately
  			newLeafNode->size = INTARRAYLEAFSIZE - mid;
	  		currLeafNode->size = mid;

			// Insert to right node
			if(index > INTARRAYLEAFSIZE / 2) {
				insertLeafNode(key, rid, newLeafNode, index - mid);
			}
			// Insert to left node
			else {
				insertLeafNode(key, rid, currLeafNode, index);
			}

			// Set rightSibPageNo appropriately
			newLeafNode->rightSibPageNo = currLeafNode->rightSibPageNo;
  			currLeafNode->rightSibPageNo = newPageId;

  			// Unpin the nodes
  			bufMgr->unPinPage(file, currPageId, true);
  			bufMgr->unPinPage(file, newPageId, true);

  			// Return values using pointers
  			*middleValueFromChild = newLeafNode->keyArray[0];
  			*newlyCreatedPageId = newPageId;
  		}
  	}
  	// Recursive Case: current node is not a leaf node
  	else {
  		NonLeafNodeInt *currNonLeafNode = (NonLeafNodeInt *)currNode;

  		// Find the correct child node
  		int childIndex = currNonLeafNode->size;
  		for(int i = 0; i < currNonLeafNode->size; i++) {
  			if (currNonLeafNode->keyArray[i] > key) {
  				childIndex = i;
  				break;
  			}
  		}
  		PageId currChildId = currNonLeafNode->pageNoArray[childIndex];

  		// Recursive call to the child
		int newChildMiddleKey;
		PageId newChildId;
		insertEntryHelper(key, rid, currChildId, &newChildMiddleKey, &newChildId, currNonLeafNode->level == 1);

		// If there is no split in child node
		if ((int) newChildId == 0) {
		  bufMgr->unPinPage(file, currPageId, false);
		  *middleValueFromChild = 0;
		  *newlyCreatedPageId = 0;
		}
		// If there is a split in child node
		else {
	  		// Index of the new middle key from children to be inserted
	  		int index = currNonLeafNode->size;
	  		for(int i = 0; i < currNonLeafNode->size; i++) {
	  			if (currNonLeafNode->keyArray[i] > newChildMiddleKey) {
	  				index = i;
	  				break;
	  			}
	  		}

	  		// Check if node is not full, if true directly insert middle key to the node
  			if (currNonLeafNode->pageNoArray[INTARRAYNONLEAFSIZE] == Page::INVALID_NUMBER) {
  			//if (currNonLeafNode->size < INTARRAYNONLEAFSIZE) {
  				insertNonLeafNode(newChildMiddleKey, newChildId, currNonLeafNode, index);
  				bufMgr->unPinPage(file, currPageId, true);
	  			*middleValueFromChild = 0;
	  			*newlyCreatedPageId = 0;
  			}
  			// Otherwise, split node into 2 and pass back the middle value
  			else {
	  			// Allocate a new page for the right split
	  			PageId newPageId;
	  			Page* newNode;
	  			bufMgr->allocPage(file, newPageId, newNode);
	  			memset(newNode, 0, Page::SIZE);
	  			NonLeafNodeInt* newNonLeafNode = (NonLeafNodeInt *)newNode;
	  			newNonLeafNode->level = currNonLeafNode->level;

	  			// Split nodes depending on the cases
	  			int mid = INTARRAYNONLEAFSIZE / 2;
	  			// Case 1: the new child will be pushed
	  			if (index == mid) {
	  				for(int i = mid; i < INTARRAYNONLEAFSIZE; i++) {
						newNonLeafNode->keyArray[i-mid] = currNonLeafNode->keyArray[i];
						newNonLeafNode->pageNoArray[i-mid+1] = currNonLeafNode->pageNoArray[i+1];
						currNonLeafNode->keyArray[i] = 0;
						currNonLeafNode->pageNoArray[i+1] = Page::INVALID_NUMBER;
					}
					newNonLeafNode->pageNoArray[0] = newChildId;

					// Set size appropriately
					currNonLeafNode->size = mid;
					newNonLeafNode->size = INTARRAYNONLEAFSIZE - mid;

		  			// Unpin the nodes
		  			bufMgr->unPinPage(file, currPageId, true);
		  			bufMgr->unPinPage(file, newPageId, true);

					// Return values using pointers
		  			*middleValueFromChild = newChildMiddleKey;
		  			*newlyCreatedPageId = newPageId;
	  			}
	  			// Case 2: if new child will not be pushed
	  			else {
	  				if (INTARRAYNONLEAFSIZE % 2 == 0 && index < mid) {
	  					mid -= 1;
	  				}
	  				for(int i = mid + 1; i < INTARRAYNONLEAFSIZE; i++) {
						newNonLeafNode->keyArray[i-mid-1] = currNonLeafNode->keyArray[i];
						newNonLeafNode->pageNoArray[i-mid-1] = currNonLeafNode->pageNoArray[i];
						currNonLeafNode->keyArray[i] = 0;
						currNonLeafNode->pageNoArray[i] = Page::INVALID_NUMBER;
					}

					// Return values using pointers
					*middleValueFromChild = currNonLeafNode->keyArray[mid];
					*newlyCreatedPageId = newPageId;

					// Clear already pushed value
					currNonLeafNode->keyArray[mid] = 0;

					// Set size appropriately
					currNonLeafNode->size = mid;
					newNonLeafNode->size = INTARRAYNONLEAFSIZE - mid - 1;

					// Insert new value to left or right node appropriately
					if (index < INTARRAYNONLEAFSIZE / 2) {
						insertNonLeafNode(newChildMiddleKey, newChildId, currNonLeafNode, index);
					} else {
						insertNonLeafNode(newChildMiddleKey, newChildId, newNonLeafNode, index - mid);
					}

		  			// Unpin the nodes
		  			bufMgr->unPinPage(file, currPageId, true);
		  			bufMgr->unPinPage(file, newPageId, true);
	  			}
  			}
		}
  	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// Call the helper function to do the recursion while retrieving back new middle value and pageId if there is a split
	int middleValueFromChild;
	PageId newlyCreatedPageId;
	insertEntryHelper(*(int*)key, rid, rootPageNum, &middleValueFromChild, &newlyCreatedPageId, rootIsLeaf);

	// If there is a split to the root, create a new root
	if ((int) newlyCreatedPageId != 0) {
	  	// Allocate a new page for this new root
	  	PageId newPageId;
		Page* newPage;
		bufMgr->allocPage(file, newPageId, newPage);
		memset(newPage, 0, Page::SIZE);
		NonLeafNodeInt* newRoot = (NonLeafNodeInt *)newPage;
		
		// Update the new page appropriately
		newRoot->keyArray[0] = middleValueFromChild;
		newRoot->pageNoArray[0] = rootPageNum;
		newRoot->pageNoArray[1] = newlyCreatedPageId;
		newRoot->size = 1;
		if(rootIsLeaf) {
			newRoot->level = 1;
		} else {
			newRoot->level = 0;
		}
		rootIsLeaf = false;

		// Update global variable and IndexMetaInfo page appropriately
		Page *meta;
		bufMgr->readPage(file, headerPageNum, meta);
		IndexMetaInfo *metadata = (IndexMetaInfo *)meta;
		metadata->rootPageNo = newPageId;
		rootPageNum = newPageId;

		// Unpin the root and the IndexMetaInfo page
		bufMgr->unPinPage(file, newPageId, true);
		bufMgr->unPinPage(file, headerPageNum, true);
	}
}



// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
	 * Begin a filtered scan of the index.  For instance, if the method is called 
	 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
	 * greater than "a" and less than or equal to "d".
	 * If another scan is already executing, that needs to be ended here.
	 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
	 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
   * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
    void BTreeIndex::startScan(const void *lowValParm,
                               const Operator lowOpParm,
                               const void *highValParm,
                               const Operator highOpParm) {
        // Add your code below. Please do not remove this line.


        // Integer range values for scan
        lowValInt = *(int *) lowValParm;
        highValInt = *(int *) highValParm;

        // Double range values for scan
        lowValDouble = *(double *) lowValParm;
        highValDouble = *(double *) highValParm;

        //lowValString = *(std::string *)lowValParm;
        //highValString = *(std::string *)highValParm;


        if (lowValInt > highValInt) {
            throw BadScanrangeException();
        }

        if (lowOpParm != GT && lowOpParm != GTE) {
            throw BadOpcodesException();
        }

        if (highOpParm != LT && highOpParm != LTE) {
            throw BadOpcodesException();
        }

        // check if scan is executing, and if it is, end it
        if (scanExecuting) {
            endScan();
        }

        scanExecuting = true;

        // set private Operator variables
        lowOp = lowOpParm;
        highOp = highOpParm;

        currentPageNum = rootPageNum;

        bufMgr->readPage(file, currentPageNum, currentPageData);

        //LeafNodeInt* root = (LeafNodeInt*)currentPageData;

        if (!rootIsLeaf) {
            NonLeafNodeInt *currPage = (NonLeafNodeInt *) currentPageData;
            bool leafFound = false;

            // searching for leaves
            while (!leafFound) {
                currPage = (NonLeafNodeInt *) currentPageData;
                bool pageFound = false;
                //std::cout << currPage->level;
                if (currPage->level == 1) {
                    leafFound = true;
                }
                for (int i = 0; i < nodeOccupancy; i++) {
                    // lowValInt is greater than greatest key in node, so go to last page
                    if (currPage->keyArray[i] == 0 && lowValInt > currPage->keyArray[i]) {
                        pageFound = true;
                        bufMgr->unPinPage(file, currentPageNum, false);
                        currentPageNum = currPage->pageNoArray[i + 1];
                        bufMgr->readPage(file, currentPageNum, currentPageData);
                        break;
                    }
                    // lowValInt less than key, go to corresponding page
                    if (lowValInt <= currPage->keyArray[i]) {
                        pageFound = true;
                        bufMgr->unPinPage(file, currentPageNum, false);
                        // set current page to pageNoArray index i when
                        // lowValInt is smaller than the key to the right of it
                        currentPageNum = currPage->pageNoArray[i];
                        bufMgr->readPage(file, currentPageNum, currentPageData);
                        break;
                    }
                }
                //if(!pageFound) {
                //	bufMgr->unPinPage(file, currentPageNum, false);
                //	// set current page to final node pointer in pageNoArray
                //	currentPageNum = currPage->pageNoArray[nodeOccupancy];
                //	bufMgr->readPage(file, currentPageNum, currentPageData);
                //}
            }
            // we have found the correct leaf node

            LeafNodeInt *leafPage = (LeafNodeInt *) currentPageData;
            // used for comparison of key and bound parameters
            bool usesGt = false;
            bool usesLt = false;
            if (lowOpParm == GT) {
                usesGt = true;
            }
            if (highOpParm == LT) {
                usesLt = true;
            }

            bool searching = true;
            int keyIndex;
            // searching for value that satisfies scan criteria
            while (searching) {
                leafPage = (LeafNodeInt *) currentPageData;
                for (keyIndex = 0; keyIndex < leafOccupancy; keyIndex++) {
                    if (usesGt) {
                        if (lowValInt >= leafPage->keyArray[keyIndex]) {
                            continue;
                        }
                    } else {
                        if (lowValInt > leafPage->keyArray[keyIndex]) {
                            continue;
                        }
                    }
                    if (usesLt) {
                        if (highValInt <= leafPage->keyArray[keyIndex]) {
                            throw NoSuchKeyFoundException();
                        }
                    } else {
                        if (highValInt < leafPage->keyArray[keyIndex]) {
                            throw NoSuchKeyFoundException();
                        }
                    }
                    searching = false;
                    break;
                }
                // if we've reached final leaf node without any key matching scan criteria
                if (leafPage->rightSibPageNo == 0 && searching) {
                    throw NoSuchKeyFoundException();
                }
                // search continues to leaf node to the right of the current one
                if (searching) {
                    bufMgr->unPinPage(file, currentPageNum, false);
                    currentPageNum = leafPage->rightSibPageNo;
                    bufMgr->readPage(file, currentPageNum, currentPageData);
                }
            }
            nextEntry = keyIndex + 1;
        } else { // make root current page in scan if it is a leaf
            currentPageNum = rootPageNum;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            nextEntry = 0;
        }

    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
/**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
    void BTreeIndex::scanNext(RecordId &outRid) {
        // Add your code below. Please do not remove this line.

        // if no scan has been initialized, throw error
        if (!scanExecuting) {
            throw ScanNotInitializedException();
        }

        // could be affected by startScan
        LeafNodeInt *leafPage = (LeafNodeInt *) currentPageData;

        bool usesGt = false;
        bool usesLt = false;
        if (lowOp == GT) {
            usesGt = true;
        }
        if (highOp == LT) {
            usesLt = true;
        }

        bool searching = true;
        int keyIndex;

        // searching for value that satisfies scan criteria
        while (searching) {
            leafPage = (LeafNodeInt *) currentPageData;
            for (keyIndex = nextEntry; keyIndex < leafOccupancy; keyIndex++) {
                if (usesGt) { // not in criteria, if key <= lowValInt
                    if (lowValInt >= leafPage->keyArray[keyIndex]) {
                        continue;
                    }
                } else { // not in criteria if key < lowValInt
                    if (lowValInt > leafPage->keyArray[keyIndex]) {
                        continue;
                    }
                }
                if (usesLt) { // not in criteria if key > highValInt
                    if (highValInt <= leafPage->keyArray[keyIndex]) {
                        throw IndexScanCompletedException();
                    }
                } else { // not in criteria if key >= highValInt
                    if (highValInt < leafPage->keyArray[keyIndex]) {
                        throw IndexScanCompletedException();
                    }
                }
                searching = false;
                break;
            }
            nextEntry = keyIndex;
            // if we've reached final leaf node without any key matching scan criteria
            if (leafPage->rightSibPageNo == 0 && searching) {
                throw IndexScanCompletedException();
            }
            // search continues to leaf node to the right of the current one
            if (searching) {
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
/**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
    void BTreeIndex::endScan() {
        // Add your code below. Please do not remove this line.

        // if no scan has been initialized, throw error
        if (!scanExecuting) {
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
