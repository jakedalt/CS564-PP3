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
            file = new BlobFile(outIndexName, true);
            Page *headerPage;
            PageId *pageNum = nullptr;
            bufMgr->allocPage(file, *pageNum, headerPage);
            bufMgr->unPinPage(file, *pageNum, true);
            headerPageNum = *pageNum; // set headerPageNum
            Page *rootPage;
            PageId *rPageNum;
            bufMgr->allocPage(file, *rPageNum, rootPage);
            bufMgr->unPinPage(file, *rPageNum, true);
            rootPageNum = *rPageNum; // set rootPageNum

            // setting indexMetaInfo variables
            IndexMetaInfo *idxMeta = (IndexMetaInfo *) headerPage;
            //idxMeta = (IndexMetaInfo*)headerPage;
            idxMeta->rootPageNo = rootPageNum;
            idxMeta->attrByteOffset = attrByteOffset;
            idxMeta->attrType = attrType;
            strncpy((char *) (&(idxMeta->relationName)), relationName.c_str(), 20);
            idxMeta->relationName[19] = 0;

            LeafNodeInt *leafInt = (LeafNodeInt *) rootPage; // creating leafNodeObject, not sure
            leafInt->rightSibPageNo = 0; // root node does not have right sibling yet
            rootIsLeaf = 1;

            // setting up variables needed for file scanning
            FileScan fileScan(relationName, bufMgr);
            RecordId rid;
            std::string recordPointer;

            bool readingFile = true; // keeps track of if end of file hasn't been reached

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

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
	 * Insert a new entry using the pair <value,rid>. 
	 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
	 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
	 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
	 * Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char string
   * @param rid			Record ID of a record whose entry is getting inserted into the index.
	**/
    void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        // Add your code below. Please do not remove this line.

        //TODO We created a private int variable called rootIsLeaf that should
        // be changed to 0 when root is no longer a leaf
        //bool rootIsLeaf = true;
        int* k = (int *) key;
        if (rootPageNum == NULL) {
            //create a new page into the buffer manager and add to the b+ tree root leaf
            LeafNodeInt *root = new LeafNodeInt();
            root->keyArray[0] = *k;
            root->ridArray[0] = rid;
            root->rightSibPageNo = NULL;
            return;
        } else {
            //traverse through the tree and find where the key,id pair should be inserted

            // need to compare the new key id pair starting from the root and moving along the leaves.

            Page *RootPage;
            Page *HeaderPage; // use header page for index
            bufMgr->readPage(file, headerPageNum, HeaderPage);
            bufMgr->unPinPage(file, headerPageNum, false);
            IndexMetaInfo *idxMeta = new IndexMetaInfo();
            idxMeta = (IndexMetaInfo *) HeaderPage;
            bufMgr->readPage(file, idxMeta->rootPageNo, RootPage);
            bufMgr->unPinPage(file, idxMeta->rootPageNo, false);

            bool isLeaf = false;
            RIDKeyPair<int> *pair;
            pair->set(rid, *k);

            RecordId place_rec_id;
            Page *place_page;
            NonLeafNodeInt *cursor = (NonLeafNodeInt *) RootPage; //get the current root page using pageID

            NonLeafNodeInt *parent;

            while (!isLeaf) {

                parent = cursor;

                for (int i = 0; i < nodeOccupancy; i++) {
                    Page *nextPage;
                    bufMgr->readPage(file, cursor->pageNoArray[i], nextPage);
                    bufMgr->unPinPage(file, cursor->pageNoArray[i], false);

                    for (PageIterator iter = nextPage->begin();
                         iter != nextPage->end();
                         ++iter) {
                        RIDKeyPair<int> *comparePair;
                        comparePair->set(iter.getCurrentRecord(), cursor->keyArray[i]);
                        if (pair < comparePair) {
                            //go to the left side of the tree
                            place_rec_id = iter.getCurrentRecord();
                            place_page = nextPage;
                            isLeaf = true;
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

            LeafNodeInt *found = (LeafNodeInt *) place_page;
            //found->ridArray.length
            //need to figure out how to get the length of the keys in the current leaf node
            if (0 < leafOccupancy) {
                //the leaf we found to insert the record id key is not over flowing and we should traverse through it till the key fits
                int i = 0;// found the index where the current pair is greater than the left less than the right
                while (rid.page_number > found->ridArray[i].page_number
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
            } else {
                // handle the case here the current leafe node is overfilling
                LeafNodeInt *newLeaf = new LeafNodeInt;
                Page *newLeafPage;
                PageId *nLeafPageNum;
                bufMgr->allocPage(file, *nLeafPageNum, newLeafPage);
                bufMgr->unPinPage(file, *nLeafPageNum, true);

                int virtualNode[leafOccupancy + 1];

                // Update cursor to virtual
                // node created
                for (int i = 0; i < leafOccupancy; i++) {
                    virtualNode[i]
                            = cursor->keyArray[i];
                }
                int i = 0, j;

                // Traverse to find where the new
                // node is to be inserted
                while (*k > virtualNode[i]
                       && i < leafOccupancy) {
                    i++;
                }

                // Update the current virtual
                // Node to its previous
                for (int j = leafOccupancy + 1;
                     j > i; j--) {
                    virtualNode[j]
                            = virtualNode[j - 1];
                }

                virtualNode[i] = *k;
                //newLeaf->IS_LEAF = true;

                cursor->level++;
            	newInternal->level = cursor->level + 1;
                //cursor->size = (leafOccupancy + 1) / 2;
                //newLeaf->size
                //    = leafOccupancy + 1 - (leafOccupancy + 1) / 2;

                cursor->pageNoArray[sizeof(cursor->keyArray)]
                        = rid.page_number;

                newLeaf->rightSibPageNo
                        = cursor->pageNoArray[leafOccupancy];

                cursor->pageNoArray[leafOccupancy] = NULL;

                // Update the current virtual
                // Node's key to its previous
                for (i = 0;
                     i < sizeof(cursor->keyArray); i++) {
                    cursor->keyArray[i]
                            = virtualNode[i];
                }

                // Update the newLeaf key to
                // virtual Node
                for (i = 0, j = sizeof(cursor->keyArray);
                     i < sizeof(newLeaf->ridArray);
                     i++, j++) {
                    newLeaf->keyArray[i]
                            = virtualNode[j];
                }

                // If cursor is the root node
                if (place_rec_id.page_number == rootPageNum) {

                    // Create a new Node
                    NonLeafNodeInt *newRoot = new NonLeafNodeInt;
                    Page *newRootPage;
                    PageId *nRootPageNum;
                    bufMgr->allocPage(file, *nRootPageNum, newRootPage);
                    bufMgr->unPinPage(file, *nRootPageNum, true);
                    rootPageNum = *nRootPageNum; // set rootPageNum

                    // Update rest field of
                    // B+ Tree Node
                    newRoot->pageNoArray[0] = newLeaf->ridArray[0].page_number;
                    int index;
                    for (i = 0;
                         i < sizeof(cursor->keyArray); i++) {
                        newRoot->pageNoArray[i] = cursor->pageNoArray[i];
                        index = i;
                    }
                    newRoot->pageNoArray[index + 1] = newLeaf->rightSibPageNo;
                    rootPageNum = newRoot->pageNoArray[0];
                } else {

                    // Recursive Call for
                    // insert in internal
                    insertInternal(*k,newLeaf->ridArray[sizeof(cursor->keyArray)],
                                   parent,
                                   newLeaf, place_rec_id, *nLeafPageNum);
                }

            }
        }
    }

    void BTreeIndex::insertInternal(int k, RecordId x,
                                    NonLeafNodeInt *cursor,
                                    void *child, RecordId cursorRID, PageId child_page_id) {
        LeafNodeInt *leaf_child = (LeafNodeInt *) child;
        // If we doesn't have overflow
        if (sizeof(cursor->keyArray) < leafOccupancy) {
            int i = 0;

            // Traverse the child node
            // for current cursor node
            while (k > cursor->keyArray[i]
                   && i < sizeof(cursor->keyArray)) {
                i++;
            }

            // Traverse the cursor node
            // and update the current key
            // to its previous node key
            for (int j = sizeof(cursor->keyArray);
                 j > i; j--) {

                cursor->keyArray[j]
                        = cursor->keyArray[j - 1];
            }

            // Traverse the cursor node
            // and update the current ptr
            // to its previous node ptr
            for (int j = sizeof(cursor->keyArray) + 1;
                 j > i + 1; j--) {
                cursor->pageNoArray[j]
                        = cursor->pageNoArray[j - 1];
            }

            cursor->keyArray[i] = k;
            cursor->pageNoArray[i + 1] = child_page_id;
        }

            // For overflow, break the node
        else {

            // For new Interval
            NonLeafNodeInt *newInternal = new NonLeafNodeInt;
            Page *newIntervalPage;
            PageId *nPageNum;
            bufMgr->allocPage(file, *nPageNum, newIntervalPage);
            bufMgr->unPinPage(file, *nPageNum, true);
            rootPageNum = *nPageNum; // set rootPageNum

            int virtualKey[leafOccupancy + 1];
            PageId virtualPtr[leafOccupancy + 2];

            // Insert the current list key
            // of cursor node to virtualKey
            for (int i = 0; i < leafOccupancy; i++) {
                virtualKey[i] = cursor->keyArray[i];
            }

            // Insert the current list ptr
            // of cursor node to virtualPtr
            for (int i = 0; i < leafOccupancy + 1; i++) {
                virtualPtr[i] = cursor->pageNoArray[i];
            }

            int i = 0, j;

            // Traverse to find where the new
            // node is to be inserted
            while (k > virtualKey[i]
                   && i < leafOccupancy) {
                i++;
            }

            // Traverse the virtualKey node
            // and update the current key
            // to its previous node key
            for (int j = leafOccupancy + 1;
                 j > i; j--) {

                virtualKey[j]
                        = virtualKey[j - 1];
            }

            virtualKey[i] = k;

            // Traverse the virtualKey node
            // and update the current ptr
            // to its previous node ptr
            for (int j = leafOccupancy + 2;
                 j > i + 1; j--) {
                virtualPtr[j]
                        = virtualPtr[j - 1];
            }

            virtualPtr[i + 1] = leaf_child->ridArray[0].page_number;
            //newInternal->IS_LEAF = false;
            cursor->level++;
            newInternal->level = cursor->level + 1;
            //cursor->size
            //    = (leafOccupancy + 1) / 2;

            //newInternal->size
            //    = leafOccupancy - (leafOccupancy + 1) / 2;

            // Insert new node as an
            // internal node
            for (i = 0, j = sizeof(cursor->keyArray) + 1;
                 i < sizeof(newInternal->keyArray);
                 i++, j++) {

                newInternal->keyArray[i]
                        = virtualKey[j];
            }

            for (i = 0, j = sizeof(cursor->keyArray) + 1;
                 i < sizeof(newInternal->keyArray) + 1;
                 i++, j++) {

                newInternal->pageNoArray[i]
                        = virtualPtr[j];
            }

            // If cursor is the root node
            if (cursorRID.page_number == rootPageNum) {

                // Create a new root node
                NonLeafNodeInt *newRoot = new NonLeafNodeInt;
                newRoot->level = 0;
                Page *newPage;
                PageId *newPageNum;
                bufMgr->allocPage(file, *newPageNum, newPage);
                bufMgr->unPinPage(file, *newPageNum, true);
                rootPageNum = *newPageNum; // set rootPageNum

                // Update key value
                newRoot->keyArray[0]
                        = cursor->keyArray[sizeof(cursor->keyArray)];

                // Update rest field of
                // B+ Tree Node
                newRoot->pageNoArray[0] = cursorRID.page_number;
                newRoot->pageNoArray[1] = *nPageNum;
                //newRoot->IS_LEAF = false;
                //newRoot->size = 1;
                rootPageNum = *newPageNum;
            } else {

                // Recursive Call to insert
                // the data
                insertInternal(k,x,
                               cursor,
                               newInternal,cursorRID,child_page_id);
            }
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
