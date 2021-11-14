/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

/**
 * |----------------------------|
 * | Name					 | Student ID |
 * |---------------|------------|
 * | Cameron Cross | 9081611148 |
 * | Richard Wang  | 9081657562 |
 * |----------------------------|
 */ 

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }
  clockHand = bufs - 1;
}

/**
 * Advance clock to next frame in the buffer pool.
 */
void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

/**
 * Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back
 * to disk.
 * 
 * @param frame frame ID of frame to be allocated
 * 
 * @throws BufferExceededException if all buffer frames are pinned
 */
void BufMgr::allocBuf(FrameId& frame) {
	// count for clockHand
	uint32_t count = 0;
	bool empty = false;

	// clock algorithm, advanced until entry with refbit = false and pinCnt = 0
	while (1) {
		// advance the clock
		advanceClock();
		// increment count
		count++;
		// check for valid entry
		if (!bufDescTable[clockHand].valid) {
			// move clock hand
			frame = clockHand;
			break;
		}
		// check for refbit flag
		if (bufDescTable[clockHand].refbit) {
			bufDescTable[clockHand].refbit = false;
		}
		// check if pin count is 0
		if (bufDescTable[clockHand].pinCnt == 0) {
			empty = true;
			// write to disk if dirty
			if (bufDescTable[clockHand].dirty) {
				bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
			}
			// remove from hash table
			hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			// clear the flags
			bufDescTable[clockHand].clear();
			// move clock hand
			frame = clockHand;
			break;
		}
		// everything is pinned
		if (count >= numBufs && empty == false) {
			throw BufferExceededException();
		}
	}
}

/**
 * Reads a given page from the file into a frame.
 * Two options:
 * (a) Page is not in the buffer pool
 *    - allocate a new frame of the buffer pool for reading the page
 * (b) Page is in the buffer pool
 * 	  - set the appropriate refbit and pin count
 * 
 * @param file   File to use
 * @param PageNo Page number to be read from file
 * @param page   Used to get requested page object to read
 */ 
void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  FrameId frameNo = -1;

  try {
    // see if page exists in buffer pool
    hashTable.lookup(file, pageNo, frameNo);
		// set appropriate refbit
		bufDescTable[frameNo].refbit = true;
    //increment pin count
    bufDescTable[frameNo].pinCnt++;
  } catch (const HashNotFoundException&) {
		// allocate buffer frame
    allocBuf(frameNo);
		// insert page into hash table
		bufPool[frameNo] = file.readPage(pageNo);
    hashTable.insert(file, pageNo, frameNo);
		// set up frame properly
    bufDescTable[frameNo].Set(file, pageNo);
  }
  page = &bufPool[frameNo];
}

/**
 * Unpin a page from the buffer pool
 * 
 * @param file   File to use
 * @param pageNo Page number of page being unpinned
 * @param dirty  used to set dirty but of buffer pool frame
 * 
 * @throws PageNotPinnedException if the pin count is 0
 */ 
void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
	try {
		FrameId frameNo;
		// see if page exists in buffer pool
		hashTable.lookup(file, pageNo, frameNo);

		// ensure pin count > 0
		if (bufDescTable[frameNo].pinCnt == 0) {
			throw PageNotPinnedException(file.filename(), pageNo, frameNo);
		}
		// set dirty bit if neccessary
		if (dirty == true) {
			bufDescTable[frameNo].dirty = true;
		}
		// decrement pin count
		bufDescTable[frameNo].pinCnt--;
	}
  	catch (const HashNotFoundException&) {}
}

/**
 * Allocates a new page in the file
 * 
 * @param file    File to allocate page in
 * @param pageNo  Page number of page being allocated
 * @param page    Page to allocate space for
 */ 
void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
	FrameId frameNo = -1;
	// allocate space in buffer pool
	allocBuf(frameNo);
	// allocate empty page
	bufPool[frameNo] = file.allocatePage();
	// get page in frame of buffer pool
	page = &bufPool[frameNo];
	// get page's page number
	pageNo = page->page_number();
	// insert into hash table
	hashTable.insert(file, pageNo, frameNo);
	// set the page's flags (refbit, dirty bit, etc.)
	bufDescTable[frameNo].Set(file, pageNo);
}

/**
 * Deletes a particular page from a file.
 * 
 * @param file   File to delete page from
 * @param PageNo Number of page to dispose of
 */ 
void BufMgr::disposePage(File& file, const PageId PageNo) {
	try {
		FrameId frameNo = -1;
		// see if page exists in buffer pool
		hashTable.lookup(file, PageNo, frameNo);
	  // remove the page from buffer pool
  	hashTable.remove(file, PageNo);
		// clear bits
		bufDescTable[frameNo].clear();
		// delete page
		file.deletePage(PageNo);
	} catch (const HashNotFoundException&) {}
}

/**
 * Writes all dirty pages in buffer pool frame to disk
 * 
 * @param file File to flush dirty pages from
 * 
 * @throws PagePinnedException if some page of the file is pinned
 * @throws BadBufferException if an invalid page belonging to the file is encountered
 */ 
void BufMgr::flushFile(File& file) {
	// scan bufTable for pages belonging to file
	for (FrameId i = 0; i < numBufs; i++) {
		PageId pageNo = bufDescTable[i].pageNo;
		// check that pages belong to file
		if (bufDescTable[i].file == file) {
			// ensure page is not pinned
			if (bufDescTable[i].pinCnt > 0) {
				throw PagePinnedException(file.filename(), pageNo, i);
			}
			// ensure page is valid 
			if (!bufDescTable[i].valid) {
				throw BadBufferException (i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			// check dirty bit
			if (bufDescTable[i].dirty == true) {
				// flush page to disk
				bufDescTable[i].file.writePage(bufPool[i]);
				// set dirty bit
				bufDescTable[i].dirty = false;
			}
			// remove from hash table
			hashTable.remove(bufDescTable[i].file, pageNo);
			// clear bits
			bufDescTable[i].clear();
		}
	}
}

/**
 * Prints the contents of the buffer pool
 */ 
void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
