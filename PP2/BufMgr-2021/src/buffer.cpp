/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
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

// BufMgr::~BufMgr() {
//     for (FrameId i = 0; i < numBufs; i++) {
// 		if (bufDescTable[i].valid && bufDescTable[i].dirty) {
// 			bufDescTable[i].file.writePage(bufPool[i]);
// 		}
// 	}
// 	delete[] &bufPool;
// 	delete[] &bufDescTable;
// 	delete &hashTable;
// }

void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

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
