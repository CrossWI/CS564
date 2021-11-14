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
	bool zeroPin = false;

	while (1) {
		// advance the clock
		advanceClock();
		count++;
		BufDesc& curFrame = bufDescTable[clockHand];
		if (!curFrame.valid) {
			frame = clockHand;
			return;
		}
		if (curFrame.pinCnt == 0) {
			zeroPin = true;
			if (curFrame.dirty){
				curFrame.file.writePage(bufPool[clockHand]);
			}
			hashTable.remove(curFrame.file, curFrame.pageNo);
			curFrame.clear();
			frame = clockHand;
			return;
		}
		if (curFrame.refbit) {
			curFrame.refbit = false;
		}
		if (count == numBufs && !zeroPin) {
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
		// find the frame nmber
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
	allocBuf(frameNo);
	bufPool[frameNo] = file.allocatePage();
	page = &bufPool[frameNo];
	pageNo = page->page_number();
	hashTable.insert(file, pageNo, frameNo);
	bufDescTable[frameNo].Set(file, pageNo);

	// Allocate empty page in specified file using file.allocatePage()
  // Call allocBuf() to obtain buffer pool frame
  // Intert entry into hashtable and use Set()
  // Return page number of allocated page and pointer to buffer frame allocated for the page
}

void BufMgr::flushFile(File& file) {
	for (FrameId i = 0; i < numBufs; i++) {
		PageId pageNo = bufDescTable[i].pageNo;
		// check that pages belong to file
		if (bufDescTable[i].file == file) {
			// ensure proper pin count
			if (bufDescTable[i].pinCnt > 0) {
				throw PagePinnedException(file.filename(), pageNo, i);
			}
			// ensure valid page
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
			hashTable.remove(bufDescTable[i].file, pageNo);
			bufDescTable[i].clear();
		}
	}
	// Scan bufTable for pages belonging to file
	// For each:
		// If page is dirty, call file.writePage() to flush to disk, set dirty bit->false
		// Remove page from hashtable
		// Invoke Clear() 
	// Throws PagePinnedException if some page of the file is pinned
	// Throws BadBufferException if an invalid page is encountered
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
	try {
		FrameId frameNo = -1;
		hashTable.lookup(file, PageNo, frameNo);
	  	// remove the page from buffer pool
  		hashTable.remove(file, PageNo);
		bufDescTable[frameNo].clear();
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
