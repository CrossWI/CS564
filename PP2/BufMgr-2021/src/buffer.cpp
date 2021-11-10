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

void BufMgr::advanceClock() {
  // find the size of the buffer pool
  int clockSize = sizeof(bufPool)/sizeof(bufPool[0]);
  
  // if clockHand == clockSize, reset clockHand to position 0
  clockHand = (clockHand + 1) % clockSize;
}

void BufMgr::allocBuf(FrameId& frame) {
	// Throw BufferExceededException if all buffer frames are pinned

	// Allocates a free frame using the clock algorithm

	// If the buffer frame allocated has a valid page in it, you remove the appropriate entry from the hash table.
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
	// Check if in buffer using lookup()

  // Case 1: In buffer pool
	// Set appropriate refbit
	// Increment pinCnt
	// Return pointer to frame containing page
  FrameId frameNo = -1;

  try {
    // see if page exists in buffer pool
    hashTable.lookup(file, pageNo, frameNo);
    //increment pin count
    bufDescTable[frameNo].pinCnt++;
  } catch (badgerdb::HashNotFoundException& e) {
    allocBuf(frameNo);
    file.readPage(pageNo);

    //find available frameNo

    hashTable.insert(file, pageNo, frameNo);
  }


	// Case 2: Not in buffer pool
	// Call allocBuf() to allocate a buffer frame
	// Call file.readPage() to read page from disk
	// Insert page into hashtable
	// Invoke Set() to update pinCnt for the page to 1
	// Return pointer to frame containing page


}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  FrameId frameNo = -1;
  try {
    // find the frame nmber
    hashTable.lookup(file, pageNo, frameNo);
    bufDescTable[frameNo].pinCnt--;
  }
  catch (badgerdb::HashNotFoundException& e) {
    // throw PageNotPinnedException
    throw(PageNotPinnedException(file.filename(), pageNo, frameNo));
  }
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
	// Allocate empty page in specified file using file.allocatePage()
	// Call allocBuf() to obtain buffer pool frame
	// Intert entry into hashtable and use Set()
	// Return page number of allocated page and pointer to buffer frame allocated for the page
}

void BufMgr::flushFile(File& file) {
	// Scan bufTable for pages belonging to file
	// For each:
		// If page is dirty, call file.writePage() to flush to disk, set dirty bit->false
		// Remove page from hashtable
		// Invoke Clear() 
	// Throws PagePinnedException if some page of the file is pinned
	// Throws BadBufferException if an invalid page is encountered
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
  //remove the page from buffer pool
  hashTable.remove(file,PageNo);
  
  // delete the page
  file.deletePage(PageNo);
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
