/*-------------------------------------------------------------------------
 *
 * cbmodify.c
 *	  Routines to make a change to a conveyor belt and XLOG it if needed.
 *
 * Each function in this file implements one type of conveyor-belt write
 * operation. The pages to be modified are assumed to already have been
 * identified and locked.
 *
 * Each function in this file has a corresponding REDO function in
 * cbxlog.c, except where log_newpage is used.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/backend/access/conveyor/cbmodify.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/cbfsmpage.h"
#include "access/cbindexpage.h"
#include "access/cbmetapage.h"
#include "access/cbmodify.h"
#include "access/cbxlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"

/*
 * Create a metapage, and optionally write XLOG for the change.
 */
void
cb_create_metapage(RelFileNode *rnode,
				   ForkNumber fork,
				   Buffer metabuffer,
				   uint16 pages_per_segment,
				   bool needs_xlog)
{
	Page		metapage;
	CBMetapageData *meta;

	metapage = BufferGetPage(metabuffer);
	meta = cb_metapage_get_special(metapage);
	cb_metapage_initialize(metapage, pages_per_segment);

	if (needs_xlog)
	{
		XLogRecPtr	lsn;

		lsn = log_newpage(rnode, fork, CONVEYOR_METAPAGE, metapage, true);
		PageSetLSN(metapage, lsn);
	}

	MarkBufferDirty(metabuffer);
}

/*
 * Create a new FSM page, and optionally write XLOG for the change.
 */
CBSegNo
cb_create_fsmpage(RelFileNode *rnode,
				  ForkNumber fork,
				  BlockNumber blkno,
				  Buffer buffer,
				  uint16 pages_per_segment,
				  bool needs_xlog)
{
	Page		page;
	CBSegNo		segno;

	START_CRIT_SECTION();

	page = BufferGetPage(buffer);
	segno = cb_fsmpage_initialize(page, blkno, pages_per_segment);
	MarkBufferDirty(buffer);

	if (needs_xlog)
	{
		XLogRecPtr	lsn;

		lsn = log_newpage(rnode, fork, blkno, page, true);
		PageSetLSN(page, lsn);
	}

	END_CRIT_SECTION();

	return segno;
}

/*
 * Insert a payload page, and optionally write XLOG for the change.
 *
 * Since we have no idea what the contents of the payload page ought to be,
 * it's up to the caller to initialize it before calling this function.
 * That means that the caller is also responsible for starting and ending
 * the required critical section.
 */
void
cb_insert_payload_page(RelFileNode *rnode, ForkNumber fork, Buffer metabuffer,
					   BlockNumber payloadblock, Buffer payloadbuffer,
					   bool needs_xlog, bool page_std)
{
	Page		metapage;
	Page		payloadpage;
	CBMetapageData *meta;

	Assert(CritSectionCount > 0);

	payloadpage = BufferGetPage(payloadbuffer);
	MarkBufferDirty(payloadbuffer);

	metapage = BufferGetPage(metabuffer);
	meta = cb_metapage_get_special(metapage);
	cb_metapage_advance_next_logical_page(meta, payloadblock);
	MarkBufferDirty(metabuffer);

	if (needs_xlog)
	{
		XLogRecPtr	lsn;
		int			flags = REGBUF_FORCE_IMAGE;

		if (page_std)
			flags |= REGBUF_STANDARD;

		XLogBeginInsert();
		XLogRegisterBlock(0, rnode, fork, CONVEYOR_METAPAGE, metapage,
						  REGBUF_STANDARD);
		XLogRegisterBlock(1, rnode, fork, payloadblock,
						  payloadpage, flags);
		lsn = XLogInsert(RM_CONVEYOR_ID,
						 XLOG_CONVEYOR_INSERT_PAYLOAD_PAGE);

		PageSetLSN(payloadpage, lsn);
		PageSetLSN(metapage, lsn);
	}
}

/*
 * Allocate a new payload segment, and optionally write XLOG for the change.
 *
 * If the allocation status of the segment is tracked in the metapage,
 * 'fsmblock' should be InvalidBlockNumber and 'fsmbuffer' should be
 * InvalidBuffer. Otherwise, 'fsmblock' should be the block number of the
 * relevant freespace map block and 'fsmbuffer' the corresponding buffer.
 *
 * See cb_xlog_allocate_payload_segment for the corresponding REDO routine.
 */
void
cb_allocate_payload_segment(RelFileNode *rnode,
							ForkNumber fork,
							Buffer metabuffer,
							BlockNumber fsmblock,
							Buffer fsmbuffer,
							CBSegNo segno,
							bool is_extend,
							bool needs_xlog)
{
	Page		metapage;
	CBMetapageData *meta;

	metapage = BufferGetPage(metabuffer);
	meta = cb_metapage_get_special(metapage);

	START_CRIT_SECTION();

	cb_metapage_add_index_entry(meta, segno);
	MarkBufferDirty(metabuffer);

	if (is_extend)
		cb_metapage_increment_next_segment(meta, segno);

	if (fsmblock != InvalidBlockNumber)
	{
		cb_fsmpage_set_fsm_bit(BufferGetPage(fsmbuffer), segno, true);
		MarkBufferDirty(fsmbuffer);
	}
	else
		cb_metapage_set_fsm_bit(meta, segno, true);

	if (needs_xlog)
	{
		xl_cb_allocate_payload_segment	xlrec;
		XLogRecPtr	lsn;

		xlrec.segno = segno;
		xlrec.is_extend = is_extend;

		XLogBeginInsert();
		XLogRegisterBlock(0, rnode, fork, CONVEYOR_METAPAGE, metapage,
						  REGBUF_STANDARD);
		if (fsmblock != InvalidBlockNumber)
			XLogRegisterBlock(1, rnode, fork, fsmblock,
							  BufferGetPage(fsmbuffer),
							  REGBUF_STANDARD);
		XLogRegisterData((char *) &xlrec, sizeof(xlrec));
		lsn = XLogInsert(RM_CONVEYOR_ID,
						 XLOG_CONVEYOR_ALLOCATE_PAYLOAD_SEGMENT);

		PageSetLSN(metapage, lsn);
	}

	END_CRIT_SECTION();
}

/*
 * Allocate a new index segment, and optionally write XLOG for the change.
 *
 * 'metabuffer' should be the buffer containing the metapage.
 *
 * 'indexblock' and 'indexbuffer' should be the block number and buffer for
 * the first page of the index segment.
 *
 * If any index segments already exist, then 'prevblock' should be the
 * block number of the first page of the last index segment that already
 * exists, and 'prevbuffer' the corresponding buffer; otherwise, use
 * InvalidBlockNumber and InvalidBuffer, respectively.
 *
 * Similarly, if the allocation status of the segment is tracked in an
 * FSM page, 'fsmblock' and 'fsmbuffer' should reference that page; if that
 * information is tracked in the metpaage, the InvalidBlockNumber and
 * InvalidBuffer.
 *
 * 'segno' is the segment number of the new index segment, and 'pageno'
 * is the first logical page for which it will store index information.
 */
void
cb_allocate_index_segment(RelFileNode *rnode,
						  ForkNumber fork,
						  Buffer metabuffer,
						  BlockNumber indexblock,
						  Buffer indexbuffer,
						  BlockNumber prevblock,
						  Buffer prevbuffer,
						  BlockNumber fsmblock,
						  Buffer fsmbuffer,
						  CBSegNo segno,
						  CBPageNo pageno,
						  bool needs_xlog)
{
	Page		metapage;
	Page		indexpage;
	CBMetapageData *meta;

	metapage = BufferGetPage(metabuffer);
	indexpage = BufferGetPage(indexbuffer);

	meta = cb_metapage_get_special(metapage);

	START_CRIT_SECTION();

	cb_metapage_add_index_segment(meta, segno);
	MarkBufferDirty(metabuffer);

	cb_indexpage_initialize(indexpage, pageno, true);
	MarkBufferDirty(indexbuffer);

	if (prevblock != InvalidBlockNumber)
	{
		cb_indexpage_set_next_segment(BufferGetPage(prevbuffer), segno);
		MarkBufferDirty(prevbuffer);
	}

	if (fsmblock != InvalidBlockNumber)
	{
		cb_fsmpage_set_fsm_bit(BufferGetPage(fsmbuffer), segno, true);
		MarkBufferDirty(fsmbuffer);
	}
	else
		cb_metapage_set_fsm_bit(meta, segno, true);

	if (needs_xlog)
	{
		/* XXX write xlog, set LSNs */
	}

	END_CRIT_SECTION();
}
