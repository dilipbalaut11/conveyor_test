/*-------------------------------------------------------------------------
 *
 * cbxlog.c
 *	  XLOG support for conveyor belts.
 *
 * For each REDO function in this file, see cbmodify.c for the
 * corresponding function that performs the modification during normal
 * running and logs the record that we REDO here.
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
#include "access/cbxlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"

/*
 * REDO function for cb_insert_payload_page.
 *
 * Note that the handling of block 1 is very similar to XLOG_FPI.
 */
static void
cb_xlog_insert_payload_page(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		metabuffer;
	Buffer		payloadbuffer;

	if (!XLogRecHasBlockImage(record, 1))
		elog(ERROR, "XLOG_CONVEYOR_INSERT_PAYLOAD_PAGE record did not contain full page image of payload block");
	if (XLogReadBufferForRedo(record, 1, &payloadbuffer) != BLK_RESTORED)
		elog(ERROR, "unexpected XLogReadBufferForRedo result when restoring backup block");

	/* last due to lock ordering rules; see README */
	if (XLogReadBufferForRedo(record, 0, &metabuffer) == BLK_NEEDS_REDO)
	{
		Page	metapage = BufferGetPage(metabuffer);
		CBMetapageData *meta;
		BlockNumber	payloadblock;

		meta = cb_metapage_get_special(metapage);
		XLogRecGetBlockTag(record, 1, NULL, NULL, &payloadblock);
		cb_metapage_advance_next_logical_page(meta, payloadblock);
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuffer);
	}

	UnlockReleaseBuffer(metabuffer);
	UnlockReleaseBuffer(payloadbuffer);
}

/*
 * REDO function for cb_allocate_payload_segment.
 */
static void
cb_xlog_allocate_payload_segment(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_cb_allocate_payload_segment *xlrec;
	Buffer		metabuffer;
	bool		have_fsm_page = XLogRecGetBlockTag(record, 1, NULL, NULL, NULL);
	Buffer		fsmbuffer = InvalidBuffer;

	xlrec = (xl_cb_allocate_payload_segment *) XLogRecGetData(record);

	if (have_fsm_page &&
		XLogReadBufferForRedo(record, 1, &fsmbuffer) == BLK_NEEDS_REDO)
	{
		Page	fsmpage = BufferGetPage(fsmbuffer);

		cb_fsmpage_set_fsm_bit(fsmpage, xlrec->segno, true);
		PageSetLSN(fsmpage, lsn);
		MarkBufferDirty(fsmbuffer);
	}

	/* last due to lock ordering rules; see README */
	if (XLogReadBufferForRedo(record, 0, &metabuffer) == BLK_NEEDS_REDO)
	{
		Page	metapage = BufferGetPage(metabuffer);
		CBMetapageData *meta;

		meta = cb_metapage_get_special(metapage);
		cb_metapage_add_index_entry(meta, xlrec->segno);
		if (xlrec->is_extend)
			cb_metapage_increment_next_segment(meta, xlrec->segno);
		if (!have_fsm_page)
			cb_metapage_set_fsm_bit(meta, xlrec->segno, true);
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuffer);
	}

	if (BufferIsValid(metabuffer))
		UnlockReleaseBuffer(metabuffer);
	if (BufferIsValid(fsmbuffer))
		UnlockReleaseBuffer(fsmbuffer);
}

/*
 * REDO function for cb_allocate_index_segment.
 */
static void
cb_xlog_allocate_index_segment(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_cb_allocate_index_segment *xlrec;
	bool		have_prev_page;
	bool		have_fsm_page;
	Buffer		metabuffer;
	Buffer		indexbuffer;
	Buffer		prevbuffer = InvalidBuffer;
	Buffer		fsmbuffer = InvalidBuffer;
	Page		indexpage;

	have_prev_page = XLogRecGetBlockTag(record, 2, NULL, NULL, NULL);
	have_fsm_page = XLogRecGetBlockTag(record, 3, NULL, NULL, NULL);

	xlrec = (xl_cb_allocate_index_segment *) XLogRecGetData(record);

	indexbuffer = XLogInitBufferForRedo(record, 1);
	indexpage = BufferGetPage(indexbuffer);
	cb_indexpage_initialize(indexpage, xlrec->pageno);
	PageSetLSN(indexpage, lsn);
	MarkBufferDirty(indexbuffer);

	if (have_prev_page &&
		XLogReadBufferForRedo(record, 2, &prevbuffer) == BLK_NEEDS_REDO)
	{
		Page	prevpage = BufferGetPage(prevbuffer);

		cb_indexpage_set_next_segment(prevpage, xlrec->segno);
		PageSetLSN(prevpage, lsn);
		MarkBufferDirty(prevbuffer);
	}

	if (have_fsm_page &&
		XLogReadBufferForRedo(record, 3, &fsmbuffer) == BLK_NEEDS_REDO)
	{
		Page	fsmpage = BufferGetPage(fsmbuffer);

		cb_fsmpage_set_fsm_bit(fsmpage, xlrec->segno, true);
		PageSetLSN(fsmpage, lsn);
		MarkBufferDirty(fsmbuffer);
	}

	/* last due to lock ordering rules; see README */
	if (XLogReadBufferForRedo(record, 0, &metabuffer) == BLK_NEEDS_REDO)
	{
		Page	metapage = BufferGetPage(metabuffer);
		CBMetapageData *meta;

		meta = cb_metapage_get_special(metapage);
		cb_metapage_add_index_segment(meta, xlrec->segno);
		if (xlrec->is_extend)
			cb_metapage_increment_next_segment(meta, xlrec->segno);
		if (!have_fsm_page)
			cb_metapage_set_fsm_bit(meta, xlrec->segno, true);
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuffer);
	}

	if (BufferIsValid(metabuffer))
		UnlockReleaseBuffer(metabuffer);
	if (BufferIsValid(indexbuffer))
		UnlockReleaseBuffer(indexbuffer);
	if (BufferIsValid(prevbuffer))
		UnlockReleaseBuffer(prevbuffer);
	if (BufferIsValid(fsmbuffer))
		UnlockReleaseBuffer(fsmbuffer);
}

/*
 * REDO function for cb_allocate_index_page.
 */
static void
cb_xlog_allocate_index_page(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_cb_allocate_index_page *xlrec;
	Buffer		indexbuffer;
	Page		indexpage;

	xlrec = (xl_cb_allocate_index_page *) XLogRecGetData(record);

	indexbuffer = XLogInitBufferForRedo(record, 0);
	indexpage = BufferGetPage(indexbuffer);
	cb_indexpage_initialize(indexpage, xlrec->pageno);
	PageSetLSN(indexpage, lsn);
	MarkBufferDirty(indexbuffer);

	UnlockReleaseBuffer(indexbuffer);
}

/*
 * REDO function for cb_relocate_index_entries.
 */
static void
cb_xlog_relocate_index_entries(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_cb_relocate_index_entries *xlrec;
	Buffer		metabuffer;
	Buffer		indexbuffer;
	ReadBufferMode	mode;

	xlrec = (xl_cb_relocate_index_entries *) XLogRecGetData(record);

	mode = xlrec->pageoffset == 0 ? RBM_ZERO_AND_LOCK : RBM_NORMAL;
	if (XLogReadBufferForRedoExtended(record, 1, mode, false,
									  &indexbuffer) == BLK_NEEDS_REDO)
	{
		Page	indexpage = BufferGetPage(indexbuffer);

		if (xlrec->pageoffset == 0)
			cb_indexpage_initialize(indexpage, xlrec->index_page_start);

		cb_indexpage_add_index_entries(indexpage, xlrec->pageoffset,
									   xlrec->num_index_entries,
									   xlrec->index_entries);
		PageSetLSN(indexpage, lsn);
		MarkBufferDirty(indexbuffer);
	}

	/* NB: metapage must be last due to lock ordering rules */
	if (XLogReadBufferForRedo(record, 0, &metabuffer) == BLK_NEEDS_REDO)
	{
		Page	metapage = BufferGetPage(metabuffer);
		CBMetapageData *meta;

		meta = cb_metapage_get_special(metapage);
		cb_metapage_remove_index_entries(meta, xlrec->num_index_entries, true);
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuffer);
	}

	if (BufferIsValid(metabuffer))
		UnlockReleaseBuffer(metabuffer);
	if (BufferIsValid(indexbuffer))
		UnlockReleaseBuffer(indexbuffer);
}

/*
 * REDO function for cb_logical_truncate.
 */
static void
cb_xlog_logical_truncate(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_cb_logical_truncate *xlrec;
	Buffer		metabuffer;

	xlrec = (xl_cb_logical_truncate *) XLogRecGetData(record);

	if (XLogReadBufferForRedo(record, 0, &metabuffer) == BLK_NEEDS_REDO)
	{
		Page	metapage = BufferGetPage(metabuffer);
		CBMetapageData *meta;

		meta = cb_metapage_get_special(metapage);
		cb_metapage_advance_oldest_logical_page(meta, xlrec->oldest_keeper);
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuffer);
	}

	if (BufferIsValid(metabuffer))
		UnlockReleaseBuffer(metabuffer);
}

/*
 * Main entrypoint for conveyor belt REDO.
 */
void
conveyor_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_CONVEYOR_INSERT_PAYLOAD_PAGE:
			cb_xlog_insert_payload_page(record);
			break;
		case XLOG_CONVEYOR_ALLOCATE_PAYLOAD_SEGMENT:
			cb_xlog_allocate_payload_segment(record);
			break;
		case XLOG_CONVEYOR_ALLOCATE_INDEX_SEGMENT:
			cb_xlog_allocate_index_segment(record);
			break;
		case XLOG_CONVEYOR_ALLOCATE_INDEX_PAGE:
			cb_xlog_allocate_index_page(record);
			break;
		case XLOG_CONVEYOR_RELOCATE_INDEX_ENTRIES:
			cb_xlog_relocate_index_entries(record);
			break;
		case XLOG_CONVEYOR_LOGICAL_TRUNCATE:
			cb_xlog_logical_truncate(record);
			break;
		default:
			elog(PANIC, "conveyor_redo: unknown op code %u", info);
	}
}
