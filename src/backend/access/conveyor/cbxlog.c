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

	if (!XLogRecHasBlockImage(record, 1))
		elog(ERROR, "XLOG_CONVEYOR_INSERT_PAYLOAD_PAGE record did not contain full page image of payload block");
	if (XLogReadBufferForRedo(record, 1, &payloadbuffer) != BLK_RESTORED)
		elog(ERROR, "unexpected XLogReadBufferForRedo result when restoring backup block");

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

	if (have_fsm_page &&
		XLogReadBufferForRedo(record, 1, &fsmbuffer) == BLK_NEEDS_REDO)
	{
		Page	fsmpage = BufferGetPage(fsmbuffer);

		cb_fsmpage_set_fsm_bit(fsmpage, xlrec->segno, true);
		PageSetLSN(fsmpage, lsn);
		MarkBufferDirty(fsmbuffer);
	}

	if (BufferIsValid(metabuffer))
		UnlockReleaseBuffer(metabuffer);
	if (BufferIsValid(fsmbuffer))
		UnlockReleaseBuffer(fsmbuffer);
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
		default:
			elog(PANIC, "conveyor_redo: unknown op code %u", info);
	}
}
