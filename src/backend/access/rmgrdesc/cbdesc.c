/*-------------------------------------------------------------------------
 *
 * cbdesc.c
 *	  rmgr descriptor routines for access/conveyor/cbxlog.c
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/cbdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/cbxlog.h"

extern void
conveyor_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_CONVEYOR_INSERT_PAYLOAD_PAGE:
			{
				/* Nothing extra to print. */
				break;
			}

		case XLOG_CONVEYOR_ALLOCATE_PAYLOAD_SEGMENT:
			{
				xl_cb_allocate_payload_segment *xlrec;

				xlrec = (xl_cb_allocate_payload_segment *) rec;

				appendStringInfo(buf, "segno %u is_extend %d",
								 xlrec->segno, xlrec->is_extend ? 1 : 0);
				break;
			}


		case XLOG_CONVEYOR_ALLOCATE_INDEX_SEGMENT:
			{
				xl_cb_allocate_index_segment *xlrec;

				xlrec = (xl_cb_allocate_index_segment *) rec;

				appendStringInfo(buf, "segno %u pageno " UINT64_FORMAT " is_extend %d",
								 xlrec->segno, xlrec->pageno,
								 xlrec->is_extend ? 1 : 0);
				break;
			}


		case XLOG_CONVEYOR_RELOCATE_INDEX_ENTRIES:
			{
				xl_cb_relocate_index_entries *xlrec;
				unsigned	i;

				xlrec = (xl_cb_relocate_index_entries *) rec;

				appendStringInfo(buf, "pageoffset %u num_index_entries %u index_page_start " UINT64_FORMAT,
								 xlrec->pageoffset, xlrec->num_index_entries,
								 xlrec->index_page_start);
				for (i = 0; i < xlrec->num_index_entries; ++i)
				{
					if (i == 0)
						appendStringInfoString(buf, " entries");
					appendStringInfo(buf, " %u", xlrec->index_entries[i]);
				}
				break;
			}

		case XLOG_CONVEYOR_LOGICAL_TRUNCATE:
			{
				xl_cb_logical_truncate *xlrec;

				xlrec = (xl_cb_logical_truncate *) rec;

				appendStringInfo(buf, "oldest_keeper " UINT64_FORMAT,
								 xlrec->oldest_keeper);
				break;
			}
	}
}

extern const char *
conveyor_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_CONVEYOR_INSERT_PAYLOAD_PAGE:
			id = "INSERT_PAYLOAD_PAGE";
			break;
		case XLOG_CONVEYOR_ALLOCATE_PAYLOAD_SEGMENT:
			id = "ALLOCATE_PAYLOAD_SEGMENT";
			break;
		case XLOG_CONVEYOR_ALLOCATE_INDEX_SEGMENT:
			id = "ALLOCATE_INDEX_SEGMENT";
			break;
		case XLOG_CONVEYOR_ALLOCATE_INDEX_PAGE:
			id = "ALLOCATE_INDEX_PAGE";
			break;
		case XLOG_CONVEYOR_RELOCATE_INDEX_ENTRIES:
			id = "RELOCATE_INDEX_ENTRIES";
			break;
		case XLOG_CONVEYOR_LOGICAL_TRUNCATE:
			id = "LOGICAL_TRUNCATE";
			break;
	}

	return id;
}
