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
		case XLOG_CONVEYOR_ALLOCATE_PAYLOAD_SEGMENT:
			{
				xl_cb_allocate_payload_segment *xlrec;

				xlrec = (xl_cb_allocate_payload_segment *) rec;

				appendStringInfo(buf, "segno %u", xlrec->segno);
				break;
			}
	}
}

extern const char *
conveyor_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & XLR_INFO_MASK)
	{
		case XLOG_CONVEYOR_ALLOCATE_PAYLOAD_SEGMENT:
			id = "ALLOCATE_PAYLOAD_SEGMENT";
			break;
	}

	return id;
}
