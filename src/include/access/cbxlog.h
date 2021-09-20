/*-------------------------------------------------------------------------
 *
 * cbxlog.h
 *	  XLOG support for conveyor belts.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbxlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBXLOG_H
#define CBXLOG_H

#include "access/cbdefs.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"

#define XLOG_CONVEYOR_INSERT_PAYLOAD_PAGE			0x10
#define	XLOG_CONVEYOR_ALLOCATE_PAYLOAD_SEGMENT		0x20
#define	XLOG_CONVEYOR_ALLOCATE_INDEX_SEGMENT		0x30

typedef struct xl_cb_allocate_payload_segment
{
	CBSegNo		segno;
	bool		is_extend;
} xl_cb_allocate_payload_segment;

typedef struct xl_cb_allocate_index_segment
{
	CBSegNo		segno;
	CBPageNo	pageno;
	bool		is_extend;
} xl_cb_allocate_index_segment;

extern void conveyor_desc(StringInfo buf, XLogReaderState *record);
extern void conveyor_redo(XLogReaderState *record);
extern const char *conveyor_identify(uint8 info);

#endif							/* CBXLOG_H */
