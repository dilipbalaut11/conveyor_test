/*-------------------------------------------------------------------------
 *
 * cbindexpage_format.h
 *	  Actual on-disk format for a conveyor-belt index page.
 *
 * Backend code should not typically include this file directly, even if
 * it's code that is part of the conveyor belt implemenation. Instead, it
 * should use the interface routines defined in cbindexpage.h.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbindexpage_format.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBINDEXPAGE_FORMAT_H
#define CBINDEXPAGE_FORMAT_H

#include "access/cbindexpage.h"

/* Magic number for the index page. */
#define CB_INDEXPAGE_MAGIC	0x62334c54

/*
 * A conveyor belt index page will store a struct of this type in the page's
 * special space.
 */
typedef struct CBIndexPageData
{
	/* Always CB_INDEXPAGE_MAGIC. */
	uint32		cbidx_magic;

	/*
	 * If this is the first page of an index segment, then this is the number
	 * of pages in this segment that are initialized, at least 1 and up to
	 * the number of pages per segment for this conveyor belt. If this is
	 * not the first page of the index segment, then 0.
	 */
	uint16		cbidx_pages_initialized;

	/*
	 * If this is the first page of an index segment and there is at least one
	 * index segment after this one, then this is the segment number of the
	 * next such segment. Otherwise, it's CB_INVALID_SEGMENT.
	 */
	CBSegNo		cbidx_next_segment;

	/*
	 * The first logical page number of the first segment whose index entry
	 * is stored on this page. Technically this isn't required, but it seems
	 * good to have for sanity checks.
	 */
	CBPageNo	cbidx_first_page;

	/* The actual index entries stored on this page. */
	CBSegNo		cbidx_entry[CB_INDEXPAGE_INDEX_ENTRIES];
} CBIndexPageData;

#endif
