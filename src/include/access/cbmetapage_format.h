/*-------------------------------------------------------------------------
 *
 * cbmetapage_format.h
 *	  Actual on-disk format for a conveyor-belt metapage.
 *
 * Backend code should not typically include this file directly, even if
 * it's code that is part of the conveyor belt implemenation. Instead, it
 * should use the interface routines defined in cbmetapage.h.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbmetapage_format.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBMETAPAGE_FORMAT_H
#define CBMETAPAGE_FORMAT_H

#include "access/cbmetapage.h"

/* Magic number for the metapage. */
#define	CB_METAPAGE_MAGIC	0x43304e76

/* Conveyor belt metapage version. */
#define CBM_VERSION	1

/*
 * Conveyor belt metapage.
 */
struct CBMetapageData
{
	/*
	 * Basic information.
	 *
	 * cbm_magic should always be CB_METAPAGE_MAGIC, and cbm_version should
	 * always be CB_VERSION (or an older, supported version greater than or
	 * equal to 1, but right now 1 is the current version).
	 *
	 * cbm_pages_per_segment is the number of pages per segment. Making this
	 * larger reduces the number of index and freespace map segments required
	 * and decreases fragmentation at the storage level, but it also increases
	 * the granularity of space reuse.
	 *
	 * cbm_flags stores flags. Currently, the only flag is
	 * CBM_FLAG_INDEX_SEGMENT_FULL.
	 */
	uint32		cbm_magic;
	uint32		cbm_version;
	uint16		cbm_pages_per_segment;
	uint16		cbm_flags;

	/*
	 * Logical start and end of the conveyor belt.
	 *
	 * cbm_oldest_logical_page is the smallest logical page number that has
	 * not yet been truncated away. The conveyor belt is free to remove older
	 * data or recycle the pages for new data, but doesn't necessarily do so
	 * immediately.
	 *
	 * cbm_next_logical_page is the smallest logical page number that has not
	 * yet been allocated.
	 */
	CBPageNo	cbm_oldest_logical_page;
	CBPageNo	cbm_next_logical_page;

	/*
	 * Information for logical-to-physical indexing.
	 *
	 * cbm_index_start is the oldest logical page number for which we might
	 * still have a logical-to-physical mapping. It can be older than
	 * cbm_oldest_logical_page if we haven't thrown away all the old data yet,
	 * but it can't be newer.
	 *
	 * cbm_index_metapage_start is the oldest logical page number whose
	 * logical-to-physical mapping, if it exists, is stored in the metapage.
	 * It cannot be smaller than cbm_index_start.
	 *
	 * cbm_oldest_index_segment and cbm_newest_index_segment are the oldest
	 * and newest index segments that exist. Both values will be
	 * CB_INVALID_SEGMENT if there are no index segments. Otherwise, the
	 * mapping for cbm_oldest_logical_page is stored in the first entry in the
	 * first page of cbm_oldest_index_segment.
	 *
	 * Note that the end of the newest index segment will often be unused, and
	 * its contents undefined. That's because new index entries are always
	 * inserted directly into the metapage, and later moved into index
	 * segments in bulk.
	 *
	 * cbm_index_segments_moved is the total number of times in the history
	 * of this conveyor belt that an index segment has been physically
	 * moved to a different segment number. This helps backends to know
	 * whether their cached notions of where index entries for particular
	 * logical pages are located are still valid.
	 *
	 * cbm_next_segment is the lowest-numbered segment that does not yet
	 * exist.
	 */
	CBPageNo	cbm_index_start;
	CBPageNo	cbm_index_metapage_start;
	CBSegNo		cbm_oldest_index_segment;
	CBSegNo		cbm_newest_index_segment;
	uint64		cbm_index_segments_moved;
	CBSegNo		cbm_next_segment;

	/*
	 * In-metapage portion of index and freespace map.
	 */
	CBSegNo		cbm_index[CB_METAPAGE_INDEX_ENTRIES];
	uint8		cbm_freespace_map[CB_METAPAGE_FREESPACE_BYTES];
};

/*
 * Conveyor belt metapage flags.
 *
 * CBM_FLAG_INDEX_SEGMENT_FULL indicates that there is no room in the
 * newest index segment for any more index entries.
 */
#define	CBM_FLAG_INDEX_SEGMENT_FULL			0x0001

#endif
