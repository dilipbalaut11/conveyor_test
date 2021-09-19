/*-------------------------------------------------------------------------
 *
 * cbindexpage.h
 *	  APIs for accessing conveyor belt index pages.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbindexpage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBINDEXPAGE_H
#define CBINDEXPAGE_H

#include "access/cbdefs.h"
#include "storage/bufpage.h"

/*
 * Number of index page bytes reserved for fixed-size data.
 *
 * This needs to be at least large enough to hold a PageHeader plus the
 * non-array fields in CBIndexPageData. We make it comfortably larger than
 * that in case we ever want to enlarge CBIndexPageData.
 */
#define		CB_INDEXPAGE_RESERVE_BYTES			128

/*
 * Number of index entries per index page.
 */
#define		CB_INDEXPAGE_INDEX_ENTRIES \
		((BLCKSZ - CB_INDEXPAGE_RESERVE_BYTES) / sizeof(CBSegNo))

/*
 * Function prototypes.
 */
extern void cb_indexpage_initialize(Page page, CBPageNo pageno,
									bool first_in_segment);
extern BlockNumber cb_indexpage_find_logical_page(Page page,
												  CBPageNo pageno,
												  uint16 pages_per_segment);
extern void cb_indexpage_add_index_entry(Page page, CBPageNo pageno,
										 CBSegNo segno,
										 uint16 pages_per_segment);
extern void cb_indexpage_set_next_segment(Page page, CBSegNo segno);
extern CBSegNo cb_indexpage_get_next_segment(Page page);
extern void cb_indexpage_increment_pages_initalized(Page page);
extern void cb_indexpage_decrement_pages_initalized(Page page);

/*
 * How many index entries will fit into an index segment?
 */
static inline unsigned
cb_index_entries_per_index_segment(uint16 pages_per_segment)
{
	return CB_INDEXPAGE_INDEX_ENTRIES * (unsigned) pages_per_segment;
}

/*
 * How many logical pages can we map using a single index segment?
 */
static inline unsigned
cb_logical_pages_per_index_segment(uint16 pages_per_segment)
{
	return cb_index_entries_per_index_segment(pages_per_segment)
		* (unsigned) pages_per_segment;
}

/*
 * How many logical pages can we map using a single index page?
 */
static inline unsigned
cb_logical_pages_per_index_page(uint16 pages_per_segment)
{
	return CB_INDEXPAGE_INDEX_ENTRIES * (unsigned) pages_per_segment;
}

#endif							/* CBINDEXPAGE_H */
