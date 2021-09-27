/*-------------------------------------------------------------------------
 *
 * cbindexpage.c
 *	  APIs for accessing conveyor belt index pages.
 *
 * Similar to cbmetapage.c, this file abstracts accesses to conveyor
 * belt index pages, and should be the only backend code that understands
 * their internal structure.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/backend/access/conveyor/cbindexpage.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/cbfsmpage.h"
#include "access/cbindexpage.h"
#include "access/cbindexpage_format.h"

static CBIndexPageData *cb_indexpage_get_special(Page page);

/*
 * Initialize an index page.
 *
 * If this is the first page in a new index segment, it has to be the newest
 * segment, so there's no next segment yet. And there's never a next segment
 * for a page that is not the first one in the segment.
 */
void
cb_indexpage_initialize(Page page, CBPageNo pageno, bool first_in_segment)
{
	CBIndexPageData *ipd;
	int		i;

	PageInit(page, BLCKSZ, sizeof(CBIndexPageData));
	ipd = (CBIndexPageData *) PageGetSpecialPointer(page);
	ipd->cbidx_magic = CB_INDEXPAGE_MAGIC;
	ipd->cbidx_pages_initialized = first_in_segment ? 1 : 0;
	ipd->cbidx_next_segment = CB_INVALID_SEGMENT;
	ipd->cbidx_first_page = pageno;

	for (i = 0; i < CB_INDEXPAGE_INDEX_ENTRIES; ++i)
		ipd->cbidx_entry[i] = CB_INVALID_SEGMENT;
}

/*
 * Figure out where a certain logical page is physically located.
 *
 * It is the caller's responsibility to supply the correct index page.
 */
BlockNumber
cb_indexpage_find_logical_page(Page page, CBPageNo pageno,
							   uint16 pages_per_segment)
{
	CBIndexPageData *ipd = cb_indexpage_get_special(page);
	unsigned	offset;
	CBSegNo		segno;

	if (pageno < ipd->cbidx_first_page)
		elog(ERROR, "can't find index entry for page " UINT64_FORMAT " on an index page that starts at page " UINT64_FORMAT,
			 pageno, ipd->cbidx_first_page);
	offset = (pageno - ipd->cbidx_first_page) / pages_per_segment;
	if (offset > CB_INDEXPAGE_INDEX_ENTRIES)
		elog(ERROR, "can't find index entry for page " UINT64_FORMAT " on an index page that starts at page " UINT64_FORMAT,
			 pageno, ipd->cbidx_first_page);
	segno = ipd->cbidx_entry[offset];
	if (segno == CB_INVALID_SEGMENT)
		elog(ERROR, "no index entry for page " INT64_FORMAT, pageno);

	return cb_segment_to_block(pages_per_segment, segno,
							   pageno % pages_per_segment);
}

/*
 * Add index entries for logical pages beginning at 'pageno'.
 *
 * It is the caller's responsibility to supply the correct index page, and
 * to make sure that there is enough room for the entries to be added.
 */
void
cb_indexpage_add_index_entries(Page page,
							   CBPageNo pageno,
							   unsigned num_index_entries,
							   CBSegNo *index_entries,
							   uint16 pages_per_segment)
{
	CBIndexPageData *ipd = cb_indexpage_get_special(page);
	unsigned	offset;

	if (num_index_entries < 1 || num_index_entries > CB_INDEXPAGE_INDEX_ENTRIES)
		elog(ERROR, "can't add %u index entries to an index page",
			 num_index_entries);
	if (pageno < ipd->cbidx_first_page)
		elog(ERROR, "can't add index entries starting with page " UINT64_FORMAT " to an index page that starts at page " UINT64_FORMAT,
			 pageno, ipd->cbidx_first_page);
	offset = (pageno - ipd->cbidx_first_page) / pages_per_segment;
	if (offset + num_index_entries >= CB_INDEXPAGE_INDEX_ENTRIES)
		elog(ERROR, "can't place %u index entries starting with page " UINT64_FORMAT " on an index page that starts at page " UINT64_FORMAT,
			 num_index_entries, pageno, ipd->cbidx_first_page);

	memcpy(&ipd->cbidx_entry[offset], index_entries,
		   num_index_entries * sizeof(CBSegNo));
}

/*
 * Set the next index segment.
 *
 * This should only be used on the first page of an index segment, since
 * that's where the next segment number is stored.
 */
void
cb_indexpage_set_next_segment(Page page, CBSegNo segno)
{
	CBIndexPageData *ipd = cb_indexpage_get_special(page);

	if (ipd->cbidx_pages_initialized == 0)
		elog(ERROR, "only the first page of a segment can have a next segment");

	ipd->cbidx_next_segment = segno;
}

/*
 * Get the next index segment.
 *
 * This should only be used on the first page of an index segment, since
 * that's where the next segment number is stored.
 */
CBSegNo
cb_indexpage_get_next_segment(Page page)
{
	CBIndexPageData *ipd = cb_indexpage_get_special(page);

	if (ipd->cbidx_pages_initialized == 0)
		elog(ERROR, "only the first page of a segment can have a next segment");

	return ipd->cbidx_next_segment;
}

/*
 * Increment our count of the number of pages of an index segment that have
 * been initialized.
 *
 * This should only be used on the first page of an index segment, since
 * that's where the count is stored.
 */
void
cb_indexpage_increment_pages_initialized(Page page)
{
	CBIndexPageData *ipd = cb_indexpage_get_special(page);

	if (ipd->cbidx_pages_initialized == 0)
		elog(ERROR, "only the first page of a segment can have a next segment");

	ipd->cbidx_pages_initialized++;
}

/*
 * Decrement our count of the number of pages of an index segment that have
 * been initialized.
 *
 * This should only be used on the first page of an index segment, since
 * that's where the count is stored.
 */
void
cb_indexpage_decrement_pages_initialized(Page page)
{
	CBIndexPageData *ipd = cb_indexpage_get_special(page);

	if (ipd->cbidx_pages_initialized == 0)
		elog(ERROR, "only the first page of a segment can have a next segment");
	if (ipd->cbidx_pages_initialized == 1)
		elog(ERROR, "number of initialized pages is already down to one");

	ipd->cbidx_pages_initialized--;
}

/*
 * Given a page that is known to be a conveyor belt free space map page,
 * return a pointer to the CBFSMPageData, after checking the magic number.
 */
static CBIndexPageData *
cb_indexpage_get_special(Page page)
{
	CBIndexPageData *ipd = (CBIndexPageData *) PageGetSpecialPointer(page);

	if (ipd->cbidx_magic != CB_INDEXPAGE_MAGIC)
		elog(ERROR, "bad magic number in conveyor belt index page: %08X",
			 ipd->cbidx_magic);

	return ipd;
}
