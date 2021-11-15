/*-------------------------------------------------------------------------
 *
 * cbmetapage.c
 *	  APIs for accessing conveyor belt metapages.
 *
 * The goal of this file is to provide a set of functions that can be
 * used to perform all necessary access to or modification of a conveyor
 * belt metapage. The functions in this file should be the only backend
 * code that knows about the actual organization of CBMetapageData,
 * but they shouldn't know about the internals of other types of pages
 * (like index segment or freespace map pages) nor should they know
 * about buffers or locking.
 *
 * Much - but not all - of the work done here is sanity checking. We
 * do this partly to catch bugs, and partly as a defense against the
 * possibility that the metapage is corrupted on disk. Because of the
 * latter possibility, most of these checks use an elog(ERROR) rather
 * than just Assert.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/backend/access/conveyor/cbmetapage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/cbfsmpage.h"
#include "access/cbindexpage.h"
#include "access/cbmetapage.h"
#include "access/cbmetapage_format.h"

/*
 * Initialize metapage.
 */
void
cb_metapage_initialize(Page page, uint16 pages_per_segment)
{
	CBMetapageData *meta;
	int			i;

	PageInit(page, BLCKSZ, sizeof(CBMetapageData));
	meta = (CBMetapageData *) PageGetSpecialPointer(page);
	meta->cbm_magic = CB_METAPAGE_MAGIC;
	meta->cbm_version = CBM_VERSION;
	meta->cbm_pages_per_segment = pages_per_segment;

	/*
	 * PageInit has already zeroed the page, so we only need to initialize any
	 * fields that need to be non-zero. Everything of type CBPageNo and all of
	 * the freespace map should start out as 0, but most of the fields of
	 * CBSegNo fields need to be set to CB_INVALID_SEGMENT.
	 */
	meta->cbm_oldest_index_segment = CB_INVALID_SEGMENT;
	meta->cbm_newest_index_segment = CB_INVALID_SEGMENT;
	for (i = 0; i < CB_METAPAGE_INDEX_ENTRIES; ++i)
		meta->cbm_index[i] = CB_INVALID_SEGMENT;
}

/*
 * Given a page that is known to be a conveyor belt metapage, return a
 * pointer to the CBMetapageData.
 *
 * We take the opportunity to perform some basic sanity checks here.
 */
CBMetapageData *
cb_metapage_get_special(Page page)
{
	CBMetapageData *meta = (CBMetapageData *) PageGetSpecialPointer(page);

	if (meta->cbm_magic != CB_METAPAGE_MAGIC)
		elog(ERROR, "bad magic number in conveyor belt metapage: %08X",
			 meta->cbm_magic);
	if (meta->cbm_version != CBM_VERSION)
		elog(ERROR, "bad version in conveyor belt metapage: %08X",
			 meta->cbm_version);
	if (meta->cbm_pages_per_segment == 0)
		elog(ERROR, "conveyor belt may not have zero pages per segment");

	return meta;
}

/*
 * Deduce what we can about the physical location of a logical page.
 *
 * If the logical page precedes the logical truncation point, returns false.
 * Otherwise, returns true.
 *
 * If the physical location of the block can be computed based on the data
 * in the metapage, sets *blkno to the appropriate block number. Otherwise,
 * sets *blkno to InvalidBlockNumber.
 */
bool
cb_metapage_find_logical_page(CBMetapageData *meta,
							  CBPageNo pageno,
							  BlockNumber *blkno)
{
	CBPageNo	relp;
	CBSegNo		segno;
	unsigned	segoff;

	/* Physical location unknown, unless we later discover otherwise. */
	*blkno = InvalidBlockNumber;

	/* Is it too old to be accessible? */
	if (pageno < meta->cbm_oldest_logical_page)
		return false;

	/* Is it too old to have an index entry in the metapage? */
	if (pageno < meta->cbm_index_metapage_start)
	{
		/* Index entry exists, but not on metapage. */
		return true;
	}

	/* Is it too new to have an index entry? */
	relp = pageno - meta->cbm_index_metapage_start;
	if (relp >= CB_METAPAGE_INDEX_ENTRIES * meta->cbm_pages_per_segment)
		return false;

	/* Index entry must be in the metapage, if it exists at all. */
	segno = meta->cbm_index[relp / meta->cbm_pages_per_segment];
	segoff = relp % meta->cbm_pages_per_segment;
	if (segno == CB_INVALID_SEGMENT)
		return false;

	/* Location identified! */
	*blkno = cb_segment_to_block(meta->cbm_pages_per_segment, segno, segoff);
	return true;
}

/*
 * Tell the caller what needs to be done to insert a page.
 *
 * Regardless of the return value, *next_pageno and *next_segno will be
 * set to the lowest-numbered logical page that is not allocated and the
 * lowest segment number that is not allocated, respectively. In addition,
 * *index_start will be set to the first logical page number covered by the
 * index, *index_metapage_start to the first logical page number covered by
 * the metapage portion of the index, and *newest_index_segment to the segment
 * number of the newest index segment, or CB_INVALID_SEGMENT if there is none.
 *
 * If the return value is CBM_INSERT_OK, *blkno will be set to the block number
 * of the first unused page in the unfilled payload segment.
 *
 * If the return value is CBM_INSERT_NEEDS_INDEX_ENTRIES_RELOCATED, *blkno
 * will be set to the first not-entirely-filled page in the newest index
 * segment.
 */
CBMInsertState
cb_metapage_get_insert_state(CBMetapageData *meta,
							 BlockNumber *blkno,
							 CBPageNo *next_pageno,
							 CBSegNo *next_segno,
							 CBPageNo *index_start,
							 CBPageNo *index_metapage_start,
							 CBSegNo *newest_index_segment)
{
	CBPageNo	relp;
	CBSegNo		segno;
	unsigned	segoff;

	/* Set the values that we return unconditionally. */
	*next_pageno = meta->cbm_next_logical_page;
	*next_segno = meta->cbm_next_segment;
	*index_start = meta->cbm_index_start;
	*index_metapage_start = meta->cbm_index_metapage_start;
	*newest_index_segment = meta->cbm_newest_index_segment;

	/* Compute next logical page number relative to start of metapage. */
	relp = meta->cbm_next_logical_page - meta->cbm_index_metapage_start;

	/*
	 * If the next logical page number doesn't fit on the metapage, we need to
	 * make space by relocating some index entries to an index segment.
	 *
	 * Potentially, we could instead clean out some index entries from the
	 * metapage that now precede the logical truncation point, but that would
	 * require a cleanup lock on the metapage, and it normally isn't going to
	 * be possible, because typically the last truncate operation will have
	 * afterwards done any such work that is possible. We might miss an
	 * opportunity in the case where the last truncate operation didn't clean
	 * up fully, but hopefully that's rare enough that we don't need to stress
	 * about it.
	 *
	 * If the newest index segment is already full, then a new index segment
	 * will need to be created. Otherwise, some entries can be copied into the
	 * existing index segment.
	 */
	if (relp >= CB_METAPAGE_INDEX_ENTRIES * meta->cbm_pages_per_segment)
	{
		unsigned	entries;
		unsigned	maxentries;

		entries = meta->cbm_entries_in_newest_index_segment;
		maxentries = CB_INDEXPAGE_INDEX_ENTRIES * meta->cbm_pages_per_segment;

		if (entries > maxentries)
			elog(ERROR,
				 "newest index segment listed as using %u of %u entries",
				 entries, maxentries);
		else if (entries == maxentries ||
				 meta->cbm_newest_index_segment == CB_INVALID_SEGMENT)
			return CBM_INSERT_NEEDS_INDEX_SEGMENT;
		else
		{
			/* Figure out which block should be targeted. */
			*blkno = cb_segment_to_block(meta->cbm_pages_per_segment,
										 meta->cbm_newest_index_segment,
										 entries / CB_INDEXPAGE_INDEX_ENTRIES);

			return CBM_INSERT_NEEDS_INDEX_ENTRIES_RELOCATED;
		}
	}

	/* Compute current insertion segment and offset. */
	segno = meta->cbm_index[relp / meta->cbm_pages_per_segment];
	segoff = meta->cbm_next_logical_page % meta->cbm_pages_per_segment;

	/*
	 * If the next logical page number would be covered by an index entry that
	 * does not yet exist, we need a new payload segment.
	 */
	if (segno == CB_INVALID_SEGMENT)
		return CBM_INSERT_NEEDS_PAYLOAD_SEGMENT;

	/* Looks like we can go ahead and insert a page. Hooray! */
	*blkno = cb_segment_to_block(meta->cbm_pages_per_segment, segno, segoff);
	return CBM_INSERT_OK;
}

/*
 * Advance the next logical page number for this conveyor belt by one.
 *
 * We require the caller to specify the physical block number where the new
 * block was placed. This allows us to perform some sanity-checking.
 */
void
cb_metapage_advance_next_logical_page(CBMetapageData *meta,
									  BlockNumber blkno)
{
	BlockNumber expected_blkno;
	CBPageNo	dummy_pageno;
	CBSegNo		dummy_segno;

	/* Perform sanity checks. */
	if (cb_metapage_get_insert_state(meta, &expected_blkno, &dummy_pageno,
									 &dummy_segno, &dummy_pageno,
									 &dummy_pageno, &dummy_segno)
		!= CBM_INSERT_OK)
		elog(ERROR, "no active insertion segment");
	if (blkno != expected_blkno)
		elog(ERROR, "new page is at block %u but expected block %u",
			 blkno, expected_blkno);

	/* Do the real work. */
	meta->cbm_next_logical_page++;
}

/*
 * Advance the oldest logical page number for this conveyor belt.
 */
void
cb_metapage_advance_oldest_logical_page(CBMetapageData *meta,
										CBPageNo oldest_logical_page)
{
	/*
	 * Something must be desperately wrong if an effort is ever made to set
	 * the value backwards or even to the existing value. Higher-level code
	 * can choose to do nothing in such cases rather than rejecting them, but
	 * this function should only get called when we're committed to dirtying
	 * the page and (if required) writing WAL.
	 */
	if (meta->cbm_oldest_logical_page >= oldest_logical_page)
		elog(ERROR, "oldest logical page is already " UINT64_FORMAT " so can't be set to " UINT64_FORMAT,
			 meta->cbm_oldest_logical_page, oldest_logical_page);

	/* Do the real work. */
	meta->cbm_oldest_logical_page = oldest_logical_page;
}

/*
 * Get the oldest and next logical page numbers for this conveyor belt.
 */
void
cb_metapage_get_bounds(CBMetapageData *meta, CBPageNo *oldest_logical_page,
					   CBPageNo *next_logical_page)
{
	*oldest_logical_page = meta->cbm_oldest_logical_page;
	*next_logical_page = meta->cbm_next_logical_page;
}

/*
 * Compute the number of index entries that are used in the metapage.
 *
 * For our purposes here, an index entry isn't used unless there are some
 * logical pages associated with it. It's possible that the real number
 * of index entries is one higher than the value we return, but if so,
 * no pages have been allocated from the final segment just yet.
 *
 * The reason this is OK is that the intended purpose of this function is
 * to figure out where a new index entry ought to be put, and we shouldn't
 * be putting a new index entry into the page at all unless all of the
 * existing entries point to segments that are completely full. If we
 * needed to know how many entries had been filled in, whether or not any
 * of the associated storage was in use, we could do that by adding 1 to
 * the value computed here here if the entry at that offset is already
 * initialized.
 */
int
cb_metapage_get_index_entries_used(CBMetapageData *meta)
{
	CBPageNo	relp;

	/*
	 * Compute next logical page number relative to start of metapage.
	 *
	 * NB: The number of index entries could be equal to the number that will
	 * fit on the page, but it cannot be more.
	 */
	relp = meta->cbm_next_logical_page - meta->cbm_index_metapage_start;
	if (relp > CB_METAPAGE_INDEX_ENTRIES * meta->cbm_pages_per_segment)
		elog(ERROR,
			 "next logical page " UINT64_FORMAT " not in metapage index starting at " UINT64_FORMAT,
			 meta->cbm_next_logical_page, meta->cbm_index_start);

	/* Now we can calculate the answer. */
	return relp / meta->cbm_pages_per_segment;
}

/*
 * Add a new index entry to the metapage.
 */
void
cb_metapage_add_index_entry(CBMetapageData *meta, CBSegNo segno)
{
	int			offset = cb_metapage_get_index_entries_used(meta);

	/* Sanity checks. */
	if (offset >= CB_METAPAGE_INDEX_ENTRIES)
		elog(ERROR, "no space for index entries remains on metapage");
	if (meta->cbm_index[offset] != CB_INVALID_SEGMENT)
		elog(ERROR, "index entry at offset %d unexpectedly in use for segment %u",
			 offset, meta->cbm_index[offset]);

	/* Add the entry. */
	meta->cbm_index[offset] = segno;
}

/*
 * Remove index entries from the metapage.
 *
 * This needs to be done in two cases. First, it might be that the whole
 * index is in the metapage and that we're just trimming away some unused
 * entries. In that case, pass relocating = false. Second, it might be that
 * we're relocating index entries from the metapage to an index segment to
 * make more space in the metapage. In that case, pass relocating = true.
 */
void
cb_metapage_remove_index_entries(CBMetapageData *meta, unsigned count,
								 bool relocating)
{
	int			used = cb_metapage_get_index_entries_used(meta);
	int			offset;

	/* This shouldn't be called unless there is some real work to do. */
	Assert(count > 0);

	/* Sanity checks. */
	if (used < count)
		elog(ERROR,
			 "can't remove %d entries from a page containing only %d entries",
			 count, used);
	if (!relocating &&
		(meta->cbm_oldest_index_segment != CB_INVALID_SEGMENT ||
		 meta->cbm_newest_index_segment != CB_INVALID_SEGMENT ||
		 meta->cbm_index_start != meta->cbm_index_metapage_start))
		elog(ERROR, "removed index entries should be relocated if index segments exist");
	if (relocating &&
		(meta->cbm_oldest_index_segment == CB_INVALID_SEGMENT ||
		 meta->cbm_newest_index_segment == CB_INVALID_SEGMENT))
		elog(ERROR, "removed index entries can't be relocated if no index segments exist");

	/* Move any entries that we are keeping. */
	if (count < used)
		memmove(&meta->cbm_index[0], &meta->cbm_index[count],
				sizeof(CBSegNo) * (used - count));

	/* Zap the entries that were formerly in use and are no longer. */
	for (offset = used - count; offset < used; ++offset)
		meta->cbm_index[offset] = CB_INVALID_SEGMENT;

	/*
	 * Adjust meta->cbm_index_metapage_start to compensate for the index
	 * entries that we just removed.
	 */
	meta->cbm_index_metapage_start +=
		count * meta->cbm_pages_per_segment;
	if (relocating)
		meta->cbm_entries_in_newest_index_segment += count;
	else
		meta->cbm_index_start = meta->cbm_index_metapage_start;
}

/*
 * Return a pointer to the in-metapage index entries.
 */
CBSegNo *
cb_metapage_get_index_entry_pointer(CBMetapageData *meta)
{
	return meta->cbm_index;
}

/*
 * Return various pieces of information that are needed to initialize for
 * access to a conveyor belt.
 */
void
cb_metapage_get_critical_info(CBMetapageData *meta,
							  uint16 *pages_per_segment,
							  uint64 *index_segments_moved)
{
	*pages_per_segment = meta->cbm_pages_per_segment;
	*index_segments_moved = meta->cbm_index_segments_moved;
}

/*
 * Return various pieces of information that are needed to access index
 * segments.
 */
void
cb_metapage_get_index_info(CBMetapageData *meta,
						   CBPageNo *index_start,
						   CBPageNo *index_metapage_start,
						   CBSegNo *oldest_index_segment,
						   CBSegNo *newest_index_segment,
						   uint64 *index_segments_moved)
{
	*index_start = meta->cbm_index_start;
	*index_metapage_start = meta->cbm_index_metapage_start;
	*oldest_index_segment = meta->cbm_oldest_index_segment;
	*newest_index_segment = meta->cbm_newest_index_segment;
	*index_segments_moved = meta->cbm_index_segments_moved;
}

/*
 * Update the metapage to reflect the addition of an index segment.
 */
void
cb_metapage_add_index_segment(CBMetapageData *meta, CBSegNo segno)
{
	meta->cbm_newest_index_segment = segno;
	meta->cbm_entries_in_newest_index_segment = 0;
	if (meta->cbm_oldest_index_segment == CB_INVALID_SEGMENT)
		meta->cbm_oldest_index_segment = segno;
}

/*
 * Update the metapage to reflect the removal of an index segment.
 *
 * 'segno' should be the successor of the index segment being removed,
 * or CB_INVALID_SEGMENT if, at present, only one index segment exists.
 */
void
cb_metapage_remove_index_segment(CBMetapageData *meta, CBSegNo segno)
{
	if (meta->cbm_oldest_index_segment == CB_INVALID_SEGMENT ||
		meta->cbm_newest_index_segment == CB_INVALID_SEGMENT)
		elog(ERROR, "can't remove index segment when none remain");

	if (segno == CB_INVALID_SEGMENT)
	{
		if (meta->cbm_oldest_index_segment !=
			meta->cbm_newest_index_segment)
			elog(ERROR, "can't remove last index segment when >1 remain");
		meta->cbm_oldest_index_segment = CB_INVALID_SEGMENT;
		meta->cbm_newest_index_segment = CB_INVALID_SEGMENT;
	}
	else
	{
		if (meta->cbm_oldest_index_segment ==
			meta->cbm_newest_index_segment)
			elog(ERROR, "must remove last index segment when only one remains");
		meta->cbm_oldest_index_segment = segno;
	}
}

/*
 * Examine the metapage state to determine how to go about recycling space.
 *
 * If the return value is CBM_OBSOLETE_SEGMENT_ENTRIES, then
 * *oldest_index_segment will be set, and the caller should remove obsolete
 * entries from that segment and/or the segment itself.
 *
 * If the return value is CBM_OBSOLETE_METAPAGE_ENTRIES, then *metapage_segno
 * will be set to a payload segment that can be deallocated, and
 * *metapage_offset to the location in the metapage where the index entry
 * referencing that segment is stored.
 *
 * If the return value is CBM_OBSOLETE_METAPAGE_START, then there are
 * no index segments and no uncleared index entries in the metapage that
 * are obsolete, but some cleared index entries can be discarded.
 * *metapage_offset will be set to the smallest metapage offset that cannot
 * be cleared (either because it is still in use, or because it is not yet
 * allocated).
 *
 * If the return value is CBM_OBSOLETE_NOTHING, there's nothing to do.
 */
CBMObsoleteState
cb_metapage_get_obsolete_state(CBMetapageData *meta,
							   CBSegNo *oldest_index_segment,
							   CBSegNo *metapage_segno,
							   unsigned *metapage_offset)
{
	CBPageNo	istart = meta->cbm_index_start;
	CBPageNo	imstart = meta->cbm_index_metapage_start;
	CBPageNo	olpage = meta->cbm_oldest_logical_page;
	uint16		pps = meta->cbm_pages_per_segment;
	unsigned	keep_offset;
	unsigned	offset;

	/* Sanity checks. */
	if (olpage < istart)
		elog(ERROR,
			 "index starts at " UINT64_FORMAT " but oldest logical page is " UINT64_FORMAT,
			istart, olpage);
	if (imstart < istart)
		elog(ERROR,
			 "metapage index starts at " UINT64_FORMAT " but index starts at " UINT64_FORMAT,
			imstart, istart);
	if (istart % pps != 0)
		elog(ERROR,
			 "index start " UINT64_FORMAT " is not a multiple of pages per segment",
			 istart);
	if (imstart % pps != 0)
		elog(ERROR,
			 "index metapage start " UINT64_FORMAT " is not a multiple of pages per segment",
			 imstart);

	/*
	 * Detect the case where there is no obsolete data in the index.
	 *
	 * This happens if the oldest logical page is either equal to the start
	 * of the index, or follows it by less than the number of pages per
	 * segment. In the latter case, some but not all of the pages in the
	 * oldest payload segment are obsolete. We can only clean up entire
	 * payload semgents, so in such cases there is nothing to do.
	 */
	if (istart + pps > olpage)
		return CBM_OBSOLETE_NOTHING;

	/*
	 * If there are any index segments, then the first step is to remove
	 * index entries from those segments, and the second step is to remove
	 * the segments themselves if they end up containing no useful entries.
	 * We need not consider doing anything in the metapage itself until no
	 * index segments remain.
	 */
	if (meta->cbm_oldest_index_segment != CB_INVALID_SEGMENT)
	{
		*oldest_index_segment = meta->cbm_oldest_index_segment;
		return CBM_OBSOLETE_SEGMENT_ENTRIES;
	}

	/*
	 * Since there are no index pages, the whole index is in the metapage,
	 * and therefore the logical page number should be somewhere in the range
	 * of pages covered by the metapage.
	 */
	if (olpage < imstart)
		elog(ERROR,
			 "oldest logical page " UINT64_FORMAT " precedes metapage start " UINT64_FORMAT " but there are no index segments",
			 olpage, imstart);

	/* Search for obsolete index entries that have not yet been cleared. */
	keep_offset = (olpage - imstart) / pps;
	for (offset = 0; offset < keep_offset; ++offset)
	{
		if (meta->cbm_index[offset] != CB_INVALID_SEGMENT)
		{
			*metapage_segno = meta->cbm_index[offset];
			*metapage_offset = offset;
			return CBM_OBSOLETE_METAPAGE_ENTRIES;
		}
	}

	/*
	 * Apparently, there's nothing left to do but discard already-cleared
	 * index entries.
	 */
	*metapage_offset = keep_offset;
	return CBM_OBSOLETE_METAPAGE_START;
}

/*
 * Clear a single index entry from the metapage.
 *
 * We require that the caller provide not only the offset but the segment
 * number that is expected to be found at that offset. That lets us check
 * that nothing unexpected has occurred.
 */
void
cb_metapage_clear_obsolete_index_entry(CBMetapageData *meta,
									   CBSegNo segno,
									   unsigned offset)
{
	if (meta->cbm_index[offset] != offset)
		elog(ERROR,
			 "index entry at offset %u was expected to be %u but found %u",
			 offset, segno, meta->cbm_index[offset]);

	meta->cbm_index[offset] = CB_INVALID_SEGMENT;
}

/*
 * Returns the lowest unused segment number covered by the metapage,
 * or CB_INVALID_SEGMENT if none.
 */
CBSegNo
cb_metapage_find_free_segment(CBMetapageData *meta)
{
	unsigned	i;
	unsigned	j;

	for (i = 0; i < CB_METAPAGE_FREESPACE_BYTES; ++i)
	{
		uint8	b = meta->cbm_freespace_map[i];

		if (b == 0xFF)
			continue;

		for (j = 0; j < BITS_PER_BYTE; ++j)
		{
			if ((b & (1 << j)) == 0)
				return (i * BITS_PER_BYTE) + j;
		}
	}

	return CB_INVALID_SEGMENT;
}

/*
 * Get the allocation status of a segment from the metapage fsm.
 */
bool
cb_metapage_get_fsm_bit(CBMetapageData *meta, CBSegNo segno)
{
	uint8		byte;
	uint8		mask;

	if (segno >= CB_METAPAGE_FREESPACE_BYTES * BITS_PER_BYTE)
		elog(ERROR, "segment %u out of range for metapage fsm", segno);

	byte = meta->cbm_freespace_map[segno / BITS_PER_BYTE];
	mask = 1 << (segno % BITS_PER_BYTE);
	return (byte & mask) != 0;
}

/*
 * Set the allocation status of a segment in the metapage fsm.
 *
 * new_state should be true if the bit is currently clear and should be set,
 * and false if the bit is currently set and should be cleared. Don't call
 * this unless you know that the bit actually needs to be changed.
 */
void
cb_metapage_set_fsm_bit(CBMetapageData *meta, CBSegNo segno, bool new_state)
{
	uint8	   *byte;
	uint8		mask;
	uint8		old_state;

	if (segno >= CB_FSM_SEGMENTS_FOR_METAPAGE)
		elog(ERROR, "segment %u out of range for metapage fsm", segno);

	byte = &meta->cbm_freespace_map[segno / BITS_PER_BYTE];
	mask = 1 << (segno % BITS_PER_BYTE);
	old_state = (*byte & mask) != 0;

	if (old_state == new_state)
		elog(ERROR, "metapage fsm bit for segment %u already has value %d",
			 segno, old_state ? 1 : 0);

	if (new_state)
		*byte |= mask;
	else
		*byte &= ~mask;
}

/*
 * Increment the count of segments allocated.
 */
void
cb_metapage_increment_next_segment(CBMetapageData *meta, CBSegNo segno)
{
	if (segno != meta->cbm_next_segment)
		elog(ERROR, "extending to create segment %u but next segment is %u",
			 segno, meta->cbm_next_segment);

	meta->cbm_next_segment++;
}

/*
 * Increment the count of index segments moved.
 */
void
cb_metapage_increment_index_segments_moved(CBMetapageData *meta)
{
	meta->cbm_index_segments_moved++;
}
