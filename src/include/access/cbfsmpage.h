/*-------------------------------------------------------------------------
 *
 * cbfsmpage.h
 *	  APIs for accessing conveyor belt free space map pages.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbfsmpage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBFSMPAGE_H
#define CBFSMPAGE_H

#include "access/cbdefs.h"
#include "access/cbmetapage.h"
#include "storage/bufpage.h"

/*
 * Number of free space map bytes reserved for fixed-size data.
 *
 * This needs to be at least large enough to hold a PageHeader plus the
 * non-array fields in CBFSMPageData. We make it comfortably larger than
 * that in case we ever want to enlarge CBFSMPageData.
 */
#define		CB_FSMPAGE_RESERVE_BYTES	128

/*
 * Number of bytes left over to store segment allocation status.
 */
#define		CB_FSMPAGE_FREESPACE_BYTES	(BLCKSZ - CB_FSMPAGE_RESERVE_BYTES)

/*
 * Number of segments covered by one FSM page.
 */
#define		CB_FSM_SEGMENTS_PER_FSMPAGE \
		(CB_FSMPAGE_FREESPACE_BYTES * BITS_PER_BYTE)

/*
 * Function prototypes.
 */
extern CBSegNo cb_fsmpage_initialize(Page page, BlockNumber blkno,
									 uint16 pages_per_segment);
extern bool cb_fsmpage_get_fsm_bit(Page page, CBSegNo segno);
extern void cb_fsmpage_set_fsm_bit(Page page, CBSegNo segno, bool new_state);
extern CBSegNo cbfsmpage_find_free_segment(Page page);

/*
 * Where is the first FSM block located?
 */
static inline BlockNumber
cb_first_fsm_block(uint16 pages_per_segment)
{
	/* Add 1 to account for the metapage. */
	return 1 + CB_FSM_SEGMENTS_FOR_METAPAGE * (BlockNumber) pages_per_segment;
}

/*
 * How far apart are FSM blocks?
 */
static inline unsigned
cb_fsm_block_spacing(uint16 pages_per_segment)
{
	/* Add 1 to account for the FSM page itself. */
	return 1 + CB_FSM_SEGMENTS_PER_FSMPAGE * (unsigned) pages_per_segment;
}

/*
 * Figure out which block number contains a certain block within a certain
 * segment.
 */
static inline BlockNumber
cb_segment_to_block(uint16 pages_per_segment, CBSegNo segno, unsigned segoff)
{
	unsigned	extra_pages = 1;

	Assert(segoff < pages_per_segment);

	if (segno >= CB_FSM_SEGMENTS_FOR_METAPAGE)
		extra_pages += 1 + (segno - CB_FSM_SEGMENTS_FOR_METAPAGE)
			/ CB_FSM_SEGMENTS_PER_FSMPAGE;

	return extra_pages + segno * pages_per_segment + segoff;
}

/*
 * Figure out the segment number of the first segment covered by an FSM page.
 */
static inline CBSegNo
cb_first_segment_for_fsm_page(BlockNumber blkno, uint16 pages_per_segment)
{
	BlockNumber	first_fsm_block = cb_first_fsm_block(pages_per_segment);
	unsigned	fsm_block_spacing = cb_fsm_block_spacing(pages_per_segment);
	unsigned	fsm_index;

	Assert(blkno >= first_fsm_block);
	Assert((blkno - first_fsm_block) % fsm_block_spacing == 0);

	fsm_index = (blkno - first_fsm_block) / fsm_block_spacing;
	return CB_FSM_SEGMENTS_FOR_METAPAGE
		+ (fsm_index * CB_FSM_SEGMENTS_PER_FSMPAGE);
}

#endif							/* CBFSMPAGE_H */
