/*-------------------------------------------------------------------------
 *
 * cbmetapage.h
 *	  APIs for accessing conveyor belt metapages.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbmetapage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBMETAPAGE_H
#define CBMETAPAGE_H

#include "access/cbdefs.h"
#include "storage/bufpage.h"

/* Opaque struct for the actual metapage format. */
struct CBMetapageData;
typedef struct CBMetapageData CBMetapageData;

/* In which block number is the metapage stored? */
#define CONVEYOR_METAPAGE	0

/*
 * Number of metapage bytes reserved for fixed-size data.
 *
 * This needs to be at least large enough to hold a PageHeader plus the
 * non-array fields in CBMetaPageData. We make it comfortably larger than
 * that in case we ever want to enlarge CBMetaPageData.
 */
#define		CB_METAPAGE_RESERVE_BYTES			256

/*
 * Number of index entries and freespace map bytes stored in the metapage.
 *
 * Somewhat arbitrarily, we allocate half the page to index entries and
 * the remaining space to freespace map bytes. Note that the freespace map
 * is much more compact than the index (1 bit per segment vs. 4 bytes per
 * segment) so a little bit of space goes a long way. We could further
 * reduce the size of the freespace map to make room for more index entries,
 * but it doesn't seem like it would have much of an impact either way.
 */
#define		CB_METAPAGE_INDEX_BYTES				(BLCKSZ / 2)
#define		CB_METAPAGE_INDEX_ENTRIES	\
		(CB_METAPAGE_INDEX_BYTES / sizeof(CBSegNo))
#define		CB_METAPAGE_FREESPACE_BYTES \
		(BLCKSZ - CB_METAPAGE_RESERVE_BYTES - \
			CB_METAPAGE_INDEX_ENTRIES * sizeof(CBSegNo))

/*
 * Number of segments whose allocation status can be tracked in the metapage.
 */
#define		CB_FSM_SEGMENTS_FOR_METAPAGE \
		(CB_METAPAGE_FREESPACE_BYTES * BITS_PER_BYTE)

/*
 * Possible states of the metapage with regard to the proposed insertion of
 * a payload page.
 *
 * CBM_INSERT_OK means that the most recent index entry is for a payload
 * segment that is not yet full. All other values imply that this is not
 * the case.
 *
 * CBM_INSERT_NEEDS_PAYLOAD_SEGMENT means that there is still room in the
 * metapage for more index entries.
 *
 * CBM_INSERT_NEEDS_INDEX_ENTRIES_RELOCATED means that there is no more room
 * in the metapage for additional index entries, but there is room in the
 * newest index segment for entries to be relocated from the metapage.
 *
 * CBM_INSERT_NEEDS_INDEX_ENTRY_SPACE means that there is no more room in
 * the metapage for additional index entries, and the newest index segment
 * is full, too.
 */
typedef enum
{
	CBM_INSERT_OK,
	CBM_INSERT_NEEDS_PAYLOAD_SEGMENT,
	CBM_INSERT_NEEDS_INDEX_ENTRIES_RELOCATED,
	CBM_INSERT_NEEDS_INDEX_SEGMENT
} CBMInsertState;

/*
 * Function prototypes.
 */
extern void cb_metapage_initialize(Page page, uint16 pages_per_segment);
extern CBMetapageData *cb_metapage_get_special(Page page);
extern bool cb_metapage_find_logical_page(CBMetapageData *meta,
										  CBPageNo pageno,
										  BlockNumber *blkno);
extern CBMInsertState cb_metapage_find_next_logical_page(CBMetapageData *meta,
														 CBPageNo *pageno,
														 BlockNumber *blkno,
														 CBSegNo *next_segno);
extern CBMInsertState cb_metapage_get_insert_state(CBMetapageData *meta,
												   BlockNumber *blkno,
												   CBPageNo *next_pageno,
												   CBSegNo *next_segno,
												   CBPageNo *index_metapage_start,
												   CBSegNo *newest_index_segment);
extern void cb_metapage_advance_next_logical_page(CBMetapageData *meta,
												  BlockNumber blkno);
extern void cb_metapage_advance_oldest_logical_page(CBMetapageData *meta,
													CBPageNo oldest_logical_page);
extern void cb_metapage_get_bounds(CBMetapageData *meta,
								   CBPageNo *oldest_logical_page,
								   CBPageNo *next_logical_page);
extern int	cb_metapage_get_index_entries_used(CBMetapageData *meta);
extern void cb_metapage_add_index_entry(CBMetapageData *meta, CBSegNo segno);
extern void cb_metapage_remove_index_entries(CBMetapageData *meta,
											 unsigned count,
											 bool relocating);
extern CBSegNo *cb_metapage_get_index_entry_pointer(CBMetapageData *meta);
extern void cb_metapage_get_critical_info(CBMetapageData *meta,
										  uint16 *pages_per_segment,
										  uint64 *index_segments_moved);
extern void cb_metapage_get_index_info(CBMetapageData *meta,
									   CBPageNo *index_start,
									   CBPageNo *index_metapage_start,
									   CBSegNo *oldest_index_segment,
									   CBSegNo *newest_index_segment,
									   uint64 *index_segments_moved);
extern void cb_metapage_add_index_segment(CBMetapageData *meta,
										  CBSegNo segno);
extern void cb_metapage_remove_index_segment(CBMetapageData *meta,
											 CBSegNo segno);
extern CBSegNo cb_metapage_find_free_segment(CBMetapageData *meta);
extern bool cb_metapage_get_fsm_bit(CBMetapageData *meta, CBSegNo segno);
extern void cb_metapage_set_fsm_bit(CBMetapageData *meta, CBSegNo segno,
									bool new_state);
extern void cb_metapage_increment_next_segment(CBMetapageData *meta,
											   CBSegNo segno);
extern void cb_metapage_increment_index_segments_moved(CBMetapageData *meta);

#endif							/* CBMETAPAGE_H */
