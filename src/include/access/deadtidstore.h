/*-------------------------------------------------------------------------
 *
 * deadtidstore.h
 *		Public API for deadtidstore.
 *
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/deadtidstore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEADTIDSTORE_H
#define DEADTIDSTORE_H

#include "access/conveyor.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"

/*
 * In conveyor belt instead of saving the complete deadtids we will store the
 * compressed format, i.e. [blkno, noffsets] [off-1][off-2]..[off-n], and
 * whenever we are going for the index vacuum we can convert it back to the
 * dead tid array.  So instead of converting it right after pruning each page
 * as we are doing it now, we will delay it until we are not going for index
 * vacuum.  Instead of BlockNumber, we use BlockIdData to avoid alignment
 * padding.
 */
typedef struct DTS_BlockHeader
{
	BlockIdData		blkid;
	uint16			noffsets;
} DTS_BlockHeader;

/*
 * In the special space of the page we store the first pageno of the next
 * vacuum run.  For more details refer comment atop DTS_RunState.
 */
typedef struct DTS_PageData
{
	CBPageNo	nextrunpage;
} DTS_PageData;

typedef struct DTS_DeadTidState DTS_DeadTidState;

/* Usable space of the page. */
#define DTS_PageMaxDataSpace \
		(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - sizeof(DTS_PageData))

/* Compute required space for storing (block header + noffset). */
#define DTS_BlkDataSize(noffset) \
		sizeof(DTS_BlockHeader) + sizeof (OffsetNumber) * (noffset)

/*
 * We will copy the block data to the page iff the remaining space can fit
 * the block header and at least one offset.
 */
#define DTS_MinCopySize	sizeof(DTS_BlockHeader) + sizeof(OffsetNumber)

extern DTS_DeadTidState *DTS_InitDeadTidState(Relation rel,
											  CBPageNo min_idxvac_page);
extern void DTS_InsertDeadtids(DTS_DeadTidState *deadtidstate, char *data,
							   int *pdatasizes, int npages);
extern int DTS_ReadDeadtids(DTS_DeadTidState *deadtidstate,
							CBPageNo from_pageno, CBPageNo to_pageno,
							int maxtids, ItemPointerData *deadtids,
							CBPageNo *last_pageread, CBPageNo *next_runpage);
extern void DTS_LoadDeadtids(DTS_DeadTidState *deadtidstate,
							 BlockNumber blkno);
extern bool DTS_DeadtidExists(DTS_DeadTidState *deadtidstate,
							  BlockNumber blkno, OffsetNumber offset,
							  bool *setunused);
extern void DTS_ReleaseDeadTidState(DTS_DeadTidState *deadtidstate);
extern void DTS_Vacuum(DTS_DeadTidState	*deadtidstate, CBPageNo	pageno);

#endif							/* DEADTIDSTORE_H */
