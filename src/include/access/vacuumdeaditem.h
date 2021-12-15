/*-------------------------------------------------------------------------
 *
 * vacuumdeaditem.h
 *		Public API for vacuumdeaditem.
 *
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/vacuumdeaditem.h
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
typedef struct VDI_BlockHeader
{
	BlockIdData		blkid;
	uint16			noffsets;
} VDI_BlockHeader;

/*
 * In the special space of the page we store the first pageno of the next
 * vacuum run.  For more details refer comment atop VDI_RunState.
 */
typedef struct VDI_PageData
{
	CBPageNo	nextrunpage;
} VDI_PageData;

typedef struct VDI_DeadItemState VDI_DeadItemState;

/* Usable space of the page. */
#define VDI_PageMaxDataSpace \
		(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - sizeof(VDI_PageData))

/* Compute required space for storing (block header + noffset). */
#define VDI_BlkDataSize(noffset) \
		sizeof(VDI_BlockHeader) + sizeof (OffsetNumber) * (noffset)

/*
 * We will copy the block data to the page iff the remaining space can fit
 * the block header and at least one offset.
 */
#define VDI_MinCopySize	sizeof(VDI_BlockHeader) + sizeof(OffsetNumber)

extern VDI_DeadItemState *VDI_InitDeadItemState(Relation rel);
extern void VDI_InsertDeadItems(VDI_DeadItemState *deaditemstate, int ntids,
								ItemPointerData *deadtids);
extern int VDI_ReadDeadItems(VDI_DeadItemState *deaditemstate,
							 CBPageNo from_pageno, CBPageNo to_pageno,
							 int maxtids, ItemPointerData *deadtids,
							 CBPageNo *last_pageread);
extern void VDI_LoadDeadItems(VDI_DeadItemState *deaditemstate,
							  BlockNumber blkno, int maxtids);
extern bool VDI_DeadItemExists(VDI_DeadItemState *deaditemstate,
							  ItemPointerData *tid);
extern void VDI_SaveVacuumRun(VDI_DeadItemState *deaditemstate);							  
extern void VDI_ReleaseDeadItemState(VDI_DeadItemState *deaditemstate);
extern bool VDI_HasDeadItems(VDI_DeadItemState *deaditemstate);
extern CBPageNo VDI_GetLastPage(VDI_DeadItemState *deaditemstate);
extern CBPageNo VDI_GetOldestPage(VDI_DeadItemState *deaditemstate);
extern void VDI_Vacuum(VDI_DeadItemState *deaditemstate, CBPageNo pageno);

#endif							/* DEADTIDSTORE_H */
