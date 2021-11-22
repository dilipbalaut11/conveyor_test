/*-------------------------------------------------------------------------
 *
 * deadtidstore.h
 *		dead tid store
 *
 *
 * Portions Copyright (c) 2007-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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

struct DTS_DeadTidState;
typedef struct DTS_DeadTidState DTS_DeadTidState;

extern DTS_DeadTidState *DTS_InitDeadTidState(Relation rel);
extern void DTS_InsertDeadtids(DTS_DeadTidState *deadtidstate, int ntids,
							   ItemPointerData *deadtids);
extern int DTS_ReadDeadtids(DTS_DeadTidState *deadtidstate,
							CBPageNo from_pageno, CBPageNo to_pageno,
							int maxtids, ItemPointerData *deadtids,
							CBPageNo *last_pageread);
extern void DTS_LoadDeadtids(DTS_DeadTidState *deadtidstate, BlockNumber blkno,
							 int maxtids);
extern bool DTS_DeadtidExists(DTS_DeadTidState *deadtidstate,
							  ItemPointerData *tid);
extern void DTS_SetNextRun(DTS_DeadTidState	*deadtidstate);
extern void DTS_Vacuum(DTS_DeadTidState	*deadtidstate, CBPageNo	pageno);

#endif							/* DEADTIDSTORE_H */
