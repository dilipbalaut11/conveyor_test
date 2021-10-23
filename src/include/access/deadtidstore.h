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

extern void DTS_AppendTid(Relation rel, int ntids, ItemPointerData *deadtids);
extern int DTS_ReadDeadTids(Relation rel, CBPageNo start_pageno, int maxtids,
				 ItemPointerData *deadtids, CBPageNo *last_pageread);


#endif							/* DEADTIDSTORE_H */
