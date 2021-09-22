/*-------------------------------------------------------------------------
 *
 * conveyor.h
 *	  Public API for conveyor belt storage.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/conveyor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONVEYOR_H
#define CONVEYOR_H

#include "access/cbdefs.h"
#include "common/relpath.h"
#include "utils/relcache.h"
#include "storage/bufmgr.h"

struct ConveyorBelt;
typedef struct ConveyorBelt ConveyorBelt;

extern ConveyorBelt *ConveyorBeltInitialize(Relation rel,
											ForkNumber fork,
											uint16 pages_per_segment,
											MemoryContext mcxt);
extern ConveyorBelt *ConveyorBeltOpen(Relation rel,
									  ForkNumber fork,
									  MemoryContext mcxt);
extern Buffer ConveyorBeltGetNewPage(ConveyorBelt *cb, CBPageNo *pageno);
extern void ConveyorBeltPerformInsert(ConveyorBelt *cb, Buffer buffer,
									  bool page_std);
extern void ConveyorBeltCleanupInsert(ConveyorBelt *cb, Buffer buffer);
extern Buffer ConveyorBeltReadBuffer(ConveyorBelt *cb, CBPageNo pageno,
									 int mode,
									 BufferAccessStrategy strategy);
extern void ConveyorBeltGetBounds(ConveyorBelt *cb,
								  CBPageNo *oldest_logical_page,
								  CBPageNo *next_logical_page);
extern void ConveyorBeltTruncate(ConveyorBelt *cb, CBPageNo oldest_keeper);
extern void ConveyorBeltVacuum(ConveyorBelt *cb);
extern void ConveyorBeltCompact(ConveyorBelt *cb);
extern ConveyorBelt *ConveyorBeltRewrite(ConveyorBelt *cb,
										 Relation rel,
										 ForkNumber fork,
										 MemoryContext mcxt);

#endif							/* CONVEYOR_H */
