/*-------------------------------------------------------------------------
 *
 * deadtidstore.c
 *	  Store and fetch deadtids.
 *
 * Create dead tid fork and manage storing and retriving deadtids using
 * conveyor belt infrastructure.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/backend/access/conveyor/deadtidstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/conveyor.h"
#include "access/deadtidstore.h"
#include "miscadmin.h"
#include "storage/smgr.h"
#include "utils/rel.h"

#define DTS_PAGES_PER_SEGMENT	4
#define DTS_TID_PERPAGE \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) / sizeof(ItemPointerData)
#define DTS_PAGE_TID_COUNT(page) \
	(((PageHeader) (page))->pd_lower -  MAXALIGN(SizeOfPageHeaderData)) / sizeof(ItemPointerData)

/*
 *	DTS_AppendTid() -- flush out dead TIDs to the dead tid store
 *
 *  *pageno - output logical pageno from where it started dumping the deadtids.
 */
void
DTS_AppendTid(Relation rel, int ntids, ItemPointerData *deadtids)
{
	ConveyorBelt   *cb;
	SMgrRelation	reln;

	/* Open the relation at smgr level. */
	reln = RelationGetSmgr(rel);

	/*
	 * If the dead_tid fork doesn't exist then create it and initialize the
	 * conveyor belt, otherwise just open the conveyor belt.
	 */
	if (!smgrexists(reln, DEADTID_FORKNUM))
	{
		smgrcreate(reln, DEADTID_FORKNUM, false);
		cb = ConveyorBeltInitialize(rel,
									DEADTID_FORKNUM,
									DTS_PAGES_PER_SEGMENT,
									CurrentMemoryContext);
	}
	else
		cb = ConveyorBeltOpen(rel, DEADTID_FORKNUM, CurrentMemoryContext);

	/* Loop until we store all dead tids in the conveyor belt. */
	while(ntids > 0)
	{
		Buffer	buffer;
		Page	page;
		char   *pagedata;
		int		count;
		PageHeader	phdr;
		CBPageNo	pageno;

		/* Get a new page from the ConveyorBelt. */
		buffer = ConveyorBeltGetNewPage(cb, &pageno);
		page = BufferGetPage(buffer);
		PageInit(page, BLCKSZ, 0);
		phdr = (PageHeader) page;
		pagedata = PageGetContents(page);

		/* Compute how many dead tids we can store into this page. */
		count =  Min(DTS_TID_PERPAGE, ntids);

		START_CRIT_SECTION();
		/*
		 * Copy the data into the page and set the pd_lower.  We are just
		 * copying ItemPointerData array so we don't need any alignment which
		 * should be SHORTALIGN and sizeof(ItemPointerData) will take care of
		 * that so we don't need any other alignment.
		 */
		memcpy(pagedata, deadtids,
			   sizeof(ItemPointerData) * count);
		phdr->pd_lower += sizeof(ItemPointerData) * count;
		ConveyorBeltPerformInsert(cb, buffer);

		END_CRIT_SECTION();

		ConveyorBeltCleanupInsert(cb, buffer);

		/* Adjust deadtid pointer and the remaining tid count. */
		deadtids += count;
		ntids -= count;
	}
}

/*
 * DTS_ReadDeadTids - Read dead tids from the deat tid store
 *
 * start_pageno - Logical pageno from where to start reading the dead tids
 * deadtids - Pre allocated array for holding the dead tids, the allocated size
 * is sufficient to hold maxtids.
 * *last_pageread - Store last page number we have read dead tids from.
 *
 * Returns number of dead tids actually read from the storage.
 */
int
DTS_ReadDeadTids(Relation rel, CBPageNo start_pageno, int maxtids,
				 ItemPointerData *deadtids, CBPageNo *last_pageread)
{
	SMgrRelation	reln;
	ConveyorBelt   *cb;
	Page			page;
	Buffer			buffer;
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;
	int				count;
	int				ndeadtids = 0;
	int				prevpageno = start_pageno;

	/* Open the relation at smgr level. */
	reln = RelationGetSmgr(rel);

	/* The deadtid fork must exists. */
	Assert(smgrexists(reln, DEADTID_FORKNUM));

	/* Open the conveyor belt. */
	cb = ConveyorBeltOpen(rel, DEADTID_FORKNUM, CurrentMemoryContext);

	/* 
	 * Read the conveyor belt bounds and check page validity,
	 * XXX eventually here this function should be called only for assert
	 * checking.
	 */
	ConveyorBeltGetBounds(cb, &oldest_pageno, &next_pageno);
	if (start_pageno < oldest_pageno || start_pageno >= next_pageno)
		return 0;

	/* Loop until we read maxtids. */
	while(ndeadtids < maxtids && start_pageno < next_pageno)
	{
		buffer = ConveyorBeltReadBuffer(cb, start_pageno, BUFFER_LOCK_SHARE, NULL);
		if (BufferIsInvalid(buffer))
			break;

		page = BufferGetPage(buffer);
		count = DTS_PAGE_TID_COUNT(page);

		/*
		 * Don't stop reading in middle of the page, so if remaning tids to
		 * read are less than the tid in this page then stop here.
		 */
		if (maxtids - ndeadtids < count)
			break;

		/* Read data into the deadtid buffer. */
		memcpy(deadtids + ndeadtids, PageGetContents(page),
			   sizeof(ItemPointerData) * count);
		UnlockReleaseBuffer(buffer);

		ndeadtids += count;
		prevpageno = start_pageno;
		start_pageno++;
	}

	*last_pageread = prevpageno;

	return ndeadtids;
}
