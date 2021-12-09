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
#include "access/htup_details.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/rel.h"


/*
 * State of the cache over the vacuum runs in the conveyor belt.
 */
typedef struct DTS_RunState
{
	int		num_runs;		/* number of runs in conveyor belt. */
	int		maxtid;			/* max number of tid in cache */
	int	   *runindex;		/* current index into each run. */
	CBPageNo		   *startpage;	/* start cb pageno for each run. */
	CBPageNo		   *nextpage;	/* next cb pageno for each run. */
	ItemPointerData	   *itemptrs;	/* tid cache for each runs. */
} DTS_RunState;

/*
 * Dead tid fork state.
 */
struct DTS_DeadTidState
{
	ConveyorBelt   *cb;					/* conveyor belt reference.*/
	CBPageNo		start_page;			/* start page for this run. */
	CBPageNo		last_page;			/* last page for this run. */
	int				num_tids;			/* number of of dead tids. */
	ItemPointerData	   *itemptrs;		/* dead tid cache. */
	DTS_RunState	   *deadtidrun;		/* dead tid run cache state. */
};

/* Next run page number in the special space. */
typedef struct DTS_PageData
{
	CBPageNo	nextrunpage;
} DTS_PageData;

/* Max dead tids per conveyor belt page. */
#define DTS_MaxTidsPerPage \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - sizeof(DTS_PageData)) / sizeof(ItemPointerData)

/* Compute the number of deadtids in given conveyor belt page. */
#define DTS_TidsInPage(page) \
	(((PageHeader) (page))->pd_lower -  MAXALIGN(SizeOfPageHeaderData)) / sizeof(ItemPointerData)

#define DTS_SizeOfDeadTids(cnt) \
			 mul_size(sizeof(ItemPointerData), cnt)

#define DTS_NextRunItem(runstate, run, maxtid) \
	((runstate)->itemptrs + ((run) * (maxtid) + (runstate)->runindex[(run)]))

/* non-export function prototypes */
static void dts_page_init(Page page);
static int32 dts_tidcmp(const void *a, const void *b);
static void dts_load_run(DTS_DeadTidState *deadtidstate, int run);
static void dts_merge_runs(DTS_DeadTidState *deadtidstate);
static void dts_process_runs(DTS_DeadTidState *deadtidstate);

/*
 * DTS_InitDeadTidState - intialize the deadtid state
 *
 * This will maintain the current state of reading and writing from the dead
 * tid store.
 */
DTS_DeadTidState *
DTS_InitDeadTidState(Relation rel)
{
	DTS_DeadTidState   *deadtidstate;
	ConveyorBelt	   *cb;
	SMgrRelation		reln;

	/* Open the relation at smgr level. */
	reln = RelationGetSmgr(rel);

	deadtidstate = palloc0(sizeof(DTS_DeadTidState));

	/*
	 * If the dead_tid fork doesn't exist then create it and initialize the
	 * conveyor belt, otherwise just open the conveyor belt.
	 */
	if (!smgrexists(reln, DEADTID_FORKNUM))
	{
		smgrcreate(reln, DEADTID_FORKNUM, false);
		cb = ConveyorBeltInitialize(rel,
									DEADTID_FORKNUM,
									1024,	/* What is the best value ?*/
									CurrentMemoryContext);
	}
	else
		cb = ConveyorBeltOpen(rel, DEADTID_FORKNUM, CurrentMemoryContext);

	deadtidstate->cb = cb;
	deadtidstate->start_page = CB_INVALID_LOGICAL_PAGE;
	deadtidstate->last_page = CB_INVALID_LOGICAL_PAGE;
	deadtidstate->num_tids = -1;


	return deadtidstate;
}

/*
 *	DTS_InsertDeadtids() -- Insert given deadtids into the conveyor belt.
 */
void
DTS_InsertDeadtids(DTS_DeadTidState *deadtidstate, int ntids,
				   ItemPointerData *deadtids)
{
	ConveyorBelt   *cb = deadtidstate->cb;
	CBPageNo		pageno;

	/* Loop until we flush out all the dead tids in the conveyor belt. */
	while(ntids > 0)
	{
		Buffer	buffer;
		Page	page;
		char   *pagedata;
		int		count;
		PageHeader	phdr;

		/* Get a new page from the ConveyorBelt. */
		buffer = ConveyorBeltGetNewPage(cb, &pageno);
		page = BufferGetPage(buffer);
		dts_page_init(page);
		phdr = (PageHeader) page;
		pagedata = PageGetContents(page);

		/* Compute how many dead tids we can store into this page. */
		count =  Min(DTS_MaxTidsPerPage, ntids);

		START_CRIT_SECTION();
		/*
		 * Copy the data into the page and set the pd_lower.  We are just
		 * copying ItemPointerData array so we don't need any alignment which
		 * should be SHORTALIGN and sizeof(ItemPointerData) will take care of
		 * that so we don't need any other alignment.
		 */
		memcpy(pagedata, deadtids, sizeof(ItemPointerData) * count);
		phdr->pd_lower += sizeof(ItemPointerData) * count;
		ConveyorBeltPerformInsert(cb, buffer);

		END_CRIT_SECTION();

		ConveyorBeltCleanupInsert(cb, buffer);

		/* Set the start page for this run if not yet set. */
		if (deadtidstate->start_page == CB_INVALID_LOGICAL_PAGE)
			deadtidstate->start_page = pageno;

		/* Adjust deadtid pointer and the remaining tid count. */
		deadtids += count;
		ntids -= count;
	}

	deadtidstate->last_page = pageno;
}

/*
 * DTS_ReadDeadtids - Read dead tids from the conveyor belt
 *
 * from_pageno - Conveyor belt page number from where we want to start
 * to_pageno - Conveyor belt page number upto which we want to read
 * deadtids - Pre allocated array for holding the dead tids, the allocated size
 * is sufficient to hold maxtids.
 * *last_pageread - Store last page number we have read from coveyor belt.
 *
 * Returns number of dead tids actually read from the conveyor belt.  Should
 * always be <= maxtids.
 */
int
DTS_ReadDeadtids(DTS_DeadTidState *deadtidstate, CBPageNo from_pageno,
				 CBPageNo to_pageno, int maxtids, ItemPointerData *deadtids,
				 CBPageNo *last_pageread)
{
	ConveyorBelt   *cb = deadtidstate->cb;
	Page			page;
	Buffer			buffer;
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;
	int				count;
	int				ndeadtids = 0;
	int				prevpageno = from_pageno;

	/* Read the conveyor belt bounds and check page validity. */
	ConveyorBeltGetBounds(cb, &oldest_pageno, &next_pageno);
	if (from_pageno >= next_pageno)
		return 0;

	Assert(from_pageno >= oldest_pageno &&
		   (to_pageno == CB_INVALID_LOGICAL_PAGE || to_pageno <= next_pageno));

	/*
	 * If caller has passed a valid to_pageno then don't try to read beyond
	 * that.
	 */
	if (to_pageno != CB_INVALID_LOGICAL_PAGE && to_pageno < next_pageno)
		next_pageno = to_pageno;

	/* Loop until we read maxtids. */
	while(ndeadtids < maxtids && from_pageno < next_pageno)
	{
		/* Read page from the conveyor belt. */
		buffer = ConveyorBeltReadBuffer(cb, from_pageno, BUFFER_LOCK_SHARE,
										NULL);
		if (BufferIsInvalid(buffer))
			break;

		page = BufferGetPage(buffer);
		count = DTS_TidsInPage(page);

		/*
		 * Don't stop reading in middle of the page, so if remaning tids to
		 * read are less than the tid in this page then stop here.
		 */
		if (maxtids - ndeadtids < count)
		{
			UnlockReleaseBuffer(buffer);
			break;
		}

		/* Read data into the deadtid buffer. */
		memcpy(deadtids + ndeadtids, PageGetContents(page),
			   sizeof(ItemPointerData) * count);
		UnlockReleaseBuffer(buffer);

		/* 
		 * Update the deadtid count we read and remember the previous last
		 * conveyor belt page we read.
		 */
		ndeadtids += count;
		prevpageno = from_pageno;

		/* Go to the next page. */
		from_pageno++;
	}

	*last_pageread = prevpageno;

	return ndeadtids;
}

/*
 * DTS_LoadDeadtids - load deadtid from conveyor belt into the cache.
 *
 * blkno - current heap block we are going to scan, so this function need to
 * ensure that we have all the dead tids loaded in the cache at least for
 * this heap block.
 * maxtids - max number of dead tids can be loaded in the cache.
 */
void
DTS_LoadDeadtids(DTS_DeadTidState *deadtidstate, BlockNumber blkno,
				 int maxtids)
{
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;

	Assert(deadtidstate != NULL);

	if (deadtidstate->num_tids == 0)
		return;

	/* Get conveyor belt page range. */
	ConveyorBeltGetBounds(deadtidstate->cb, &oldest_pageno, &next_pageno);

	/* No data in the conveyor belt. */
	if ((oldest_pageno == next_pageno) ||
		(oldest_pageno == deadtidstate->start_page))
		return;

	/* If deadtid cache is not yet intialize then do it now. */
	if (deadtidstate->num_tids == -1)
	{
		CBPageNo		last_pageread;

		/* Allocate space for deadtid cache. */
		deadtidstate->itemptrs =
					(ItemPointerData *) palloc(DTS_SizeOfDeadTids(maxtids));

		/*
		 * If we can load all the deadtids in the cache then read all the tids
		 * directly into the cache, and sort them. 		 
		 */
		if ((next_pageno - oldest_pageno) * DTS_MaxTidsPerPage <= maxtids)
		{
			/* Read dead tids from the dead tuple store. */
			deadtidstate->num_tids = DTS_ReadDeadtids(deadtidstate,
													  oldest_pageno,
													  CB_INVALID_LOGICAL_PAGE,
													  maxtids,
													  deadtidstate->itemptrs,
													  &last_pageread);
			Assert(last_pageread == next_pageno - 1);

			/* Sort the dead_tids array. */
			qsort((void *) deadtidstate->itemptrs, deadtidstate->num_tids,
				  sizeof(ItemPointerData), dts_tidcmp);
		}

		/*
		 * Compute the number of runs available in the conveyor belt and check
		 * whether we have enough cache to load at least one block from each
		 * run, if so then we can peroform in-memory merge of the runs.
		 */
		else
		{
			deadtidstate->deadtidrun = palloc0(sizeof(DTS_RunState));
			deadtidstate->deadtidrun->maxtid = maxtids;
			dts_process_runs(deadtidstate);
		}
	}

	/* 
	 * If all the data is already loaded in the cache then nothing to be done.
	 */
	if (deadtidstate->deadtidrun == NULL)
		return;

	/*
	 * Check if the block no of the last tid in the cache is > the input block
	 * number then we have all the tids at least for the input block so we
	 * don't need to load more tids for now.
	 */
	if (deadtidstate->num_tids > 0 &&  ItemPointerGetBlockNumberNoCheck(
		&deadtidstate->itemptrs[deadtidstate->num_tids - 1]) > blkno)
	{
		return;
	}

	/* Load more tids in cache from each runs. */
	dts_merge_runs(deadtidstate);
}

/*
 * DTS_DeadtidExists - check whether the tid already exist in deadtid cache.
 */
bool
DTS_DeadtidExists(DTS_DeadTidState *deadtidstate, ItemPointerData *tid)
{
	if (deadtidstate == NULL || deadtidstate->itemptrs == NULL)
		return false;

	return bsearch((void *) tid, (void *) deadtidstate->itemptrs,
				   deadtidstate->num_tids, sizeof(ItemPointerData),
				   dts_tidcmp);
}

/*
 * DTS_SetNextRun - Set next run start page.
 */
void
DTS_SetNextRun(DTS_DeadTidState	*deadtidstate)
{
	DTS_PageData *deadtidpd;
	Buffer	buffer;
	Page	page;

	/* Read page from the conveyor belt. */
	buffer = ConveyorBeltReadBuffer(deadtidstate->cb, deadtidstate->start_page,
									BUFFER_LOCK_EXCLUSIVE, NULL);
	if (BufferIsInvalid(buffer))
		elog(ERROR, "invalid conveyor belt page" UINT64_FORMAT,
			 deadtidstate->start_page);

	page = BufferGetPage(buffer);
	deadtidpd = (DTS_PageData *) PageGetSpecialPointer(page);
	deadtidpd->nextrunpage = deadtidstate->last_page + 1;

	/* TODO - WAL log this operation*/
	elog(LOG, "run complete setting next run page = "UINT64_FORMAT,
		 deadtidpd->nextrunpage);

	UnlockReleaseBuffer(buffer);
	if (deadtidstate->itemptrs)
		pfree(deadtidstate->itemptrs);
	pfree(deadtidstate);
}

/*
 * DTS_Vacuum - truncate and vacuum the underlying conveyor belt.
 */
void
DTS_Vacuum(DTS_DeadTidState	*deadtidstate, CBPageNo	pageno)
{
	/* Truncate the conveyor belt pages which we have already processed. */
	ConveyorBeltLogicalTruncate(deadtidstate->cb, pageno);

	/* Vacuum the conveyor belt. */
	ConveyorBeltVacuum(deadtidstate->cb);
}

/*
 * dts_page_init - Initialize an deadtid page.
 */
static void
dts_page_init(Page page)
{
	DTS_PageData	   *deadtidpd;

	PageInit(page, BLCKSZ, sizeof(DTS_PageData));
	deadtidpd = (DTS_PageData *) PageGetSpecialPointer(page);
	deadtidpd->nextrunpage = CB_INVALID_LOGICAL_PAGE;
}

/*
 * dts_tidcmp() - Compare two item pointers, return -1, 0, or +1.
 */
static int32
dts_tidcmp(const void *a, const void *b)
{
	ItemPointer iptr1 = ((const ItemPointer) a);
	ItemPointer iptr2 = ((const ItemPointer) b);

	return ItemPointerCompare(iptr1, iptr2);
}

/*
 * dts_load_run - load data from conveyor belt vacuum run into the run cache.
 */
static void
dts_load_run(DTS_DeadTidState *deadtidstate, int run)
{
	DTS_RunState	   *runstate = deadtidstate->deadtidrun;
	ItemPointerData	   *itemptrs = runstate->itemptrs;
	CBPageNo			startpage;
	CBPageNo			endpage;
	CBPageNo			prevpage;
	int					maxtidrun = runstate->maxtid / runstate->num_runs;
	int					nremaining = maxtidrun;

	startpage = runstate->nextpage[run];
	if (startpage == CB_INVALID_LOGICAL_PAGE)
	{
		runstate->runindex[run] = -1;
		return;
	}

	if (run < runstate->num_runs - 1)
		endpage = runstate->startpage[run + 1];
	else
		endpage = CB_INVALID_LOGICAL_PAGE;
	
	/* set itemptrs to the head of the requested run tids. */
	itemptrs += (maxtidrun * run);
	memset(itemptrs, 0, DTS_SizeOfDeadTids(maxtidrun));

	while(nremaining > 0)
	{
		int		ntids;

		ntids = DTS_ReadDeadtids(deadtidstate, startpage, endpage, nremaining,
								 itemptrs, &prevpage);
		if (ntids == 0)
			break;

		startpage = prevpage + 1;
		itemptrs += ntids;
		nremaining -= ntids;
	}

	if (prevpage == endpage)
		runstate->nextpage[run] = CB_INVALID_LOGICAL_PAGE;
}

/*
 * dts_merge_runs - process and merge vacuum runs from run cache.
 *
 * Pass through each vacuum run in the conveyor belt and load a few blocks from
 * starting of each block based on the cache size.
 */
static void
dts_merge_runs(DTS_DeadTidState *deadtidstate)
{
	DTS_RunState	   *runstate = deadtidstate->deadtidrun;
	ItemPointerData    *smallesttid = NULL;
	int		nRunCount;
	int		smallestrun;
	int		maxtidperrun;
	int		maxtid;
	int		ntids;
	int		preserve_tids;

	/*
	 * If this is not the first time we are merging then preserve the tids
	 * for the last block, so that if in previous merge we have loaded some
	 * tids for this block then this time we can get all.  This can be
	 * optimized if the block we are currently pruning is not the same as
	 * the last tid's block in cache then we can overwrite all the tids.
	 */
	if (deadtidstate->num_tids == -1)
	{
		maxtid = runstate->maxtid;
		preserve_tids = ntids = 0;
	}
	else
	{
		maxtid = runstate->maxtid - MaxHeapTuplesPerPage;
		preserve_tids = ntids = runstate->maxtid - maxtid;
		memcpy(deadtidstate->itemptrs,
			   &deadtidstate->itemptrs[deadtidstate->num_tids - ntids],
			   sizeof(ItemPointerData) * ntids);
	}

	maxtidperrun = runstate->maxtid/runstate->num_runs;

	/* Reload the cache from each run. */
	while (ntids < maxtid)
	{
		/*
		* Pass over each run and perform merge, if any run is out of data then
		* load more data from the conveyor belt. 
		*/
		for (nRunCount = 0; nRunCount < runstate->num_runs; nRunCount++)
		{
			ItemPointerData		*nexttid;

			if (runstate->runindex[nRunCount] == -1)
				continue;

			/* Get the next tid from this run. */
			nexttid = DTS_NextRunItem(runstate, nRunCount, maxtidperrun);

			/*
			 * If the current tid in the cache of the current run is invalid
			 * then load more tids into the cache from the conveyor belt.
			 */
			if (!ItemPointerIsValid(nexttid))
			{
				dts_load_run(deadtidstate, nRunCount);
				if (runstate->runindex[nRunCount] != -1)
				{
					runstate->runindex[nRunCount] = 0;
					nexttid = DTS_NextRunItem(runstate, nRunCount, maxtidperrun);
				}
			}

			if (ItemPointerIsValid(nexttid) &&
				(!ItemPointerIsValid(smallesttid) ||
				ItemPointerCompare(nexttid, smallesttid) < 0))
			{
				smallesttid = nexttid;
				smallestrun = nRunCount;
			}
		}

		/* If we did not find any vaid tid in this merge then we are done. */
		if (!ItemPointerIsValid(smallesttid))
			break;

		/* Go to the next index in the run which gave smallest tid. */
		(runstate->runindex[smallestrun])++;

		if (runstate->runindex[smallestrun] > maxtidperrun)
		{
			runstate->runindex[smallestrun] = 0;

			/* Load more data from the conveyor belt in this run. */
			dts_load_run(deadtidstate, smallestrun);
		}
		ItemPointerCopy(smallesttid, &deadtidstate->itemptrs[ntids]);
		smallesttid = NULL;
		ntids++;
	}

	deadtidstate->num_tids = ntids;

	/* FIXME: for debugging purpose. */
	elog(LOG, "merge run completed, next page info");

	for (nRunCount = 0; nRunCount < runstate->num_runs; nRunCount++)
	{
		elog(LOG, "run information, run = %d page = " UINT64_FORMAT,
			 nRunCount, runstate->nextpage[nRunCount]);
	}

	elog(LOG, "first tid in cache block=%d offset=%d",
			  ItemPointerGetBlockNumber(&deadtidstate->itemptrs[0]),
			  ItemPointerGetOffsetNumber(&deadtidstate->itemptrs[0]));

	elog(LOG, "last tid in cache block=%d offset=%d",
			  ItemPointerGetBlockNumber(&deadtidstate->itemptrs[ntids - 1]),
			  ItemPointerGetOffsetNumber(&deadtidstate->itemptrs[ntids - 1]));

	/* No more tids to be load from the run, so release the runstate. */
	if (preserve_tids == ntids)
	{
		if (runstate->itemptrs != NULL)
			pfree(runstate->itemptrs);
		if (runstate->startpage != NULL)
			pfree(runstate->startpage);			
		if (runstate->nextpage != NULL)
			pfree(runstate->nextpage);
		if (runstate->runindex != NULL)
			pfree(runstate->runindex);
		pfree(deadtidstate->deadtidrun);

		deadtidstate->deadtidrun = NULL;
	}
}

/*
 * dts_process_runs - Compute number of runs in deadtid and remember start
 * 					  pageno of each run. 
 */
static void
dts_process_runs(DTS_DeadTidState *deadtidstate)
{
	ConveyorBelt   *cb = deadtidstate->cb;
	DTS_RunState   *deadtidrun = deadtidstate->deadtidrun;
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;
	CBPageNo		curpageno;
	CBPageNo	   *next_run_page;
	int				maxrun = 1000;
	int				nRunCount = 0;

	/* Get conveyor belt page bounds. */
	ConveyorBeltGetBounds(cb, &oldest_pageno, &next_pageno);
	curpageno = oldest_pageno;

	/*
	 * Allocate next_run_page, this will hold the next page to load from each
	 * run.
	 */
	next_run_page = palloc(maxrun * sizeof(CBPageNo));
	deadtidrun->itemptrs =
				palloc0(sizeof(ItemPointerData) * deadtidrun->maxtid);

	/* Read page from the conveyor belt. */
	while(curpageno < next_pageno)
	{
		DTS_PageData   *deadtidpd;
		Buffer			buffer;
		Page			page;
		CBPageNo		nextrunpage;

		/* Read page from the conveyor belt. */
		buffer = ConveyorBeltReadBuffer(cb, curpageno, BUFFER_LOCK_SHARE,
										NULL);
		if (BufferIsInvalid(buffer))
			elog(ERROR, "invalid conveyor belt page" UINT64_FORMAT, curpageno);

		page = BufferGetPage(buffer);
		deadtidpd = (DTS_PageData *) PageGetSpecialPointer(page);
		nextrunpage = deadtidpd->nextrunpage;
		UnlockReleaseBuffer(buffer);

		/* If nextrun page is set then go to the next run. */
		if (nextrunpage != CB_INVALID_LOGICAL_PAGE)
		{
			/* If current memory is not enough then resize it. */
			if (nRunCount == maxrun)
			{
				maxrun *= 2;
				next_run_page = repalloc(next_run_page, maxrun * sizeof(CBPageNo));
			}

			next_run_page[nRunCount++] = curpageno;
			curpageno = nextrunpage;
		}
		else
			break;
	}

	Assert(nRunCount > 0);
	deadtidrun->num_runs = nRunCount;
	deadtidrun->startpage = palloc(nRunCount * sizeof(CBPageNo));
	deadtidrun->nextpage = palloc(nRunCount * sizeof(CBPageNo));
	deadtidrun->runindex = palloc0(nRunCount * sizeof(int));
	memcpy(deadtidrun->nextpage, next_run_page, nRunCount * sizeof(CBPageNo));
	memcpy(deadtidrun->startpage, next_run_page, nRunCount * sizeof(CBPageNo));
	pfree(next_run_page);

	/* FIXME: for debugging purpose. */
	elog(LOG, "processed vacuum runs, run count = %d", nRunCount);
	for (nRunCount = 0; nRunCount < deadtidrun->num_runs; nRunCount++)
	{
		elog(LOG, "run information, run = %d page = " UINT64_FORMAT,
			 nRunCount, deadtidrun->nextpage[nRunCount]);
	}
}
