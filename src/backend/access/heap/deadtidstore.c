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
 * State of the cache over the vacuum runs in the conveyor belt.  Druring
 * vacuum first pass we need to check if the deadtid is already present in
 * the conveyor belt then don't add it again.  For that we need to load the
 * existing conveyor belt data in cache for checking the duplicates.  So
 * instead of sorting complete conveyor belt data using tuple sort we already
 * know that each conveyor belt runs are already sorted, so we load a few
 * logical pages from each conveyor belt and perform merge sort over those.
 * So this structure maintain the current state of the cache over each runs.
 * and also the deadtids from each run.
 */
typedef struct DTS_RunState
{
	/* Total number of runs in the conveyor belt. */
	int		num_runs;

	/* Max number tids we can load into the cache from all runs. */
	int		maxtid;

	/*
	 * During merge of each run, this maintains the current deadtid index
	 * in to the cache for each runs.
	 */
	int	   *runindex;

	/* Starting logical conveyor belt pageno for each run. */
	CBPageNo		   *startpage;

	/* Next logical conveyor belt pageno to load for each run. */
	CBPageNo		   *nextpage;

	/*
	 * Current cache tids for each run.  This is not completely sorted but
	 * each run is sorted and we devide this cache size equally for the runs
	 * so we directly what is the start tid for each run.  These runs are
	 * merged into a different cache DTS_DeadTidState->itemptrs which is
	 * complete sorted cache and used for duplicate detection.
	 */
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
	ItemPointerData	   *itemptrs;		/* dead tids for the current scan block */
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

#define DTS_NextRunItem(runstate, run, maxtidperrun) \
	((runstate)->itemptrs + ((run) * (maxtidperrun) + (runstate)->runindex[(run)]))

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

	/*
	 * If the oldest_page is same as next page then there is no data in the
	 * conveyor belt, simmiarlly if oldest page is same as the start page of
	 * this run then there is no previous runs in the conveyor belt so we don't
	 * need to worry about checking the duplicates.
	 */
	if ((oldest_pageno == next_pageno) ||
		(oldest_pageno == deadtidstate->start_page))
		return;

	/* If deadtid cache is not yet intialize then do it now. */
	if (deadtidstate->num_tids == -1)
	{
		/* Allocate space for deadtid cache. */
		deadtidstate->itemptrs = (ItemPointerData *) palloc(
									DTS_SizeOfDeadTids(MaxHeapTuplesPerPage));

		/*
		 * Compute the number of runs available in the conveyor belt and check
		 * whether we have enough cache to load at least one block from each
		 * run, if so then we can peroform in-memory merge of the runs.
		 */
		deadtidstate->deadtidrun = palloc0(sizeof(DTS_RunState));
		deadtidstate->deadtidrun->maxtid = maxtids;
		dts_process_runs(deadtidstate);
	}

	/*
	 * Check the current block number in the deadtid cache and the block is
	 * smaller than the block we are processing now then load the data for
	 * the next block.
	 */
	if (deadtidstate->num_tids > 0 &&
		ItemPointerGetBlockNumber(&deadtidstate->itemptrs[0]) >= blkno)
		return;
	else
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
 * DTS_ReleaseDeadTidState - Cleanup the deadtid state and also set the next
 * 							 vacuum run page in the conveyor belt
 */
void
DTS_ReleaseDeadTidState(DTS_DeadTidState *deadtidstate)
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

	/* cleanup and release the deadtid state. */
	if (deadtidstate->deadtidrun != NULL)
	{
		DTS_RunState   *deadtidrun = deadtidstate->deadtidrun;

		if (deadtidrun->itemptrs != NULL)
			pfree(deadtidrun->itemptrs);
		if (deadtidrun->startpage != NULL)
			pfree(deadtidrun->startpage);
		if (deadtidrun->nextpage != NULL)
			pfree(deadtidrun->nextpage);
		if (deadtidrun->runindex != NULL)
			pfree(deadtidrun->runindex);

		pfree(deadtidrun);
	}
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

	runstate->runindex[run] = 0;

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

	if (prevpage >= endpage)
		runstate->nextpage[run] = CB_INVALID_LOGICAL_PAGE;
	else if (startpage == prevpage)
		runstate->nextpage[run] = prevpage;
	else
		runstate->nextpage[run] = prevpage + 1;
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
	BlockNumber			blkno = InvalidBlockNumber;
	int		nRunCount;
	int		smallestrun;
	int		maxtidperrun;
	int		ntids = 0;

	/*
	 * If we can not pull at least one block from each run then we can not
	 * perform the in memory merge so we will have to perform disk based
	 * merging.  Which is not yet implemented.
	 */
	if (runstate->num_runs * DTS_MaxTidsPerPage > runstate->maxtid)
	{
		elog(ERROR, "disk based merging is not yet implemeted");
	}

	/* Compute the max tids can be fit into each run cache. */
	maxtidperrun = runstate->maxtid / runstate->num_runs;

	/* Load data for the next block from the cache. */
	while (1)
	{
		/*
		 * Pass over each run and perform merge, if any run is out of data then
		 * load more data from the conveyor belt.
		 */
		for (nRunCount = 0; nRunCount < runstate->num_runs; nRunCount++)
		{
			ItemPointerData		*nexttid = NULL;

			if (runstate->runindex[nRunCount] == -1)
				continue;

			/* Get the next tid from this run. */
			if (runstate->runindex[nRunCount] < maxtidperrun)
				nexttid = DTS_NextRunItem(runstate, nRunCount, maxtidperrun);

			/*
			 * If the current tid in the cache of the current run is invalid
			 * then load more tids into the cache from the conveyor belt.
			 */
			if (!ItemPointerIsValid(nexttid))
			{
				dts_load_run(deadtidstate, nRunCount);
				if (runstate->runindex[nRunCount] != -1)
					nexttid = DTS_NextRunItem(runstate, nRunCount, maxtidperrun);
			}

			/* If we did not get a valid tid from this run then continue. */
			if (!ItemPointerIsValid(nexttid))
				continue;

			/* Set the smallest tid. */
			if (!ItemPointerIsValid(smallesttid) ||
				ItemPointerCompare(nexttid, smallesttid) < 0)
			{
				smallesttid = nexttid;
				smallestrun = nRunCount;
			}
		}

		/* If we did not find any vaid tid in this merge then we are done. */
		if (!ItemPointerIsValid(smallesttid))
			break;

		/* This tid is for another block so we are done. */
		if (!BlockNumberIsValid(blkno))
			blkno = ItemPointerGetBlockNumber(smallesttid);
		else if (ItemPointerGetBlockNumber(smallesttid) != blkno)
			break;

		/* Go to the next index in the run which gave smallest tid. */
		(runstate->runindex[smallestrun])++;

		ItemPointerCopy(smallesttid, &deadtidstate->itemptrs[ntids]);
		smallesttid = NULL;
		ntids++;
		Assert(ntids <= MaxHeapTuplesPerPage);
	}

	deadtidstate->num_tids = ntids;
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

	/*
	 * Set startpage and next page for each run.  Initially both will be same
	 * but as we pull tid from the conveyor belt the nextpage for each run
	 * will move to the next page to be fetched from the conveyor belt
	 * whereas the statspage will remain the same throughout the run.
	 */
	memcpy(deadtidrun->startpage, next_run_page, nRunCount * sizeof(CBPageNo));
	memcpy(deadtidrun->nextpage, next_run_page, nRunCount * sizeof(CBPageNo));
	pfree(next_run_page);
}
