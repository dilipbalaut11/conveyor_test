/*-------------------------------------------------------------------------
 *
 * vacuumdeaditem.c
 *	  Support functions to store and fetch dead items during vacuum
 *
 *
 * Create dead item fork and manage storing and retriving deaditems using
 * conveyor belt infrastructure.
 *
 * src/backend/access/heap/vacuumdeaditem.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/conveyor.h"
#include "access/vacuumdeaditem.h"
#include "access/htup_details.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Whenever we perform the first pass of the vacuum we flush the dead items to
 * the conveyor belt multiple times, but the complete pass over the heap is
 * considered as a single vacuum run in the conveyor belt and all the dead
 * items of a single run is in sorted order because during vacuum we scan and
 * prune the heap in the block order.
 *
 * So while performing the first vacuum pass it is also possible that we
 * encounter the same dead item again which we already inserted into the
 * conveyor belt in one of the previous vacuum run.  But there is no way to
 * identify whether that is already there in the conveyor or not except for
 * checking it in the conveyor belt itself.  However the whole conveyor belt
 * data might not fit into the maintence_work_mem and that makes it difficult
 * to search an individual item in the conveyor belt.
 *
 * Since we know the fact that the individual vacuum runs in the conveyor belt
 * are sorted so we can perform a merge over each run in order to generate
 * a sorted data.  For that we maintain a cache over each run and pull a couple
 * of pages in the cache from each run and then merge them into a block-level
 * cache.  The block-level cache is a very small cache that only holds the
 * items for the current block which we are going to prune in the vacuum.
 *
 * This data structure maintains the current state of the cache over the
 * previous vacuum runs.
 */
typedef struct VDI_RunState
{
	int		num_runs;

	/*
	 * Current position of each run w.r.t the conveyor belt.
	 *
	 * startpage keep accound for the first conveyor belt pageno and the
	 * nextpage keep account for the next page to be fetched from the conveyor
	 * belt for each run.
	 */
	CBPageNo   *startpage;
	CBPageNo   *nextpage;

	/*
	 * Cache information over each run.
	 *
	 * runindex maintain the current index into the cache over each run.  The
	 * max_items is the total items can fit into the complete itemptrs cache.
	 * The cache is equally divided among all runs.  However this could be
	 * optimized such that instead of equally dividing we can maintain some
	 * offset for each run inside this cache so we don't consume equal space if
	 * some run are really small and don't need that much space.
	 */
	int	   *runindex;
	int		max_items;
	ItemPointerData	   *itemptrs;
} VDI_RunState;

/*
 * This maintains the state over the conveyor belt for a single vacuum pass.
 * This tracks the start and last page of the conveyor belt for the current run
 * and at the end of the current run, the last page will be updated in the
 * special space of the first page of the run so that we knows the boundary for
 * each vacuum run in the conveyor belt.
 */
struct VDI_DeadItemState
{
	/* Conveyor belt reference. */
	ConveyorBelt   *cb;

	/*
	 * Current vacuum tracking
	 *
	 * During prune vacuum pass we can flush the dead items to the conveyor belt
	 * in multiple small check.  So the start_page remember what is the first
	 * conveyor belt pageno where we started flushing and the last_page keep
	 * track of the last conveyor belt into which we flushed.  At the end of
	 * the vacuum pass we will set the last_page into the special space of the
	 * first page for marking the vacuum run boundary.
	 */
	CBPageNo		start_page;
	CBPageNo		last_page;

	/*
	 * Members for maintaning cache for duplicate check.
	 *
	 * completed is mark true if there is no more data to be fetched from the
	 * deaditemrun cache, and num_items is the number dead items loaded into the
	 * items array.  We don't need to maintain the block number because in this
	 * cache at a time we holds items only for a single block.
	 */
	bool			completed;
	int				num_items;
	ItemPointerData	   *itemptrs;

	/* Dead item run cache state. */
	VDI_RunState   *deaditemrun;
};

/* Max dead items per conveyor belt page. */
#define VDI_MaxItemsPerPage \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - sizeof(VDI_PageData)) / sizeof(ItemPointerData)

/* Compute the number of deaditems in given conveyor belt page. */
#define VDI_ItemsInPage(page) \
	(((PageHeader) (page))->pd_lower -  MAXALIGN(SizeOfPageHeaderData)) / sizeof(ItemPointerData)

#define VDI_SizeOfDeadItems(cnt) \
			 mul_size(sizeof(ItemPointerData), cnt)

#define VDI_NextRunItem(runstate, run, maxitemperrun) \
	((runstate)->itemptrs + ((run) * (maxitemperrun) + (runstate)->runindex[(run)]))


/* Initial number of vacuum runs in the conveyor belt. */
#define VDI_NUM_VACUUM_RUN		100

/* non-export function prototypes */
static void vdi_page_init(Page page);
static int32 vdi_itemcmp(const void *a, const void *b);
static void vdi_load_run(VDI_DeadItemState *deaditemstate, int run);
static void vdi_merge_runs(VDI_DeadItemState *deaditemstate);
static void vdi_process_runs(VDI_DeadItemState *deaditemstate);

/*
 * VDI_InitDeadItemState - intialize the deaditem state
 *
 * This create new deaditem fork for the given relation and initialize the
 * conveyor belt.  The deaditem state will maintain the current state of
 * insertion/read from the conveyor belt.
 */
VDI_DeadItemState *
VDI_InitDeadItemState(Relation rel)
{
	SMgrRelation		reln;
	ConveyorBelt	   *cb;
	VDI_DeadItemState   *deaditemstate;

	/* Open the relation at smgr level. */
	reln = RelationGetSmgr(rel);

	/* Allocate memory for deaditemstate. */
	deaditemstate = palloc0(sizeof(VDI_DeadItemState));

	/*
	 * If the DEADTID_FORKNUM doesn't exist then create it and initialize the
	 * conveyor belt, otherwise just open the conveyor belt.
	 */
	if (!smgrexists(reln, DEADTID_FORKNUM))
	{
		smgrcreate(reln, DEADTID_FORKNUM, false);
		cb = ConveyorBeltInitialize(rel,
									DEADTID_FORKNUM,
									1024,	/* XXX What is the best value ?*/
									CurrentMemoryContext);
	}
	else
		cb = ConveyorBeltOpen(rel, DEADTID_FORKNUM, CurrentMemoryContext);

	/* Initialize the dead item state. */
	deaditemstate->cb = cb;
	deaditemstate->start_page = CB_INVALID_LOGICAL_PAGE;
	deaditemstate->last_page = CB_INVALID_LOGICAL_PAGE;
	deaditemstate->num_items = -1;
	deaditemstate->completed = false;

	return deaditemstate;
}

/*
 * VDI_InsertDeadItems() -- Insert given dead items into the conveyor belt.
 */
void
VDI_InsertDeadItems(VDI_DeadItemState *deaditemstate, int nitems,
					ItemPointerData *deaditems)
{
	ConveyorBelt   *cb = deaditemstate->cb;
	CBPageNo		pageno;

	/* Loop until we flush out all the dead items into the conveyor belt. */
	while(nitems > 0)
	{
		Buffer	buffer;
		Page	page;
		char   *pagedata;
		int		count;
		PageHeader	phdr;

		/* Get a new page from the ConveyorBelt. */
		buffer = ConveyorBeltGetNewPage(cb, &pageno);
		page = BufferGetPage(buffer);
		vdi_page_init(page);

		phdr = (PageHeader) page;
		pagedata = PageGetContents(page);

		/* Compute how many dead items we can store into this page. */
		count =  Min(VDI_MaxItemsPerPage, nitems);

		START_CRIT_SECTION();

		/*
		 * Copy the data into the page and set the pd_lower.  We are just
		 * copying ItemPointerData array so we don't need any alignment which
		 * should be SHORTALIGN and sizeof(ItemPointerData) will take care of
		 * that so we don't need any other alignment.
		 */
		memcpy(pagedata, deaditems, sizeof(ItemPointerData) * count);
		phdr->pd_lower += sizeof(ItemPointerData) * count;
		ConveyorBeltPerformInsert(cb, buffer);

		END_CRIT_SECTION();

		ConveyorBeltCleanupInsert(cb, buffer);

		/* Set the start page for this run if not yet set. */
		if (deaditemstate->start_page == CB_INVALID_LOGICAL_PAGE)
			deaditemstate->start_page = pageno;

		/* Adjust deaditem pointer and the remaining item count. */
		deaditems += count;
		nitems -= count;
	}

	deaditemstate->last_page = pageno;
}

/*
 * VDI_ReadDeadItems - Read dead items from the conveyor belt
 *
 * 'from_pageno' is the conveyor belt page number from where we want to start
 * and the 'to_pageno' upto which we want to read.
 * 'deaditemss' is pre allocated array for holding the dead items, the
 * allocated size is sufficient to hold 'maxitems'.
 *
 * On successful read last page number we have read from conveyor belt will be
 * stored into the '*last_pageread'.
 *
 * Returns number of dead items actually read from the conveyor belt.  Should
 * always be <= maxitems.
 */
int
VDI_ReadDeadItems(VDI_DeadItemState *deaditemstate, CBPageNo from_pageno,
				  CBPageNo to_pageno, int maxitems, ItemPointerData *deaditems,
				  CBPageNo *last_pageread)
{
	ConveyorBelt   *cb = deaditemstate->cb;
	Page			page;
	Buffer			buffer;
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;
	int				count;
	int				ndeaditems = 0;
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

	/* Loop until we read maxitems. */
	while(ndeaditems < maxitems && from_pageno < next_pageno)
	{
		/* Read page from the conveyor belt. */
		buffer = ConveyorBeltReadBuffer(cb, from_pageno, BUFFER_LOCK_SHARE,
										NULL);
		if (BufferIsInvalid(buffer))
			break;

		page = BufferGetPage(buffer);
		count = VDI_ItemsInPage(page);

		/*
		 * Don't stop reading in middle of the page, so if remaning items to
		 * read are less than the item in this page then stop here.
		 */
		if (maxitems - ndeaditems < count)
		{
			UnlockReleaseBuffer(buffer);
			break;
		}

		/* Read data into the deaditem buffer. */
		memcpy(deaditems + ndeaditems, PageGetContents(page),
			   sizeof(ItemPointerData) * count);
		UnlockReleaseBuffer(buffer);

		/* 
		 * Update the deaditem count we read and remember the previous last
		 * conveyor belt page we read.
		 */
		ndeaditems += count;
		prevpageno = from_pageno;

		/* Go to the next page. */
		from_pageno++;
	}

	*last_pageread = prevpageno;

	return ndeaditems;
}

/*
 * VDI_LoadDeadItems - Load dead items from conveyor belt for given block.
 *
 * 'blkno' is current heap block we are going to scan, so this function need to
 * ensure that we have all the previous dead items loaded into the cache for
 * for this block from the conveyor belt.
 *
 * This is to ensure that when we are adding dead items to the conveyor belt
 * we don't add the duplicate items.
 */
void
VDI_LoadDeadItems(VDI_DeadItemState *deaditemstate, BlockNumber blkno,
				 int maxitems)
{
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;

	Assert(deaditemstate != NULL);

	if (deaditemstate->num_items == 0)
		return;

	/* Get conveyor belt page range. */
	ConveyorBeltGetBounds(deaditemstate->cb, &oldest_pageno, &next_pageno);

	/*
	 * If the oldest_page is same as next page then there is no data in the
	 * conveyor belt, simmiarlly if oldest page is same as the start page of
	 * this run then there is no previous runs in the conveyor belt so we don't
	 * need to worry about checking the duplicates.
	 */
	if (oldest_pageno == next_pageno ||
		deaditemstate->start_page == oldest_pageno)
		return;

	/* If deaditem cache is not yet intialize then do it now. */
	if (deaditemstate->num_items == -1)
	{
		/* Allocate space for deaditem cache. */
		deaditemstate->itemptrs = (ItemPointerData *) palloc(
									VDI_SizeOfDeadItems(MaxHeapTuplesPerPage));

		/*
		 * Compute the number of runs available in the conveyor belt and check
		 * whether we have enough cache to load at least one block from each
		 * run, if so then we can peroform in-memory merge of the runs.
		 */
		deaditemstate->deaditemrun = palloc0(sizeof(VDI_RunState));
		deaditemstate->deaditemrun->max_items = maxitems;
		vdi_process_runs(deaditemstate);
	}

	/*
	 * Check the current block number in the deaditem cache and the block is
	 * smaller than the block we are processing now then load the data for
	 * the next block.
	 */
	if (deaditemstate->num_items > 0 &&
		ItemPointerGetBlockNumber(&deaditemstate->itemptrs[0]) >= blkno)
		return;
	else
		vdi_merge_runs(deaditemstate);
}

/*
 * VDI_DeadItemExists - check whether the item already exists in conveyor belt.
 *
 * Returns true if the item already present in the conveyor belt, false
 * otherwise.
 */
bool
VDI_DeadItemExists(VDI_DeadItemState *deaditemstate, ItemPointerData *item)
{
	if (deaditemstate == NULL || deaditemstate->itemptrs == NULL)
		return false;

	return bsearch((void *) item, (void *) deaditemstate->itemptrs,
				   deaditemstate->num_items, sizeof(ItemPointerData),
				   vdi_itemcmp);
}

/*
 * VDI_SaveVacuumRun - Save the current vacuum run.
 *
 * Save the current vacuum run by storing the last page of the run into the
 * special space of the first page of the run.
 */
void
VDI_SaveVacuumRun(VDI_DeadItemState *deaditemstate)
{
	Buffer			buffer;
	Page			page;
	VDI_PageData   *deaditempd;

	/* There is no data in this run so nothing to be done. */
	if (deaditemstate->start_page == CB_INVALID_LOGICAL_PAGE)
		return;

	/* Read page from the conveyor belt. */
	buffer = ConveyorBeltReadBuffer(deaditemstate->cb, deaditemstate->start_page,
									BUFFER_LOCK_EXCLUSIVE, NULL);
	if (BufferIsInvalid(buffer))
		elog(ERROR, "invalid conveyor belt pageno " UINT64_FORMAT,
			 deaditemstate->start_page);

	page = BufferGetPage(buffer);
	deaditempd = (VDI_PageData *) PageGetSpecialPointer(page);
	deaditempd->nextrunpage = deaditemstate->last_page + 1;

	/* TODO - WAL log this operation */

	UnlockReleaseBuffer(buffer);
}

/*
 * VDI_ReleaseDeadItemState - Release the dead item state.
 *
 * Release memory for the deaditem state and its members.
 */
void
VDI_ReleaseDeadItemState(VDI_DeadItemState *deaditemstate)
{
	/* cleanup and release the deaditem state. */
	if (deaditemstate->deaditemrun != NULL)
	{
		VDI_RunState   *deaditemrun = deaditemstate->deaditemrun;

		if (deaditemrun->itemptrs != NULL)
			pfree(deaditemrun->itemptrs);
		if (deaditemrun->startpage != NULL)
			pfree(deaditemrun->startpage);			
		if (deaditemrun->nextpage != NULL)
			pfree(deaditemrun->nextpage);
		if (deaditemrun->runindex != NULL)
			pfree(deaditemrun->runindex);

		pfree(deaditemrun);
	}
	if (deaditemstate->itemptrs)
		pfree(deaditemstate->itemptrs);

	pfree(deaditemstate);
}

/*
 * VDI_HasDeadItems - check do we have any dead items in the conveyor belt.
 *
 */
bool
VDI_HasDeadItems(VDI_DeadItemState *deaditemstate)
{
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;

	Assert(deaditemstate != NULL);

	/* Get conveyor belt page range. */
	ConveyorBeltGetBounds(deaditemstate->cb, &oldest_pageno, &next_pageno);

	/*
	 * If the oldest_page is same as next page then there is no data in the
	 * conveyor belt.
	 */
	if (oldest_pageno == next_pageno)
		return false;

	Assert(oldest_pageno < next_pageno);

	return true;
}

/*
 * VDI_GetLastPage - Returns last conveyor belt page.
 */
CBPageNo
VDI_GetLastPage(VDI_DeadItemState *deaditemstate)
{
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;

	Assert(deaditemstate != NULL);

	/* Get conveyor belt page range. */
	ConveyorBeltGetBounds(deaditemstate->cb, &oldest_pageno, &next_pageno);

	return next_pageno - 1;
}

/*
 * VDI_GetOldestPage - Returns oldest conveyor belt page.
 */
CBPageNo
VDI_GetOldestPage(VDI_DeadItemState *deaditemstate)
{
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;

	Assert(deaditemstate != NULL);

	/* Get conveyor belt page range. */
	ConveyorBeltGetBounds(deaditemstate->cb, &oldest_pageno, &next_pageno);

	return oldest_pageno;
}

/*
 * VDI_Vacuum - truncate and vacuum the underlying conveyor belt.
 */
void
VDI_Vacuum(VDI_DeadItemState *deaditemstate, CBPageNo pageno)
{
	/* Truncate the conveyor belt pages which we have already processed. */
	ConveyorBeltLogicalTruncate(deaditemstate->cb, pageno);

	/* Vacuum the conveyor belt. */
	ConveyorBeltVacuum(deaditemstate->cb);
}

/*
 * vdi_page_init - Initialize an deaditem page.
 */
static void
vdi_page_init(Page page)
{
	PageHeader	p = (PageHeader) page;
	VDI_PageData	   *deaditempd;

	PageInit(page, BLCKSZ, sizeof(VDI_PageData));
	deaditempd = (VDI_PageData *) PageGetSpecialPointer(page);
	deaditempd->nextrunpage = CB_INVALID_LOGICAL_PAGE;

	/* Start writing data from the maxaligned offset. */
	p->pd_lower = MAXALIGN(SizeOfPageHeaderData);
}

/*
 * vdi_itemcmp() - Compare two item pointers, return -1, 0, or +1.
 */
static int32
vdi_itemcmp(const void *a, const void *b)
{
	ItemPointer iptr1 = ((const ItemPointer) a);
	ItemPointer iptr2 = ((const ItemPointer) b);

	return ItemPointerCompare(iptr1, iptr2);
}

/*
 * vdi_load_run - load data from conveyor belt vacuum run into the run cache.
 *
 * Pull more data from the conveyor belt into the cache.  This will pull the
 * data only for the input vacuum 'run'.
 */
static void
vdi_load_run(VDI_DeadItemState *deaditemstate, int run)
{
	VDI_RunState	   *runstate = deaditemstate->deaditemrun;
	ItemPointerData	   *itemptrs = runstate->itemptrs;
	CBPageNo			startpage;
	CBPageNo			endpage;
	CBPageNo			prevpage;
	int					maxitemrun = runstate->max_items / runstate->num_runs;
	int					nremaining = maxitemrun;

	/*
	 * If the next conveyor belt page to be processed for this run is set to
	 * CB_INVALID_LOGICAL_PAGE that means we have already pulled all the data
	 * from this run.  So set the runindex for this run to -1 so that next
	 * time the merge will skip this run.
	 */
	prevpage = startpage = runstate->nextpage[run];
	if (startpage == CB_INVALID_LOGICAL_PAGE)
	{
		runstate->runindex[run] = -1;
		return;
	}

	/* Refilling this run's cache with new data so reset the runoffset. */
	runstate->runindex[run] = 0;	

	if (run < runstate->num_runs - 1)
		endpage = runstate->startpage[run + 1];
	else
		endpage = CB_INVALID_LOGICAL_PAGE;
	
	/* set itemptrs to the head of the requested run items. */
	itemptrs += (maxitemrun * run);
	memset(itemptrs, 0, VDI_SizeOfDeadItems(maxitemrun));

	while(nremaining > 0)
	{
		int		nitems;

		nitems = VDI_ReadDeadItems(deaditemstate, startpage, endpage,
								   nremaining, itemptrs, &prevpage);
		if (nitems == 0)
			break;

		startpage = prevpage + 1;
		itemptrs += nitems;
		nremaining -= nitems;
	}

	if (prevpage >= endpage)
		runstate->nextpage[run] = CB_INVALID_LOGICAL_PAGE;
	else if (startpage == prevpage)
		runstate->nextpage[run] = prevpage;
	else
		runstate->nextpage[run] = prevpage + 1;
}

/*
 * vdi_merge_runs - process and merge vacuum runs from run cache.
 *
 * Pass through each vacuum run in the conveyor belt and load a few blocks from
 * starting of each vacuum runs based on the cache size.
 */
static void
vdi_merge_runs(VDI_DeadItemState *deaditemstate)
{
	VDI_RunState	   *runstate = deaditemstate->deaditemrun;
	ItemPointerData    *smallestitem = NULL;
	BlockNumber			blkno = InvalidBlockNumber;
	int		nRunCount;
	int		smallestrun;
	int		maxitemperrun;
	int		nitems = 0;

	/*
	 * If we can not pull at least one block from each run then we can not
	 * perform the in memory merge so we will have to perform disk based
	 * merging.  Which is not yet implemented.
	 *
	 * XXX maybe instead of pulling whole conveyor belt page we can consider
	 * that at least if we can pull all the offset w.r.t. one block header then
	 * also we can consider doing the in-memory sort merge.
	 */
	if (runstate->num_runs * VDI_MaxItemsPerPage > runstate->max_items)
	{
		elog(ERROR, "disk based merging is not yet implemeted");
	}

	/* Compute the max items can be fit into each run cache. */
	maxitemperrun = runstate->max_items / runstate->num_runs;

	/* Load data for the next block from the cache. */
	while (1)
	{
		/*
		 * Pass over each run and perform merge, if any run is out of data then
		 * load more data from the conveyor belt.
		 */
		for (nRunCount = 0; nRunCount < runstate->num_runs; nRunCount++)
		{
			ItemPointerData		*nextitem = NULL;

			if (runstate->runindex[nRunCount] == -1)
				continue;

			/* Get the next item from this run. */
			if (runstate->runindex[nRunCount] < maxitemperrun)
				nextitem = VDI_NextRunItem(runstate, nRunCount, maxitemperrun);

			/*
			 * If the current item in the cache of the current run is invalid
			 * then load more items into the cache from the conveyor belt.
			 */
			if (!ItemPointerIsValid(nextitem))
			{
				vdi_load_run(deaditemstate, nRunCount);
				if (runstate->runindex[nRunCount] != -1)
					nextitem = VDI_NextRunItem(runstate, nRunCount, maxitemperrun);
			}

			/* If we did not get a valid item from this run then continue. */
			if (!ItemPointerIsValid(nextitem))
				continue;

			/* Set the smallest item. */
			if (!ItemPointerIsValid(smallestitem) ||
				ItemPointerCompare(nextitem, smallestitem) < 0)
			{
				smallestitem = nextitem;
				smallestrun = nRunCount;
			}
		}

		/* If we did not find any vaid item in this merge then we are done. */
		if (!ItemPointerIsValid(smallestitem))
			break;

		/* This item is for another block so we are done. */
		if (!BlockNumberIsValid(blkno))
			blkno = ItemPointerGetBlockNumber(smallestitem);
		else if (ItemPointerGetBlockNumber(smallestitem) != blkno)
			break;					

		/* Go to the next index in the run which gave smallest item. */
		(runstate->runindex[smallestrun])++;

		ItemPointerCopy(smallestitem, &deaditemstate->itemptrs[nitems]);
		smallestitem = NULL;
		nitems++;
		Assert(nitems <= MaxHeapTuplesPerPage);
	}

	deaditemstate->num_items = nitems;
}

/*
 * vdi_process_runs - Compute number of vacuums runs in conveyor belt.
 *
 * Pass over the vacuum run in the conveyor belt and remember the start page
 * for each vacuum run.  Later vdi_load_run() will maintain cache over each
 * run and pull the data from each run as and when needed.
 */
static void
vdi_process_runs(VDI_DeadItemState *deaditemstate)
{
	ConveyorBelt   *cb = deaditemstate->cb;
	VDI_RunState   *deaditemrun = deaditemstate->deaditemrun;
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;
	CBPageNo		curpageno;
	CBPageNo	   *next_run_page;
	int				maxrun = VDI_NUM_VACUUM_RUN;
	int				nRunCount = 0;
	int				nelement;

	/* Get conveyor belt page bounds. */
	ConveyorBeltGetBounds(cb, &oldest_pageno, &next_pageno);
	curpageno = oldest_pageno;

	/*
	 * Allocate next_run_page, this will hold the next page to load from each
	 * run.  Initially start with VDI_NUM_VACUUM_RUN and while processing the
	 * runs, if the number of runs are more that this value then expand the
	 * memory.
	 */
	maxrun = VDI_NUM_VACUUM_RUN;
	next_run_page = palloc(maxrun * sizeof(CBPageNo));
	deaditemrun->itemptrs = palloc0(sizeof(ItemPointerData) *
								   deaditemrun->max_items);			

	/* Read page from the conveyor belt. */
	while(curpageno < next_pageno)
	{
		VDI_PageData   *deaditempd;
		Buffer			buffer;
		Page			page;
		CBPageNo		nextrunpage;

		/* Read page from the conveyor belt. */
		buffer = ConveyorBeltReadBuffer(cb, curpageno, BUFFER_LOCK_SHARE,
										NULL);
		if (BufferIsInvalid(buffer))
			elog(ERROR, "invalid conveyor belt page" UINT64_FORMAT, curpageno);

		page = BufferGetPage(buffer);
		deaditempd = (VDI_PageData *) PageGetSpecialPointer(page);
		nextrunpage = deaditempd->nextrunpage;
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

	/*
	 * If we are in this function we must have at least one previous vacuum
	 * run, otherwise caller should have exited before only based on conveyor
	 * belt page bounds.
	 */
	Assert(nRunCount > 0);

	/*
	 * We have the start page for each run and the end page can be computed
	 * as the start page of the next run so only for the last run we need to
	 * store one extra element which will be treated as a end page for the
	 * last run.
	 */
	nelement = nRunCount + 1;
	next_run_page[nRunCount] = curpageno;
	deaditemrun->num_runs = nRunCount;
	deaditemrun->startpage = palloc(nelement * sizeof(CBPageNo));
	deaditemrun->nextpage = palloc(nelement * sizeof(CBPageNo));
	deaditemrun->runindex = palloc0(nelement * sizeof(int));

	/*
	 * Set startpage and next page for each run.  Initially both will be same
	 * but as we pull data from the conveyor belt the nextpage for each run
	 * will move to the next page to be fetched from the conveyor belt whereas
	 * the startpage will remain the same throughout complete vacuum pass.
	 */
	memcpy(deaditemrun->startpage, next_run_page, nelement * sizeof(CBPageNo));
	memcpy(deaditemrun->nextpage, next_run_page, nelement * sizeof(CBPageNo));

	pfree(next_run_page);	
}
