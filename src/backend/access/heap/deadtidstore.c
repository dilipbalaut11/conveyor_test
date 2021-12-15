/*-------------------------------------------------------------------------
 *
 * deadtidstore.c
 *	  Wrapper over conveyor belt to store and fetch deadtids.
 *
 * Create dead tid fork and manage storing and retriving deadtids using
 * conveyor belt infrastructure.
 *
 * src/backend/access/heap/deadtidstore.c
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
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Whenever we perform the first pass of the vacuum we flush the dead items to
 * the conveyor belt multiple times, but the complete pass over the heap is
 * considered as a single vacuum run in the conveyor belt and all the tids of a
 * single run is in sorted order because during vacuum we scan and prune the
 * heap in the block order.
 *
 * So while performing the first vacuum pass it is also possible that we
 * encounter the same deadtid again which we already inserted into the conveyor
 * belt in one of the previous vacuum run.  But there is no way to identify
 * whether that is already there in the conveyor or not except for checking it
 * in the conveyor belt itself.  However the whole conveyor belt data might
 * not fit into the maintence_work_mem and that makes it difficult to search
 * individual item in the conveyor belt.
 *
 * But since we know the fact that the individual vacuum runs in the conveyor
 * belt are sorted so we can perform a merge over each run in order to generate
 * a sorted data.  For that we maintain a cache over each run and pull a couple
 * of pages in the cache from each run and then merge them into a block-level
 * cache.  The block-level cache is a very small cache that only holds the
 * offsets for the current block which we are going to prune in the vacuum.
 *
 * This data structure maintains the current state of the cache over the
 * previous vacuum runs.
 */
typedef struct DTS_RunState
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
	 * runoffset maintain the current offset into the cache over each run.
	 * We do not maintain the exact offset in this instead we keep the offset
	 * of the next block header.  And for fetching the offsets within a block
	 * header we use a local index.  Total size of the cache is stored in
	 * cachesize and the cache is the actual cache holds data from each run.
	 * This cache is equally divided among all runs.  However this could be
	 * ptimized such that instead of equally dividing we can maintain some
	 * offset for each run inside this cache so we don't consume
	 * equal space if some run are really small and don't need that much space.
	 */
	int	   *runoffset;
	Size	cachesize;
	char   *cache;
} DTS_RunState;

/*
 * This maintains the state over the conveyor belt for a single vacuum pass.
 * This tracks the start and last page of the conveyor belt for the current run
 * and at the end of the current run, the last page will be updated in the
 * special space of the first page of the run so that we knows the boundary for
 * each vacuum run in the conveyor belt.
 */
struct DTS_DeadTidState
{
	/* Conveyor belt reference. */
	ConveyorBelt   *cb;

	/*
	 * Current vacuum tracking
	 *
	 * During prune vacuum pass we can flush the dead tids to the conveyor belt
	 * in multiple small check.  So the start_page remember what is the first
	 * conveyor belt pageno where we started flushing and the last_page keep
	 * track of the last conveyor belt into which we flushed.  At the end of
	 * the vacuum pass we will set the last_page into the special space of the
	 * first page for marking the vacuum run boundary.  Minimum conveyor belt
	 * pageno upto which vacuum is done for all indexes is stored into
	 * min_idxvac_page, so that during prune phase if we identify that some
	 * items are already there in the conveyor belt and the index vacuum is
	 * also done the we mark that item unused as well.
	 */
	CBPageNo		start_page;
	CBPageNo		last_page;
	CBPageNo		min_idxvac_page;

	/*
	 * Members for maintaning cache for duplicate check.
	 *
	 * completed is mark true if there is no more data to be fetched from the
	 * deadtidrun cache, and num_offsets is the number dead offset loaded
	 * into the offsets array.  We don't need to maintain the block number
	 * because in this cache at a time we holds offsets only for a single
	 * block.
	 *
	 * cansetunused keep a flag w.r.t each offset in the offsets array that a
	 * particular offset can be set unused or not.  Basically we know the last
	 * logical conveyor belt pageno for each vacuum run and while merging the
	 * runs we also know that the particular offset is coming from which run.
	 * So that time if the last page of the run is smaller than then minimum
	 * index vacuum page then we mark this true, false otherwise.
	 */
	bool			completed;
	int				num_offsets;
	OffsetNumber   *offsets;
	bool		   *cansetunused;

	/* Dead tid run cache state. */
	DTS_RunState   *deadtidrun;
};

/*
 * DTS_GetRunBlkHdr - Get next block header in the given run.
 *
 * DTS_RunState's runoffset point to the current block header for each run in
 * the runcache.  The data in the conveyor belt are always stored in form of
 * [DTS_BlockHeader][Offset array], and this runoffset will always move over
 * the block header, it will never point to any intermediate data.
 */
#define DTS_GetRunBlkHdr(runstate, run, sizeperrun) \
	(DTS_BlockHeader *)(((char *)((runstate)->cache)) + \
						((run) * (sizeperrun) + (runstate)->runoffset[(run)]))

/* Initial number of vacuum runs in the conveyor belt. */
#define DTS_NUM_VACUUM_RUN		100

/* non-export function prototypes */
static void dts_page_init(Page page);
static int32 dts_offsetcmp(const void *a, const void *b);
static void dts_load_run(DTS_DeadTidState *deadtidstate, int run);
static void dts_merge_runs(DTS_DeadTidState *deadtidstate, BlockNumber blkno);
static void dts_process_runs(DTS_DeadTidState *deadtidstate);
static int dts_read_pagedata(Page page, ItemPointerData *deadtids,
							 int maxtids);
static void AssertCheckPageData(Page page);

/*
 * DTS_InitDeadTidState - intialize the deadtid state
 *
 * This create new deadtid fork for the given relation and initialize the
 * conveyor belt.  The deadtid state will maintain the current state of
 * insertion/read from the conveyor belt.
 */
DTS_DeadTidState *
DTS_InitDeadTidState(Relation rel, CBPageNo min_idxvac_page)
{
	CBPageNo	oldest_pageno;
	CBPageNo	next_pageno;
	SMgrRelation		reln;
	ConveyorBelt	   *cb;
	DTS_DeadTidState   *deadtidstate;

	/* Open the relation at smgr level. */
	reln = RelationGetSmgr(rel);

	/* Allocate memory for deadtidstate. */
	deadtidstate = palloc0(sizeof(DTS_DeadTidState));

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

	/* Initialize the dead tid state. */
	deadtidstate->cb = cb;
	deadtidstate->start_page = CB_INVALID_LOGICAL_PAGE;
	deadtidstate->last_page = CB_INVALID_LOGICAL_PAGE;
	deadtidstate->num_offsets = -1;
	deadtidstate->completed = false;

	ConveyorBeltGetBounds(cb, &oldest_pageno, &next_pageno);

	if (min_idxvac_page != CB_INVALID_LOGICAL_PAGE &&
		min_idxvac_page > oldest_pageno)
		deadtidstate->min_idxvac_page = min_idxvac_page;
	else
		deadtidstate->min_idxvac_page = CB_INVALID_LOGICAL_PAGE;

	return deadtidstate;
}

/*
 * DTS_InsertDeadtids() -- Insert given data into the conveyor belt.
 *
 * 'data' is data to be inserted into the conveyor belt and this data is
 * in multiple of conveyor belt pages. The 'npages' tell us actually how many
 * pages to be inserted and 'datasizes' is bytes to be inserted into each
 * page.  We should know exactly how many valid bytes are copied into each
 * conveyor belt page so that we can set proper value for the pd_lower.  We
 * to do that because in dts_load_run we copy data from multiple pages into
 * cache and we don't want there to be any gap between the data and for that
 * we need to know the exact size of valid data.
 */
void
DTS_InsertDeadtids(DTS_DeadTidState *deadtidstate, char *data, int *datasizes,
				   int npages)
{
	ConveyorBelt   *cb = deadtidstate->cb;
	CBPageNo		pageno;
	int				nwritten = 0;

	/* Loop until we write all the pages in to the conveyor belt. */
	while(nwritten < npages)
	{
		Buffer	buffer;
		Page	page;
		PageHeader	phdr;
		char   *pagedata;
		int		copysz = 0;

		/* Get a new page from the conveyor belt. */
		buffer = ConveyorBeltGetNewPage(cb, &pageno);
		page = BufferGetPage(buffer);
		dts_page_init(page);

		phdr = (PageHeader) page;
		pagedata = (char *) page + phdr->pd_lower;

		START_CRIT_SECTION();

		copysz = datasizes[nwritten];
		if (copysz > DTS_PageMaxDataSpace)
			copysz = DTS_PageMaxDataSpace;

		/* Write data into the current page and update pd_lower. */
		memcpy(pagedata, data, copysz);
		phdr->pd_lower += copysz;

		Assert(phdr->pd_lower <= phdr->pd_upper);

		/*
		 * Add new page to the conveyor belt this will be WAL logged
		 * internally.
		 */
		ConveyorBeltPerformInsert(cb, buffer);

		END_CRIT_SECTION();

		AssertCheckPageData(page);

		/*
		 * Update the data pointer and the remaining size based on how much
		 * data we have copied to this page.
		 */
		data += DTS_PageMaxDataSpace;
		nwritten++;
		ConveyorBeltCleanupInsert(cb, buffer);

		/* Set the start page for this run if not yet set. */
		if (deadtidstate->start_page == CB_INVALID_LOGICAL_PAGE)
			deadtidstate->start_page = pageno;
	}

	deadtidstate->last_page = pageno;
}

/*
 * DTS_ReadDeadtids - Read dead tids from the conveyor belt
 *
 * 'from_pageno' is the conveyor belt page number from where we want to start
 * and the 'to_pageno' upto which we want to read.
 * 'deadtids' is pre allocated array for holding the dead tids, the allocated
 * size is sufficient to hold 'maxtids'.
 *
 * On successful read last page number we have read from conveyor belt will be
 * stored into the '*last_pageread' and the first page of the next run will
 * be stored into the '*next_runpage'
 *
 * Returns number of dead tids actually read from the conveyor belt.  Should
 * always be <= maxtids.
 */
int
DTS_ReadDeadtids(DTS_DeadTidState *deadtidstate, CBPageNo from_pageno,
				 CBPageNo to_pageno, int maxtids, ItemPointerData *deadtids,
				 CBPageNo *last_pageread, CBPageNo *next_runpage)
{
	ConveyorBelt   *cb = deadtidstate->cb;
	Page			page;
	Buffer			buffer;
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;
	int				count = 0;
	int				ndeadtids = 0;
	int				prevpageno = from_pageno;

	/* Read the conveyor belt bounds and check page validity. */
	ConveyorBeltGetBounds(cb, &oldest_pageno, &next_pageno);
	if (from_pageno >= next_pageno)
		return 0;

	if (*next_runpage == CB_INVALID_LOGICAL_PAGE)
		*next_runpage = from_pageno;

	Assert(from_pageno >= oldest_pageno &&
		   (to_pageno == CB_INVALID_LOGICAL_PAGE || to_pageno <= next_pageno));

	/*
	 * If caller has passed a valid to_pageno then don't try to read beyond
	 * that.
	 */
	if (to_pageno != CB_INVALID_LOGICAL_PAGE && to_pageno < next_pageno)
		next_pageno = to_pageno;

	/* Loop until we read maxtids. */
	while(maxtids > 0 && from_pageno < next_pageno)
	{
		DTS_PageData *deadtidpd;

		/* Read page from the conveyor belt. */
		buffer = ConveyorBeltReadBuffer(cb, from_pageno, BUFFER_LOCK_SHARE,
										NULL);
		if (BufferIsInvalid(buffer))
			break;

		page = BufferGetPage(buffer);

		/*
		 * While reading the data we want to ensure that we never read the data
		 * for the ongoing vacuum pass.  So at every run header page check
		 * whether the next runpage is set or not, if it is not set the we are
		 * done.
		 */
		if (from_pageno == *next_runpage)
		{
			deadtidpd = (DTS_PageData *) PageGetSpecialPointer(page);
			*next_runpage = deadtidpd->nextrunpage;
			if (*next_runpage == CB_INVALID_LOGICAL_PAGE)
			{
				UnlockReleaseBuffer(buffer);
				break;
			}
		}

		/*
		 * Don't stop reading in middle of the page, so if remaining tids to
		 * read are less than the tid in this page then stop here.
		 */
		if (maxtids < DTS_PageMaxDataSpace / sizeof(OffsetNumber))
		{
			UnlockReleaseBuffer(buffer);
			break;
		}

		/* Read page and convert to the deadtids. */
		ndeadtids = dts_read_pagedata(page, deadtids, maxtids);

		deadtids += ndeadtids;
		maxtids -= ndeadtids;
		count += ndeadtids;

		prevpageno = from_pageno;

		/* Go to the next page. */
		from_pageno++;
		UnlockReleaseBuffer(buffer);
	}

	*last_pageread = prevpageno;

	return count;
}

/*
 * DTS_LoadDeadtids - Load dead offset from conveyor belt for given block.
 *
 * 'blkno' is current heap block we are going to scan, so this function need to
 * ensure that we have all the previous dead offsets loaded into the cache for
 * for this block from the conveyor belt.
 *
 * This is to ensure that when we are adding dead tids to the conveyor belt
 * we don't add the duplicate tids.
 */
void
DTS_LoadDeadtids(DTS_DeadTidState *deadtidstate, BlockNumber blkno)
{
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;

	Assert(deadtidstate != NULL);

	/*
	 * If there are no more pending data to be pulled from any of the vacuum
	 * run from the conveyor belt then just return.
	 */
	if (deadtidstate->completed)
		return;

	/* Get conveyor belt page range. */
	ConveyorBeltGetBounds(deadtidstate->cb, &oldest_pageno, &next_pageno);

	/*
	 * If the oldest_page is same as next page then there is no data in the
	 * conveyor belt, simmilarlly if oldest page is same as the start page of
	 * this run then there is no previous runs in the conveyor belt so we don't
	 * need to worry about checking the duplicates.
	 */
	if ((oldest_pageno == next_pageno) ||
		(oldest_pageno == deadtidstate->start_page))
	{
		deadtidstate->completed = true;
		deadtidstate->num_offsets = 0;

		return;
	}

	/* If deadtid cache is not yet intialize then do it now. */
	if (deadtidstate->num_offsets == -1)
	{
		Size	cachesize;

		/* Allocate space for dead offset cache. */
		deadtidstate->offsets =
		(OffsetNumber *) palloc0(MaxHeapTuplesPerPage * sizeof(OffsetNumber));
		deadtidstate->cansetunused =
						(bool *) palloc0(MaxHeapTuplesPerPage * sizeof(bool));

		/*
		 * Compute the number of vacuum runs available in the conveyor belt and
		 * also get the start conveyor belt pageno for each run.
		 */
		deadtidstate->deadtidrun = palloc0(sizeof(DTS_RunState));
		cachesize = Min(maintenance_work_mem * 1024, MaxAllocSize);
		cachesize = Min(cachesize, (next_pageno - oldest_pageno) * BLCKSZ);
		deadtidstate->deadtidrun->cachesize = cachesize;
		dts_process_runs(deadtidstate);
	}

	/*
	 * Read data from different vacuum run cache and merge into a sorted offset
	 * array for given 'blkno'.
	 */
	dts_merge_runs(deadtidstate, blkno);
}

/*
 * DTS_DeadtidExists - Check whether the tid already exists.
 *
 * Returns true if the offset for the given block is already present in the
 * conveyor belt.  Also set the '*setunused' flag to true if index vacuum
 * is also done for this, that mean the caller can directly mark this item
 * as unused.
 */
bool
DTS_DeadtidExists(DTS_DeadTidState *deadtidstate, BlockNumber blkno,
				  OffsetNumber offset, bool *setunused)
{
	OffsetNumber	*res;

	if (deadtidstate == NULL)
		return false;

	/* No data to be compared for this block then just return. */
	if (deadtidstate->num_offsets == 0)
		return false;

	/* Search the offset in the cache. */
	res = (OffsetNumber *) bsearch((void *) &offset,
									(void *) deadtidstate->offsets,
									deadtidstate->num_offsets,
									sizeof(OffsetNumber),
									dts_offsetcmp);
	if (res == NULL)
		return false;

	/* Get the flag whether this can be marked unused. */
	*setunused = deadtidstate->cansetunused[res - deadtidstate->offsets];

	return true;
}

/*
 * DTS_ReleaseDeadTidState - Release the dead tid state.
 *
 * Release memory for the deadtid state and its members.  This also mark the
 * current vacuum run is complete by storing the last page of the run into the
 * special space of the first page of the run.
 */
void
DTS_ReleaseDeadTidState(DTS_DeadTidState *deadtidstate)
{
	DTS_PageData *deadtidpd;
	Buffer	buffer;
	Page	page;

	/* There is no data in this run so nothing to be done. */
	if (deadtidstate->start_page == CB_INVALID_LOGICAL_PAGE)
		return;

	/* Read page from the conveyor belt. */
	buffer = ConveyorBeltReadBuffer(deadtidstate->cb, deadtidstate->start_page,
									BUFFER_LOCK_EXCLUSIVE, NULL);
	if (BufferIsInvalid(buffer))
		elog(ERROR, "invalid conveyor belt page" UINT64_FORMAT,
			 deadtidstate->start_page);

	page = BufferGetPage(buffer);
	deadtidpd = (DTS_PageData *) PageGetSpecialPointer(page);
	deadtidpd->nextrunpage = deadtidstate->last_page + 1;

	/*
	 * If min_idxvac_page is valid then we must have marked the offset unused
	 * upto this page along with pruning so we can vacuum the conveyor belt
	 * as well.
	 */
	if (deadtidstate->min_idxvac_page != CB_INVALID_LOGICAL_PAGE)
		DTS_Vacuum(deadtidstate, deadtidstate->min_idxvac_page);

	/* TODO - WAL log this operation*/
	elog(LOG, "run complete setting next run page = "UINT64_FORMAT,
		 deadtidpd->nextrunpage);

	UnlockReleaseBuffer(buffer);

	/* Cleanup and release the deadtid state. */
	if (deadtidstate->deadtidrun != NULL)
	{
		DTS_RunState   *deadtidrun = deadtidstate->deadtidrun;

		if (deadtidrun->cache != NULL)
			pfree(deadtidrun->cache);
		if (deadtidrun->startpage != NULL)
			pfree(deadtidrun->startpage);
		if (deadtidrun->nextpage != NULL)
			pfree(deadtidrun->nextpage);
		if (deadtidrun->runoffset != NULL)
			pfree(deadtidrun->runoffset);

		pfree(deadtidrun);
	}
	if (deadtidstate->offsets)
		pfree(deadtidstate->offsets);
	if (deadtidstate->cansetunused)
		pfree(deadtidstate->cansetunused);

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
	PageHeader	p = (PageHeader) page;
	DTS_PageData	   *deadtidpd;

	PageInit(page, BLCKSZ, sizeof(DTS_PageData));
	deadtidpd = (DTS_PageData *) PageGetSpecialPointer(page);
	deadtidpd->nextrunpage = CB_INVALID_LOGICAL_PAGE;

	/* Start writing data from the maxaligned offset. */
	p->pd_lower = MAXALIGN(SizeOfPageHeaderData);
}

/*
 * dts_read_pagedata - Read page data and convert to deadtids
 *
 * In conveyor belt data are copied in form of [DST_BlockHeader] and
 * [offset array], so now we process the page and convert it to tid format.
 */
static int
dts_read_pagedata(Page page, ItemPointerData *deadtids, int maxtids)
{
	char	   *pagedata;
	int			ntids = 0;
	int			nbyteread = 0;
	DTS_BlockHeader *blkhdr = NULL;

	pagedata = PageGetContents(page);

	/* Loop until we convert complete page. */
	while(1)
	{
		ItemPointerData tmp;
		OffsetNumber   *offsets;
		BlockNumber		blkno;
		int		i;

		/*
		 * While copying the data into the page we had ensured that we will
		 * copy the block data to the page iff we can copy block header and
		 * at least one offset, so based on the current nbyteread if the
		 * remaining space of the page is not enough for that data then we can
		 * stop.
		 */
		if (DTS_PageMaxDataSpace - nbyteread < DTS_MinCopySize)
			break;

		/*
		 * Read next block header, if the block header is not initialized i.e
		 * noffsets is 0 then we are done with this page.
		 */
		blkhdr = (DTS_BlockHeader *) (pagedata + nbyteread);
		blkno = BlockIdGetBlockNumber(&blkhdr->blkid);
		if (blkhdr->noffsets == 0)
			break;

		/*
		 * Skip the block header so that we reach to the offset array for this
		 * block number.
		 */
		nbyteread += sizeof(DTS_BlockHeader);
		offsets = (OffsetNumber *) (pagedata + nbyteread);

		/*
		 * Use the same block number as in the block header and generate tid
		 * w.r.t. each offset in the offset array.
		 */
		ItemPointerSetBlockNumber(&tmp, blkno);
		for (i = 0; i < blkhdr->noffsets; i++)
		{
			ItemPointerSetOffsetNumber(&tmp, offsets[i]);
			deadtids[ntids++] = tmp;
		}

		/*
		 * Skip all the offset w.r.t. the current block so that we point to the
		 * next block header in the page.
		 */
		nbyteread += (blkhdr->noffsets * sizeof(OffsetNumber));

		/*
		 * The caller has ensured that all the tids in this page should fit
		 * into the buffer so it should never exceed that.
		 */
		Assert(ntids <= maxtids);
	}

	return ntids;
}

/*
 * dts_offsetcmp() - Compare two offset numbers, return -1, 0, or +1.
 */
static int32
dts_offsetcmp(const void *a, const void *b)
{
	OffsetNumber offset1 = *((const OffsetNumber *) a);
	OffsetNumber offset2 = *((const OffsetNumber *) b);

	if (offset1 < offset2)
		return -1;
	else if (offset1 > offset2)
		return 1;
	else
		return 0;
}

/*
 * dts_load_run - load data from conveyor belt vacuum run into the run cache.
 *
 * Pull more data from the conveyor belt into the cache.  This will pull the
 * data only for the input vacuum 'run'.
 */
static void
dts_load_run(DTS_DeadTidState *deadtidstate, int run)
{
	DTS_RunState   *runstate = deadtidstate->deadtidrun;
	ConveyorBelt   *cb = deadtidstate->cb;
	CBPageNo		from_pageno;
	CBPageNo		endpage;
	CBPageNo		prevpage;
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;
	char		   *cache;
	int				runsize = runstate->cachesize / runstate->num_runs;
	int				nremaining = runsize;

	/*
	 * If the next conveyor belt page to be processed for this run is set to
	 * CB_INVALID_LOGICAL_PAGE that means we have already pulled all the data
	 * from this run.  So set the runoffset for this run to -1 so that next
	 * time the merge will skip this run.
	 */
	prevpage = from_pageno = runstate->nextpage[run];
	if (from_pageno == CB_INVALID_LOGICAL_PAGE)
	{
		runstate->runoffset[run] = -1;
		return;
	}

	/* Refilling this run's cache with new data so reset the runoffset. */
	runstate->runoffset[run] = 0;

	ConveyorBeltGetBounds(deadtidstate->cb, &oldest_pageno, &next_pageno);

	/*
	 * Set the endpage so that while reading the data for this run we do not
	 * overflow to the next run.
	 */
	if (run < runstate->num_runs - 1)
		endpage = runstate->startpage[run + 1];
	else
		endpage = next_pageno;

	/* make cache to point to the current run's space. */
	cache = runstate->cache + runsize * run;
	memset(cache, 0, runsize);

	/* We must read at least one conveyor belt page. */
	Assert(nremaining >= DTS_PageMaxDataSpace);

	while(nremaining > 0 && from_pageno < endpage)
	{
		Buffer		buffer;
		PageHeader	phdr;
		Page		page;
		int			copysz;

		/* Read page from the conveyor belt. */
		buffer = ConveyorBeltReadBuffer(cb, from_pageno, BUFFER_LOCK_SHARE,
										NULL);
		if (BufferIsInvalid(buffer))
			break;

		page = BufferGetPage(buffer);
		phdr = (PageHeader) page;

		/*
		 * There might be some empty space at the end of the page so while
		 * copying skip that size, that size will be equal to difference
		 * between pd_upper and pd_lower.
		 */
		copysz = DTS_PageMaxDataSpace - (phdr->pd_upper - phdr->pd_lower);
		memcpy(cache, PageGetContents(page), copysz);
		nremaining -= copysz;
		cache += copysz;

		prevpage = from_pageno;

		/* Go to the next page. */
		from_pageno++;
		UnlockReleaseBuffer(buffer);

		/*
		 * Don't stop reading in middle of the page, so if the remaining read
		 * size is less than the size of the page then stop, we will read this
		 * page in the next round.
		 */
		if (nremaining < DTS_PageMaxDataSpace)
			break;
	}

	if (prevpage >= endpage)
		runstate->nextpage[run] = CB_INVALID_LOGICAL_PAGE;
	else if (from_pageno == prevpage)
		runstate->nextpage[run] = prevpage;
	else
		runstate->nextpage[run] = prevpage + 1;
}

/*
 * dts_merge_runs - process and merge vacuum runs from run cache.
 *
 * Pass through each vacuum run in the conveyor belt and load a few blocks from
 * starting of each vacuum runs based on the cache size.
 *
 * On return this will have all the offset for given 'blkno' stored into the
 * 'deadtidstate->offsets' array in sorted order.  And this will also mark
 * 'deadtidstate->cansetunused' flag w.r.t each offset indicating whether the
 * index vacuum is already done for this or not.
 */
static void
dts_merge_runs(DTS_DeadTidState *deadtidstate, BlockNumber targetblk)
{
	DTS_RunState	   *runstate = deadtidstate->deadtidrun;
	DTS_BlockHeader	   *blkhdr;
	OffsetNumber		smallestoffset = InvalidOffsetNumber;
	CBPageNo			min_idxvac_page = deadtidstate->min_idxvac_page;
	int		run;
	int		smallestrun;
	int		runsize;
	int		noffsets = 0;
	int	   *runindex;
	int	   *runoffset = runstate->runoffset;
	bool	nopendingdata = true;

	/*
	 * If we can not pull at least one block from each run then we can not
	 * perform the in memory merge so we will have to perform disk based
	 * merging.  Which is not yet implemented.
	 *
	 * XXX maybe instead of pulling whole conveyor belt page we can consider
	 * that at least if we can pull all the offset w.r.t. one block header then
	 * also we can consider doing the in-memory sort merge.
	 */
	if (runstate->num_runs * DTS_PageMaxDataSpace > runstate->cachesize)
		elog(ERROR, "disk based merging is not yet implemeted");

	/* Compute the max tids can be fit into each run cache. */
	runsize = runstate->cachesize / runstate->num_runs;

	/*
	 * Maintain a local index for each run, this tracks which offset currently
	 * we are merging for each run within the current block header offset
	 * array, as soon as we go to the next block header we will reset it.
	 */
	runindex = palloc0(sizeof(int) * runstate->num_runs);

	/*
	 * Loop untill we fetch all the deadoffset from the conveyor belt for the
	 * input blkno.
	 */
	while (1)
	{
		for (run = 0; run < runstate->num_runs; run++)
		{
			DTS_BlockHeader *blkhdr;
			OffsetNumber   *nextoffset;
			BlockNumber		blkno;

			if (runoffset[run] == -1)
				continue;

			nopendingdata = false;
			blkhdr = DTS_GetRunBlkHdr(runstate, run, runsize);
			blkno = BlockIdGetBlockNumber(&blkhdr->blkid);

			/*
			 * Make sure that before we start merging offsets from this run,
			 * the run is pointing to the right block header.  If block no is
			 * of the block header already higher than the targetblk that
			 * means this run might not have more data for this block so don't
			 * process this run further.  If block header is for smaller block
			 * than we are looking for one or it is uninitialized then move
			 * until we reach to the right block header.
			 */
			while (blkno < targetblk || (blkno == 0 && blkhdr->noffsets == 0))
			{
				/*
				 * There is no more data into the current run so nothing to do
				 * for the current run.
				 */
				if (runoffset[run] == -1)
					break;

				/*
				 * If the current block header shows 0 offsets that means we
				 * have reached to the end of the cached data for this run so
				 * pull more data for this run from the conveyor belt.
				 * Otherwise, Move runoffset to point to the next block header.
				 */
				if (blkhdr->noffsets == 0)
				{
					dts_load_run(deadtidstate, run);
					runindex[run] = 0;
				}
				else
					runoffset[smallestrun] +=
										DTS_BlkDataSize(blkhdr->noffsets);

				/* Fetch the current block header from the run. */
				blkhdr = DTS_GetRunBlkHdr(runstate, run, runsize);
				blkno = BlockIdGetBlockNumber(&blkhdr->blkid);
			}

			/* We have found a matching block header. */
			if (blkno == targetblk)
			{
				/* Block header can not be with 0 offsets. */
				Assert(blkhdr->noffsets != 0);

				/* Skip block header to point to the first offset. */
				nextoffset = (OffsetNumber *)
								((char *) blkhdr + sizeof(DTS_BlockHeader));
				nextoffset += runindex[run];

				/*
				 * Set the smallest offset and remember the run from which we
				 * got the smallest offset.
				 */
				if (!(OffsetNumberIsValid(smallestoffset)) ||
					*nextoffset < smallestoffset)
				{
					smallestoffset = *nextoffset;
					smallestrun = run;
				}
			}
		}

		/*
		 * If we could not fetch next offset that means we don't have any more
		 * offset for the current block.  So we are done for now.
		 */
		if (!OffsetNumberIsValid(smallestoffset))
			break;

		/* Fetch block header from the current position of the run. */
		blkhdr = DTS_GetRunBlkHdr(runstate, smallestrun, runsize);

		/*
		 * Go to the next offset of the run from which we have found the
		 * smallest offset.
		 */
		runindex[smallestrun]++;

		/*
		 * If we have processed all the offset for this block header then move
		 * to the next block header, because due to the page split it is
		 * possible that we might get the block header for the same block
		 * again.
		 */
		if (runindex[smallestrun] >= blkhdr->noffsets)
		{
			runoffset[smallestrun] += DTS_BlkDataSize(blkhdr->noffsets);
			runindex[smallestrun] = 0;
		}

		/*
		 * Store the offset and also set a flag that whether we have already
		 * done index vacuum for this offset or not.
		 */
		deadtidstate->offsets[noffsets] = smallestoffset;
		if (min_idxvac_page != CB_INVALID_LOGICAL_PAGE &&
			runstate->startpage[smallestrun + 1] <= min_idxvac_page)
			deadtidstate->cansetunused[noffsets] = true;
		else
			deadtidstate->cansetunused[noffsets] = false;

		smallestoffset = InvalidOffsetNumber;
		noffsets++;

		/*
		 * In deadtidstate->offsets cache we are collecting the offsets only
		 * for the one block so it can never be > MaxHeapTuplesPerPage.
		 */
		Assert(noffsets <= MaxHeapTuplesPerPage);
	}

	/*
	 * Store number of offset into the the deadtidstate and also mark it
	 * completed if we have processed all the data from the conveyor belt.
	 */
	deadtidstate->num_offsets = noffsets;
	deadtidstate->completed = nopendingdata;

	/* Cleanup. */
	pfree(runindex);
}

/*
 * dts_process_runs - Compute number of vacuums runs in conveyor belt.
 *
 * Pass over the vacuum run in the conveyor belt and remember the start page
 * for each vacuum run.  Later dts_load_run() will maintain cache over each
 * run and pull the data from each run as and when needed.
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
	int				maxrun;
	int				nRunCount = 0;
	int				nelement;

	/* Get conveyor belt page bounds. */
	ConveyorBeltGetBounds(cb, &oldest_pageno, &next_pageno);
	curpageno = oldest_pageno;

	/*
	 * Allocate next_run_page, this will hold the next page to load from each
	 * run.  Initially start with DTS_NUM_VACUUM_RUN and while processing the
	 * runs, if the number of runs are more that this value then expand the
	 * memory.
	 */
	maxrun = DTS_NUM_VACUUM_RUN;
	next_run_page = palloc(maxrun * sizeof(CBPageNo));
	deadtidrun->cache = palloc0(deadtidrun->cachesize);

	/*
	 * Read page from the conveyor belt.
	 *
	 * TODO: this conveyor belt reading logic is duplicated in multiple
	 * functions with some minor differences so this could be unified.
	 */
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
			if (nRunCount == maxrun - 1)
			{
				maxrun *= 2;
				next_run_page = repalloc(next_run_page,
										 maxrun * sizeof(CBPageNo));
			}

			/* Remember the start page of the current run. */
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
	deadtidrun->num_runs = nRunCount;
	deadtidrun->startpage = palloc(nelement * sizeof(CBPageNo));
	deadtidrun->nextpage = palloc(nelement * sizeof(CBPageNo));
	deadtidrun->runoffset = palloc0(nelement * sizeof(int));

	/*
	 * Set startpage and next page for each run.  Initially both will be same
	 * but as we pull data from the conveyor belt the nextpage for each run
	 * will move to the next page to be fetched from the conveyor belt whereas
	 * the startpage will remain the same throughout complete vacuum pass.
	 */
	memcpy(deadtidrun->startpage, next_run_page, nelement * sizeof(CBPageNo));
	memcpy(deadtidrun->nextpage, next_run_page, nelement * sizeof(CBPageNo));
	pfree(next_run_page);
}

/*
 * AssertCheckPageData - Verify whether the page data format is valid or not.
 *
 * Check whether all page data are written proper format i.e.
 * [Block Header][Offset Array] and all boundries are sane.
 *
 * No-op if assertions are not in use.
 */
static void
AssertCheckPageData(Page page)
{
#ifdef USE_ASSERT_CHECKING
	PageHeader	phdr;
	char   *pagedata;
	int		nbyteread = 0;
	DTS_BlockHeader *blkhdr;

	phdr = (PageHeader) page;
	pagedata = (char *) PageGetContents(page);;

	/* Loop until we convert complete page. */
	while(1)
	{
		OffsetNumber   *offsets;
		BlockNumber		blkno;
		int		i;

		/*
		 * While copying the data into the page we have ensure that we will
		 * copy the block data to the page iff we can copy block header and
		 * at least one offset, so based on the current nbyteread if writable
		 * space of the page is not enough for that data then we can stop.
		 */
		if (DTS_PageMaxDataSpace - nbyteread < DTS_MinCopySize)
			break;

		/*
		 * Read next block header, if the block header is not initialized i.e
		 * noffsets is 0 then we are done with this page.
		 */
		blkhdr = (DTS_BlockHeader *) (pagedata + nbyteread);
		if (blkhdr->noffsets == 0)
			break;

		blkno = BlockIdGetBlockNumber(&blkhdr->blkid);

		/*
		 * Skip the block header so that we reach to the offset array for this
		 * block number.
		 */
		nbyteread += sizeof(DTS_BlockHeader);
		offsets = (OffsetNumber *) (pagedata + nbyteread);

		/*
		 * Use the same block number as in the block header and generate tid
		 * w.r.t. each offset in the offset array.
		 */
		Assert(BlockNumberIsValid(blkno));
		for (i = 0; i < blkhdr->noffsets; i++)
			Assert(OffsetNumberIsValid(offsets[i]));

		/*
		 * Skip all the offset w.r.t. the current block so that we point to the
		 * next block header in the page.
		 */
		nbyteread += (blkhdr->noffsets * sizeof(OffsetNumber));
		Assert(nbyteread <= phdr->pd_lower);
	}
#endif
}
