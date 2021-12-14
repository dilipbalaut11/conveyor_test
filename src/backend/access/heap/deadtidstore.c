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

#include <math.h>

#include "access/conveyor.h"
#include "access/deadtidstore.h"
#include "access/htup_details.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "utils/memutils.h"


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

	Size	runsize;
	/*
	 * During merge of each run, this maintains the current deadtid index
	 * in to the cache for each runs.
	 */
	int	   *runindex;
	int	   *runoffset;

	/* Starting logical conveyor belt pageno for each run. */
	CBPageNo		   *startpage;

	/* Next logical conveyor belt pageno to load for each run. */
	CBPageNo		   *nextpage;

	OffsetNumber	   *offsets;
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
	int				num_offsets;		/* number of of dead offsets. */
	ItemPointerData	   *itemptrs;		/* dead tids for the current scan block */
	BlockNumber			blkno;			/* current block number of the cached
										   offsets */
	OffsetNumber	*offsets;
	DTS_RunState	*deadtidrun;		/* dead tid run cache state. */
};

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

#define DTS_NextRunBlockHeader(runstate, run, sizeperrun) \
	(((char *)((runstate)->offsets)) + ((run) * (sizeperrun) + (runstate)->runoffset[(run)]))

/* 
 * We will copy the block data to the page iff the remaining space can fit
 * the block header and at least one offset.
 */
#define DTS_MinCopySize	sizeof(DTS_BlockHeader) + sizeof(OffsetNumber)

/* non-export function prototypes */
static void dts_page_init(Page page);
static int32 dts_offsetcmp(const void *a, const void *b);
static void dts_load_run(DTS_DeadTidState *deadtidstate, int run);
static void dts_merge_runs(DTS_DeadTidState *deadtidstate, BlockNumber blkno);
static void dts_process_runs(DTS_DeadTidState *deadtidstate);
static int dts_copy_pagedata(Page page, DTS_BlockHeader *prevblkhdr,
							 char *data, int datasize);
static int dts_read_pagedata(Page page, ItemPointerData *deadtids,
							 int maxtids);							 

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
	deadtidstate->num_offsets = -1;
	deadtidstate->blkno = InvalidBlockNumber;


	return deadtidstate;
}

/*
 *	DTS_InsertDeadtids() -- Insert given data into the conveyor belt.
 */
void
DTS_InsertDeadtids(DTS_DeadTidState *deadtidstate, char *data, int datasize)
{
	ConveyorBelt   *cb = deadtidstate->cb;
	CBPageNo		pageno = deadtidstate->last_page;
	DTS_BlockHeader	blkhdr;

	blkhdr.blkno = InvalidBlockNumber;

	/* Loop until we flush out all the dead tids in the conveyor belt. */
	while(datasize > 0)
	{
		Buffer	buffer;
		Page	page;
		int		size = 0;
		bool	newpage = false;

		/*
		 * If we are starting a new run then start from a fresh conveyor belt
		 * page, otherwise, continue writing into the previous page.  Even for
		 * the new run we can start with the last used page but for simplicity
		 * we might waste some space in page at the end of one complete vacuum
		 * pass.  This way it would be easy to perform the index pass because
		 * if we are not starting new run in the middle of the page we trace
		 * the last vacuum point just by the conveyor belt pageno.
		 */
		if (deadtidstate->last_page != CB_INVALID_LOGICAL_PAGE)
		{
			pageno = deadtidstate->last_page;
			buffer = ConveyorBeltReadBuffer(cb, pageno, BUFFER_LOCK_EXCLUSIVE,
											NULL);
			page = BufferGetPage(buffer);
		}
		else
		{
			newpage = true;
			buffer = ConveyorBeltGetNewPage(cb, &pageno);
			page = BufferGetPage(buffer);
			dts_page_init(page);
		}

		START_CRIT_SECTION();

		/* 
		 * Copy data into the current page, and update the data pointer and the
		 * remaining size based on how much we have copied.
		 */
		size = dts_copy_pagedata(page, &blkhdr, data, datasize);
		data += size;
		datasize -= size;

		/* 
		 * If we have added new page then add it to the conveyor belt this will
		 * be WAL logged internally, otherwise if we have reused the previosuly
		 * written page then WAL log it.
		 */
		if (newpage)
			ConveyorBeltPerformInsert(cb, buffer);
		else
		{
			/* TODO: WAL log this operation. */
		}

		END_CRIT_SECTION();

		if (newpage)
			ConveyorBeltCleanupInsert(cb, buffer);
		else
			UnlockReleaseBuffer(buffer);

		/* Set the start page for this run if not yet set. */
		if (deadtidstate->start_page == CB_INVALID_LOGICAL_PAGE)
			deadtidstate->start_page = pageno;

		deadtidstate->last_page = CB_INVALID_LOGICAL_PAGE;
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
	CBPageNo		next_runpage = from_pageno;
	int				count = 0;
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
		if (from_pageno == next_runpage)
		{
			deadtidpd = (DTS_PageData *) PageGetSpecialPointer(page);
			next_runpage = deadtidpd->nextrunpage;
			if (next_runpage == CB_INVALID_LOGICAL_PAGE)
			{
				UnlockReleaseBuffer(buffer);
				break;
			}
		}

		/*
		 * Don't stop reading in middle of the page, so if remaning tids to
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
 * DTS_LoadDeadtids - load deadtid from conveyor belt into the cache.
 *
 * blkno - current heap block we are going to scan, so this function need to
 * ensure that we have all the dead tids loaded in the cache at least for
 * this heap block.
 */
void
DTS_LoadDeadtids(DTS_DeadTidState *deadtidstate, BlockNumber blkno)
{
	CBPageNo		oldest_pageno;
	CBPageNo		next_pageno;

	Assert(deadtidstate != NULL);

	if (deadtidstate->num_offsets == 0)
		return;

	/* fixme */
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
	if (deadtidstate->num_offsets == -1)
	{
		Size	runsize = Min(maintenance_work_mem * 1024, MaxAllocSize);

		/* Allocate space for dead offset cache. */
		deadtidstate->offsets = (OffsetNumber *) palloc0(runsize);

		/*
		 * Compute the number of runs available in the conveyor belt and check
		 * whether we have enough cache to load at least one block from each
		 * run, if so then we can peroform in-memory merge of the runs.
		 */
		deadtidstate->deadtidrun = palloc0(sizeof(DTS_RunState));
		deadtidstate->deadtidrun->runsize = runsize;
		dts_process_runs(deadtidstate);
	}

	/*
	 * Check the current block number in the dead offset cache and the block is
	 * smaller than the block we are processing now then load the data for
	 * the next block.
	 */
	if (deadtidstate->num_tids > 0 && deadtidstate->blkno >= blkno)
		return;
	else
		dts_merge_runs(deadtidstate, blkno);
}

/*
 * DTS_DeadtidExists - check whether the offset already exists in conveyor
 * 					   belt.
 */
bool
DTS_DeadtidExists(DTS_DeadTidState *deadtidstate, BlockNumber blkno,
				  OffsetNumber offset)
{
	if (deadtidstate == NULL)
		return false;

	/* No data to be compared for this block then just return. */
	if (deadtidstate->blkno != blkno)
		return false;

	return bsearch((void *) &offset, (void *) deadtidstate->offsets,
				   deadtidstate->num_offsets, sizeof(OffsetNumber),
				   dts_offsetcmp);
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
	PageHeader	p = (PageHeader) page;
	DTS_PageData	   *deadtidpd;

	PageInit(page, BLCKSZ, sizeof(DTS_PageData));
	deadtidpd = (DTS_PageData *) PageGetSpecialPointer(page);
	deadtidpd->nextrunpage = CB_INVALID_LOGICAL_PAGE;

	/* Start writing data from the maxaligned offset. */
	p->pd_lower = MAXALIGN(SizeOfPageHeaderData);
}

/*
 * dts_copy_blockdata - copy data into given conveyor belt page.
 * buffer has data in below form.
 *
 * [block1, noffsets][offset1][offset2]..[offsetn][block2, noffset]..[offsetn]
 *
 * So the rules for storing data in this page is,
 * 1) If we can copy all the remaining data in this block then directly copy.
 * 2) Otherwise, copy them block by block so that we do page split at the
 *    correct point.
 * 3) So for page split if we can not copy all the offset of a particular block
 * in this page then if we can fit at least the block header + one offset in
 * this page then only we will split that block otherwise we will copy that
 * whole block data to the next page.  If we split we will update the noffsets
 * in the current block header and we will also return the prevblokhdr so that
 * when we will insert the remaining offsets of the splitted block that time
 * we insert the block header again.
 */
static int
dts_copy_pagedata(Page	page, DTS_BlockHeader *prevblkhdr, char *data,
				  int datasize)
{
	PageHeader	phdr;
	char   *pagedata;
	int		freespace;
	int		copysz = 0;
	bool	prevheader = false;
	bool	pagesplit = false;
	DTS_BlockHeader *blkhdr = prevblkhdr;

	phdr = (PageHeader) page;
	pagedata = (char *) page + phdr->pd_lower;
	freespace = PageGetExactFreeSpace(page);

	/*
	 * If previous block header is valid that mean there was a page split so
	 * we need to add the block header again into the new page.
	 */
	if (BlockNumberIsValid(prevblkhdr->blkno))
	{
		Assert(freespace > DTS_MinCopySize);
		memcpy(pagedata, prevblkhdr, sizeof(DTS_BlockHeader));
		pagedata += sizeof(DTS_BlockHeader);
		phdr->pd_lower += sizeof(DTS_BlockHeader);
		prevheader = true;
	}
	/*
	 * If we don't have enough space in the page to copy DTS_MinCopySize data
	 * no point in copying anything to this page.
	 */
	else if (freespace < DTS_MinCopySize)
		return 0;

	/*
	 * If we have enough freespace in the page then directly copy the data
	 * into the page.
	 */
	if (freespace >= datasize)
	{
		memcpy(pagedata, data, datasize);
		phdr->pd_lower += datasize;

		return datasize;
	}

	while(1)
	{
		int		ncopyoffset = 0;

		/* Stop if we can not copy block header and at least one offset. */
		if (freespace - copysz < DTS_MinCopySize)
			break;

		/* 
		 * After we have identified pagesplit we should not proceed to insert
		 * more data in the same page.
		 */
		Assert(!pagesplit);

		if (!prevheader)
		{
			blkhdr = (DTS_BlockHeader *) (data + copysz);
			copysz += sizeof(DTS_BlockHeader);
		}

		/* Compute how many offset we can copy for this block. */
		ncopyoffset = (freespace - copysz) / sizeof(OffsetNumber);

		/* 
		 * If we can not copy all the offset of the block in the remaining
		 * space then we get the page split and we need to reinsert this block
		 * header again while inserting data into the next conveyor belt page.
		 */
		if (ncopyoffset < blkhdr->noffsets)
		{
			copysz += ncopyoffset * sizeof(OffsetNumber);
			prevblkhdr->blkno = blkhdr->blkno;

			/* 
			 * adjust the noffset in the current block header as well as for
			 * the header we will insert into the new conveyor belt page. 
			 */
			prevblkhdr->noffsets = blkhdr->noffsets - ncopyoffset;
			blkhdr->noffsets = ncopyoffset;
			pagesplit = true;
		}
		else
			copysz += blkhdr->noffsets * sizeof(OffsetNumber);
	}

	/* If there is no pagesplit then reset the prevblkhdr. */
	if (!pagesplit)
		prevblkhdr->blkno = InvalidBlockNumber;

	memcpy(pagedata, data, copysz);
	phdr->pd_lower += copysz;

	Assert(phdr->pd_lower <= phdr->pd_upper);

	return copysz;
}

/*
 * dts_read_pagedata - read page data and convert to deadtids
 *
 * In conveyor belt data are copied in form of [DST_BlockHeader] and
 * [offset array], so now we process the page and convert this to tid format.
 */
static int
dts_read_pagedata(Page page, ItemPointerData *deadtids, int maxtids)
{
	char	   *pagedata;
	int			ntids = 0;
	int			nbyteread = 0;
	PageHeader	phdr;
	DTS_BlockHeader *blkhdr = NULL;

	phdr = (PageHeader) page;
	pagedata = PageGetContents(page);

	/* Loop until we convert complete page. */
	while(1)
	{
		ItemPointerData tmp;
		OffsetNumber   *offsets;
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
		 * both blkno and noffsets are 0 then we are done with this page.
		 */
		blkhdr = (DTS_BlockHeader *) (pagedata + nbyteread);
		if (blkhdr->blkno == 0 && blkhdr->noffsets == 0)
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
		ItemPointerSetBlockNumber(&tmp, blkhdr->blkno);
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
dts_merge_runs(DTS_DeadTidState *deadtidstate, BlockNumber blkno)
{
	return;
#if 0	
	DTS_RunState	   *runstate = deadtidstate->deadtidrun;
	ItemPointerData    *smallesttid = NULL;
	DTS_BlockHeader	   *blkhdr;
	int		run;
	int		smallestrun;
	int		maxrunsize;
	int		noffsets = 0;
	bool   *process_run = palloc0(runstate->num_runs * sizeof(bool));


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
	maxrunsize = runstate->runsize / runstate->num_runs;

	for (run = 0; run < runstate->num_runs; run++)
	{
		DTS_BlockHeader	*blkhdr = (DTS_BlockHeader *)
							DTS_NextRunBlockHeader(runstate, run, maxrunsize);

		/* block number is same so we will process this run. */
		if (blkhdr->blkno == blkno)
			 process_run[run] = true;
		else if(blkhdr->blkno < blkno)
		{
			while(blkhdr->blkno >= blkno)
			{
				runstate->runoffset[run] += (sizeof(DTS_BlockHeader) +
									sizeof(OffsetNumber) * blkhdr->noffset);
				blkhdr = (DTS_BlockHeader *) DTS_NextRunBlockHeader(runstate,
																	run,
																	maxrunsize);
			}
		}
	}

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

	deadtidstate->num_offsets = noffsets;
#endif	
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
	deadtidrun->runoffset = palloc0(nRunCount * sizeof(int));

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
