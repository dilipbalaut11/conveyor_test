/*-------------------------------------------------------------------------
 *
 * conveyor.c
 *	  Conveyor belt storage.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * src/backend/access/conveyor/conveyor.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/cbcache.h"
#include "access/cbfsmpage.h"
#include "access/cbindexpage.h"
#include "access/cbmetapage.h"
#include "access/cbmodify.h"
#include "access/conveyor.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/rel.h"

static CBSegNo ConveyorSearchFSMPages(ConveyorBelt *cb,
									  CBSegNo next_segment,
									  BlockNumber *fsmblock,
									  Buffer *fsmbuffer);
static void ConveyorBeltClearSegment(ConveyorBelt *cb, CBSegNo segno,
									 bool include_first_page);
static bool ConveyorBeltClearIndexSegmentEntries(ConveyorBelt *cb,
												 Buffer metabuffer,
												 CBSegNo index_segment,
												 CBPageNo index_vacuum_stop_point,
												 CBSegNo *next_index_segment);
static CBSegNo ConveyorBeltFreeOldestIndexSegment(ConveyorBelt *cb,
												  Buffer metabuffer,
												  CBSegNo oldest_index_segment,
												  CBPageNo index_vacuum_stop_point);
static Buffer ConveyorBeltExtend(ConveyorBelt *cb, BlockNumber blkno,
								 BlockNumber *possibly_not_on_disk_blkno);
static BlockNumber ConveyorBeltFSMBlockNumber(ConveyorBelt *cb,
											  CBSegNo segno);
static Buffer ConveyorBeltRead(ConveyorBelt *cb, BlockNumber blkno, int mode);
static Buffer ConveyorBeltPageIsUnused(Page page);

/*
 * Handle used to mediate access to a conveyor belt.
 */
struct ConveyorBelt
{
	Relation	cb_rel;
	ForkNumber	cb_fork;
	uint16		cb_pages_per_segment;
	CBCache    *cb_cache;

	/*
	 * These fields are used for communication between ConveyorBeltGetNewPage,
	 * ConveyorBeltPerformInsert, and ConveyorBeltCleanupInsert.
	 */
	RelFileNode *cb_insert_relfilenode;
	Buffer		cb_insert_metabuffer;
	BlockNumber cb_insert_block;
	Buffer		cb_insert_buffer;
};

/*
 * Create a new conveyor belt.
 */
ConveyorBelt *
ConveyorBeltInitialize(Relation rel,
					   ForkNumber fork,
					   uint16 pages_per_segment,
					   MemoryContext mcxt)
{
	ConveyorBelt *cb;
	Buffer		metabuffer;
	bool		needs_xlog;

	/* Write a metapage for the new conveyor belt, and XLOG if needed. */
	needs_xlog = RelationNeedsWAL(rel) || fork == INIT_FORKNUM;
	metabuffer = ReadBufferExtended(rel, fork, P_NEW, RBM_NORMAL, NULL);
	if (BufferGetBlockNumber(metabuffer) != CONVEYOR_METAPAGE)
		elog(ERROR, "can't initialize non-empty fork as conveyor belt");
	LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);
	cb_create_metapage(&RelationGetSmgr(rel)->smgr_rnode.node, fork,
					   metabuffer, pages_per_segment, needs_xlog);
	UnlockReleaseBuffer(metabuffer);

	/*
	 * Initialize a ConveyorBelt object so that the caller can do something
	 * with the new conveyor belt if they wish.
	 */
	cb = MemoryContextAlloc(mcxt, sizeof(ConveyorBelt));
	cb->cb_rel = rel;
	cb->cb_fork = fork;
	cb->cb_pages_per_segment = pages_per_segment;
	cb->cb_cache = cb_cache_create(mcxt, 0);
	cb->cb_insert_relfilenode = NULL;
	cb->cb_insert_metabuffer = InvalidBuffer;
	cb->cb_insert_block = InvalidBlockNumber;
	cb->cb_insert_buffer = InvalidBuffer;
	return cb;
}

/*
 * Prepare for access to an existing conveyor belt.
 */
ConveyorBelt *
ConveyorBeltOpen(Relation rel, ForkNumber fork, MemoryContext mcxt)
{
	Buffer		metabuffer;
	CBMetapageData *meta;
	ConveyorBelt *cb;
	uint16		pages_per_segment;
	uint64		index_segments_moved;

	/* Read a few critical details from the metapage. */
	metabuffer = ReadBufferExtended(rel, fork, CONVEYOR_METAPAGE,
									RBM_NORMAL, NULL);
	LockBuffer(metabuffer, BUFFER_LOCK_SHARE);
	meta = cb_metapage_get_special(BufferGetPage(metabuffer));
	cb_metapage_get_critical_info(meta,
								  &pages_per_segment,
								  &index_segments_moved);
	UnlockReleaseBuffer(metabuffer);

	/* Initialize and return the ConveyorBelt object. */
	cb = MemoryContextAlloc(mcxt, sizeof(ConveyorBelt));
	cb->cb_rel = rel;
	cb->cb_fork = fork;
	cb->cb_pages_per_segment = pages_per_segment;
	cb->cb_cache = cb_cache_create(mcxt, index_segments_moved);
	cb->cb_insert_relfilenode = NULL;
	cb->cb_insert_metabuffer = InvalidBuffer;
	cb->cb_insert_block = InvalidBlockNumber;
	cb->cb_insert_buffer = InvalidBuffer;
	return cb;
}

/*
 * Get a new page to be added to a conveyor belt.
 *
 * On return, *pageno is set to the logical page number of the newly-added
 * page, and both the metapage and the returned buffer are exclusively locked.
 *
 * The intended use of this function is:
 *
 * buffer = ConveyorBeltGetNewPage(cb, &pageno);
 * page = BufferGetPage(buffer);
 * START_CRIT_SECTION();
 * // set page contents
 * ConveyorBeltPerformInsert(cb, buffer);
 * END_CRIT_SECTION();
 * ConveyorBeltCleanupInsert(cb, buffer);
 *
 * Note that because this function returns with buffer locks held, it's
 * important to do as little work as possible after this function returns
 * and before calling ConveyorBeltPerformInsert(). In particular, it's
 * completely unsafe to do anything complicated like SearchSysCacheN. Doing
 * so could result in undetected deadlock on the buffer LWLocks, or cause
 * a relcache flush that would break ConveyorBeltPerformInsert().
 *
 * Also note that the "set page contents" step must put some data in the
 * page, so that either pd_lower is greater than the minimum value
 * (SizeOfPageHeaderData) or pd_upper is less than the maximum value
 * (BLCKSZ).
 *
 * In future, we might want to provide the caller with an alternative to
 * calling ConveyorBeltPerformInsert, because that just logs an FPI for
 * the new page, and some callers might prefer to manage their own xlog
 * needs.
 */
Buffer
ConveyorBeltGetNewPage(ConveyorBelt *cb, CBPageNo *pageno)
{
	BlockNumber indexblock = InvalidBlockNumber;
	BlockNumber prevblock = InvalidBlockNumber;
	BlockNumber fsmblock = InvalidBlockNumber;
	BlockNumber	possibly_not_on_disk_blkno = CONVEYOR_METAPAGE + 1;
	Buffer		metabuffer;
	Buffer		indexbuffer = InvalidBuffer;
	Buffer		prevbuffer = InvalidBuffer;
	Buffer		fsmbuffer = InvalidBuffer;
	Buffer		buffer;
	CBPageNo	next_pageno;
	CBPageNo	previous_next_pageno = 0;
	CBSegNo		free_segno = CB_INVALID_SEGMENT;
	bool		needs_xlog;
	int			mode = BUFFER_LOCK_SHARE;
	int			iterations_without_next_pageno_change = 0;

	/*
	 * It would be really bad if someone called this function a second time
	 * while the buffer locks from a previous call were still held. So let's
	 * try to make sure that's not the case.
	 */
	Assert(!BufferIsValid(cb->cb_insert_metabuffer));
	Assert(!BufferIsValid(cb->cb_insert_buffer));

	/* Do any changes we make here need to be WAL-logged? */
	needs_xlog = RelationNeedsWAL(cb->cb_rel) || cb->cb_fork == INIT_FORKNUM;

	/*
	 * We don't do anything in this function that involves catalog access or
	 * accepts invalidation messages, so it's safe to cache this for the
	 * lifetime of this function. Since we'll return with buffer locks held,
	 * the caller had better not do anything like that either, so this should
	 * also still be valid when ConveyorBeltPerformInsert is called.
	 *
	 * XXX. This seems totally bogus, because we should really be doing
	 * CHECK_FOR_INTERRUPTS(), and that might accept invalidation messages.
	 */
	cb->cb_insert_relfilenode =
		&RelationGetSmgr(cb->cb_rel)->smgr_rnode.node;

	/*
	 * Read and pin the metapage.
	 *
	 * Among other things, this prevents concurrent truncations, as per the
	 * discussion in src/backend/access/conveyor/README.
	 */
	metabuffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork, CONVEYOR_METAPAGE,
									RBM_NORMAL, NULL);

	/*
	 * In the easy case where at least one payload segment exists, the newest
	 * payload segment is not full, and nobody else is trying to insert
	 * concurrently, this loop should only iterate once. However, we might not
	 * be that lucky.
	 *
	 * Since we don't want to hold the lock on the metapage while we go
	 * perform necessary preparatory work (e.g. searching through the FSM
	 * pages for a segment that can be allocated), we may find that after
	 * doing some amount of preparatory work and re-locking the metapage, the
	 * situation has changed under us. So we have to be prepared to keep going
	 * around until we get to a state where there's a non-full payload segment
	 * whose first unused page we can lock before someone else grabs it.
	 */
	while (1)
	{
		CBMetapageData *meta;
		CBMInsertState insert_state;
		BlockNumber next_blkno;
		CBPageNo	index_start;
		CBPageNo	index_metapage_start;
		CBSegNo		newest_index_segment;
		CBSegNo		next_segno;
		bool		can_allocate_segment;

		/*
		 * Examine the metapage to find out what we think we need to do in
		 * order to complete this operation.
		 *
		 * Initially, mode will be BUFFER_LOCK_SHARE. But if a previous pass
		 * through the loop found that we needed to allocate a new payload or
		 * index segement or move index entries out of the metapage, it will
		 * be BUFFER_LOCK_EXCLUSIVE. That's so that if nothing has changed
		 * concurrently, we can complete the operation before releasing the
		 * lock on the metapage.
		 *
		 * NB: Our rule is that the lock on the metapage is acquired last,
		 * after all other buffer locks. If any of indexbuffer, prevbuffer,
		 * and fsmbuffer are valid, they are also exclusively locked at this
		 * point.
		 */
		LockBuffer(metabuffer, mode);
		meta = cb_metapage_get_special(BufferGetPage(metabuffer));
		insert_state = cb_metapage_get_insert_state(meta, &next_blkno,
													&next_pageno, &next_segno,
													&index_start,
													&index_metapage_start,
													&newest_index_segment);

		/*
		 * There's no fixed upper bound on how many times this loop could
		 * iterate, because some other backend could be currently allocating
		 * pages, and that could prevent us from succeeding in allocating a
		 * page.
		 *
		 * However, if that's happening, the next logical page number should
		 * keep increasing. In the absence of any increase in the next logical
		 * page number, we might still need to iterate a few times, but
		 * not very many. For example, we might read the page the first time
		 * and realize that a new index segment is needed, create it on the
		 * second pass, move index entries into it on the third pass, and
		 * create a payload segment on the fourth pass, but then, barring
		 * concurrent activity, we should succeed in allocating a page on the
		 * next pass.
		 *
		 * Hence, if we loop a large number of times without a change in
		 * the next_pageno value, there's probably a bug. Error out instead
		 * of looping forever.
		 */
		if (next_pageno > previous_next_pageno)
		{
			previous_next_pageno = next_pageno;
			iterations_without_next_pageno_change = 0;
		}
		else if (++iterations_without_next_pageno_change >= 10)
			elog(ERROR,
				 "unable to make progress allocating page "
				 UINT64_FORMAT " (state = %d)",
				 next_pageno, (int) insert_state);

		/*
		 * next_segno need not exist on disk, but at least the first block
		 * of the previous segment should be there.
		 */
		if (next_segno > 0)
		{
			BlockNumber last_segno_first_blkno;

			last_segno_first_blkno =
				cb_segment_to_block(cb->cb_pages_per_segment,
									next_segno - 1, 0);
			if (last_segno_first_blkno > possibly_not_on_disk_blkno)
				possibly_not_on_disk_blkno = last_segno_first_blkno + 1;
		}

		/*
		 * If we need to allocate a payload or index segment, and we don't
		 * currently have a candidate, check whether the metapage knows of a
		 * free segment.
		 */
		if ((insert_state == CBM_INSERT_NEEDS_PAYLOAD_SEGMENT ||
			 insert_state == CBM_INSERT_NEEDS_INDEX_SEGMENT)
			&& free_segno == CB_INVALID_SEGMENT)
			free_segno = cb_metapage_find_free_segment(meta);

		/*
		 * If we need a new payload or index segment, see whether it's
		 * possible to complete that operation on this trip through the loop.
		 *
		 * This will only be possible if we've got an exclusive lock on the
		 * metapage.
		 *
		 * Furthermore, by rule, we cannot allocate a segment unless at least
		 * the first page of that segment is guaranteed to be on disk. This is
		 * certain to be true for any segment that's been allocated
		 * previously, but otherwise it's only true if we've verified that the
		 * size of the relation on disk is large enough.
		 */
		if (mode != BUFFER_LOCK_EXCLUSIVE ||
			free_segno == CB_INVALID_SEGMENT ||
			(insert_state != CBM_INSERT_NEEDS_PAYLOAD_SEGMENT
			 && insert_state != CBM_INSERT_NEEDS_INDEX_SEGMENT))
			can_allocate_segment = false;
		else
		{
			BlockNumber	free_segno_first_blkno;

			free_segno_first_blkno =
				cb_segment_to_block(cb->cb_pages_per_segment, free_segno, 0);
			can_allocate_segment =
				(free_segno_first_blkno < possibly_not_on_disk_blkno);
		}

		/*
		 * If it still looks like we can allocate, check for the case where we
		 * need a new index segment but don't have the other required buffer
		 * locks.
		 */
		if (can_allocate_segment &&
			insert_state == CBM_INSERT_NEEDS_INDEX_SEGMENT &&
			(!BufferIsValid(indexbuffer) || (!BufferIsValid(prevbuffer)
			&& newest_index_segment != CB_INVALID_SEGMENT)))
			can_allocate_segment = false;

		/*
		 * If it still looks like we can allocate, check for the case where
		 * the segment we planned to allocate is no longer free.
		 */
		if (can_allocate_segment)
		{
			/* fsmbuffer, if valid, is already exclusively locked. */
			if (BufferIsValid(fsmbuffer))
				can_allocate_segment =
					!cb_fsmpage_get_fsm_bit(BufferGetPage(fsmbuffer),
											free_segno);
			else
				can_allocate_segment =
					!cb_metapage_get_fsm_bit(meta, free_segno);

			/*
			 * If this segment turned out not to be free, we need a new
			 * candidate. Check the metapage here, and if that doesn't work
			 * out, free_segno will end up as CB_INVALID_SEGMENT, and we'll
			 * search the FSM pages further down.
			 */
			if (!can_allocate_segment)
				free_segno = cb_metapage_find_free_segment(meta);
		}

		/* If it STILL looks like we can allocate, do it! */
		if (can_allocate_segment)
		{
			if (insert_state == CBM_INSERT_NEEDS_PAYLOAD_SEGMENT)
			{
				cb_allocate_payload_segment(cb->cb_insert_relfilenode,
											cb->cb_fork, metabuffer,
											fsmblock, fsmbuffer, free_segno,
											free_segno >= next_segno,
											needs_xlog);

				/*
				 * We know for sure that there's now a payload segment that
				 * isn't full - and we know exactly where it's located.
				 */
				insert_state = CBM_INSERT_OK;
				next_blkno = cb_segment_to_block(cb->cb_pages_per_segment,
												 free_segno, 0);
			}
			else
			{
				Assert(insert_state == CBM_INSERT_NEEDS_INDEX_SEGMENT);

				cb_allocate_index_segment(cb->cb_insert_relfilenode,
										  cb->cb_fork, metabuffer,
										  indexblock, indexbuffer,
										  prevblock, prevbuffer,
										  fsmblock, fsmbuffer, free_segno,
										  index_metapage_start,
										  free_segno >= next_segno,
										  needs_xlog);

				/*
				 * We know for sure that there's now an index segment that
				 * isn't full, and our next move must be to relocate some
				 * index entries to that index segment.
				 */
				insert_state = CBM_INSERT_NEEDS_INDEX_ENTRIES_RELOCATED;
				next_blkno = indexblock;
			}

			/*
			 * Whether we allocated or not, the segment we intended to
			 * allocate is no longer free.
			 */
			free_segno = CB_INVALID_SEGMENT;
		}

		/*
		 * If we need to relocate index entries and if we have a lock on the
		 * correct index block, then go ahead and do it.
		 */
		if (insert_state == CBM_INSERT_NEEDS_INDEX_ENTRIES_RELOCATED &&
			next_blkno == indexblock)
		{
			unsigned	pageoffset;
			unsigned	num_index_entries;
			CBSegNo	    index_entries[CB_METAPAGE_INDEX_ENTRIES];
			CBPageNo	index_page_start;
			unsigned	logical_pages_in_index_segments;
			unsigned	index_entries_in_index_segments;

			logical_pages_in_index_segments =
				index_metapage_start - index_start;
			if (logical_pages_in_index_segments % cb->cb_pages_per_segment != 0)
				elog(ERROR, "index starts at " UINT64_FORMAT ", metapage index at " UINT64_FORMAT ", but there are %u pages per segment",
					 index_start, index_metapage_start,
					 cb->cb_pages_per_segment);
			index_entries_in_index_segments =
				logical_pages_in_index_segments / cb->cb_pages_per_segment;
			pageoffset =
				index_entries_in_index_segments % CB_INDEXPAGE_INDEX_ENTRIES;

			num_index_entries = Min(CB_METAPAGE_INDEX_ENTRIES,
									CB_INDEXPAGE_INDEX_ENTRIES - pageoffset);
			cb_metapage_get_index_entries(meta, num_index_entries,
										  index_entries);
			index_page_start = index_metapage_start -
				pageoffset * cb->cb_pages_per_segment;
			cb_relocate_index_entries(cb->cb_insert_relfilenode, cb->cb_fork,
									  metabuffer, indexblock, indexbuffer,
									  pageoffset, num_index_entries,
									  index_entries, index_page_start,
									  needs_xlog);
		}

		/* Release buffer locks and, except for the metapage, also pins. */
		LockBuffer(metabuffer, BUFFER_LOCK_UNLOCK);
		if (BufferIsValid(indexbuffer))
		{
			UnlockReleaseBuffer(indexbuffer);
			indexblock = InvalidBlockNumber;
			indexbuffer = InvalidBuffer;
		}
		if (BufferIsValid(prevbuffer))
		{
			UnlockReleaseBuffer(prevbuffer);
			prevblock = InvalidBlockNumber;
			prevbuffer = InvalidBuffer;
		}
		if (BufferIsValid(fsmbuffer))
		{
			UnlockReleaseBuffer(fsmbuffer);
			fsmblock = InvalidBlockNumber;
			fsmbuffer = InvalidBuffer;
		}

		if (insert_state != CBM_INSERT_OK)
		{
			/*
			 * Some sort of preparatory work will be needed in order to insert
			 * a new page, which will require modifying the metapage.
			 * Therefore, next time we lock it, we had better grab an
			 * exclusive lock.
			 */
			mode = BUFFER_LOCK_EXCLUSIVE;
		}
		else
		{
			/* Extend the relation if needed. */
			buffer = ConveyorBeltExtend(cb, next_blkno,
										&possibly_not_on_disk_blkno);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * If the target buffer is still unused, we're done. Otherwise,
			 * someone else grabbed that page before we did, so we must fall
			 * through and retry.
			 */
			if (ConveyorBeltPageIsUnused(BufferGetPage(buffer)))
			{
				/*
				 * Remember things that we'll need to know when the caller
				 * invokes ConveyorBeltPerformInsert and
				 * ConveyorBeltCleanupInsert.
				 */
				cb->cb_insert_block = next_blkno;
				cb->cb_insert_buffer = buffer;
				cb->cb_insert_metabuffer = metabuffer;

				/* Success, so escape toplevel retry loop. */
				break;
			}

			/* We'll have to retry with a different buffer. */
			UnlockReleaseBuffer(buffer);
		}

		/*
		 * If the metapage has no more space for index entries, but there's
		 * an index segment into which some of the existing ones could be
		 * moved, then cb_metapage_get_insert_state will have set next_blkno
		 * to the point to the block to which index entries should be moved.
		 *
		 * If the target index segment is the very last one in the conveyor
		 * belt and we're using the pages of that segment for the very first
		 * time, the target page may not exist yet, so be prepared to extend
		 * the relation.
		 */
		if (insert_state == CBM_INSERT_NEEDS_INDEX_ENTRIES_RELOCATED)
		{
			indexblock = next_blkno;
			indexbuffer = ConveyorBeltExtend(cb, indexblock,
											 &possibly_not_on_disk_blkno);
		}

		/*
		 * If we need to add a new index segment and it's not the very first
		 * one, we'll have to update the newest index page with a pointer to
		 * the index page we're going to add, so we must read and pin that
		 * page.
		 *
		 * The names "prevblock" and "prevbuffer" are intended to signify that
		 * what is currently the newest index segment will become the previous
		 * segment relative to the one we're going to add.
		 */
		if (insert_state == CBM_INSERT_NEEDS_INDEX_SEGMENT &&
			newest_index_segment != CB_INVALID_SEGMENT)
		{
			prevblock = cb_segment_to_block(cb->cb_pages_per_segment,
											newest_index_segment, 0);
			prevbuffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork,
											prevblock, RBM_NORMAL, NULL);
		}

		/*
		 * If we need to add a new segment of either type, make provisions to
		 * do so.
		 */
		if (insert_state == CBM_INSERT_NEEDS_PAYLOAD_SEGMENT ||
			insert_state == CBM_INSERT_NEEDS_INDEX_SEGMENT)
		{
			/*
			 * Search the FSM pages (and create a new one if needed) for a
			 * free segment, unless we've already have a candidate.
			 */
			if (free_segno == CB_INVALID_SEGMENT)
				free_segno = ConveyorSearchFSMPages(cb, next_segno, &fsmblock,
													&fsmbuffer);

			if (free_segno > next_segno)
			{
				/*
				 * If the FSM thinks that we ought to allocate a segment
				 * beyond what we think to be the very next one, then someone
				 * else must have concurrently added a segment, so we'll need
				 * to loop around, retake the metapage lock, refresh our
				 * knowledge of next_segno, and then find a new segment to
				 * allocate.
				 */
				free_segno = CB_INVALID_SEGMENT;
			}
			else if (free_segno == next_segno)
			{
				BlockNumber free_block;
				Buffer		free_buffer;

				/*
				 * We're allocating a new segment. At least the first page must
				 * exist on disk before we perform the allocation, which means
				 * we may need to add blocks to the relation fork.
				 */
				free_block = cb_segment_to_block(cb->cb_pages_per_segment,
												 free_segno, 0);
				free_buffer = ConveyorBeltExtend(cb, free_block,
												 &possibly_not_on_disk_blkno);
				if (insert_state == CBM_INSERT_NEEDS_INDEX_SEGMENT)
				{
					indexblock = free_block;
					indexbuffer = free_buffer;
				}
				else
					ReleaseBuffer(free_buffer);
			}
		}

		/*
		 * Prepare for next attempt by reacquiring all relevant buffer locks,
		 * except for the one on the metapage, which is acquired at the top of
		 * the loop.
		 */
		if (BufferIsValid(indexbuffer))
			LockBuffer(indexbuffer, BUFFER_LOCK_EXCLUSIVE);
		if (BufferIsValid(prevbuffer))
			LockBuffer(prevbuffer, BUFFER_LOCK_EXCLUSIVE);
		if (BufferIsValid(fsmbuffer))
			LockBuffer(fsmbuffer, BUFFER_LOCK_EXCLUSIVE);
	}

	/*
	 * Relock the metapage. Caller should immediately start a critical section
	 * and populate the buffer.
	 */
	LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);

	/* All done. */
	*pageno = next_pageno;
	return buffer;
}

/*
 * Actually insert a new page into the conveyor belt.
 *
 * See ConveyorBeltGetNewPage for the intended usage of this fucntion.
 */
void
ConveyorBeltPerformInsert(ConveyorBelt *cb, Buffer buffer)
{
	bool		needs_xlog;

	/*
	 * We don't really need the caller to tell us which buffer is involved,
	 * because we already have that information. We insist on it anyway as a
	 * debugging cross-check.
	 */
	if (cb->cb_insert_buffer != buffer)
	{
		if (BufferIsValid(cb->cb_insert_buffer))
			elog(ERROR, "there is no pending insert");
		else
			elog(ERROR,
				 "pending insert expected for buffer %u but got buffer %u",
				 cb->cb_insert_buffer, buffer);
	}

	/*
	 * ConveyorBeltPageIsUnused is used by ConveyorBeltGetNewPage to figure
	 * out whether a concurrent inserter got there first. Here, we're the
	 * concurrent inserter, and must have initialized the page in a way that
	 * makes that function return false for the newly-inserted page, so that
	 * other backends can tell we got here first.
	 */
	if (ConveyorBeltPageIsUnused(BufferGetPage(buffer)))
		elog(ERROR, "can't insert an unused page");

	/* Caller should be doing this inside a critical section. */
	Assert(CritSectionCount > 0);

	/* We should have the details stashed by ConveyorBeltGetNewPage. */
	Assert(cb->cb_insert_relfilenode != NULL);
	Assert(BufferIsValid(cb->cb_insert_metabuffer));
	Assert(BufferIsValid(cb->cb_insert_buffer));
	Assert(BlockNumberIsValid(cb->cb_insert_block));

	/* Update metapage, mark buffers dirty, and write XLOG if required. */
	needs_xlog = RelationNeedsWAL(cb->cb_rel) || cb->cb_fork == INIT_FORKNUM;
	cb_insert_payload_page(cb->cb_insert_relfilenode, cb->cb_fork,
						   cb->cb_insert_metabuffer,
						   cb->cb_insert_block, buffer,
						   needs_xlog);

	/*
	 * Buffer locks will be released by ConveyorBeltCleanupInsert, but we can
	 * invalidate some other fields now.
	 */
	cb->cb_insert_relfilenode = NULL;
	cb->cb_insert_block = InvalidBlockNumber;
}

/*
 * Clean up following the insertion of a new page into the conveyor belt.
 *
 * See ConveyorBeltGetNewPage for the intended usage of this fucntion.
 */
void
ConveyorBeltCleanupInsert(ConveyorBelt *cb, Buffer buffer)
{
	/* Debugging cross-check, like ConveyorBeltPerformInsert. */
	if (cb->cb_insert_buffer != buffer)
	{
		if (BufferIsValid(cb->cb_insert_buffer))
			elog(ERROR, "there is no pending insert");
		else
			elog(ERROR,
				 "pending insert expected for buffer %u but got buffer %u",
				 cb->cb_insert_buffer, buffer);
	}

	/* Release buffer locks and pins. */
	Assert(BufferIsValid(cb->cb_insert_buffer));
	Assert(BufferIsValid(cb->cb_insert_metabuffer));
	UnlockReleaseBuffer(cb->cb_insert_buffer);
	UnlockReleaseBuffer(cb->cb_insert_metabuffer);
	cb->cb_insert_buffer = InvalidBuffer;
	cb->cb_insert_metabuffer = InvalidBuffer;
}

/*
 * Read a logical page from a conveyor belt. If the page has already been
 * truncated away or has not yet been created, returns InvalidBuffer.
 * Otherwise, reads the page using the given strategy and locks it using
 * the given buffer lock mode.
 */
Buffer
ConveyorBeltReadBuffer(ConveyorBelt *cb, CBPageNo pageno, int mode,
					   BufferAccessStrategy strategy)
{
	BlockNumber index_blkno,
				payload_blkno;
	Buffer		metabuffer,
				index_buffer,
				payload_buffer;
	CBMetapageData *meta;
	CBPageNo	index_start,
				index_metapage_start,
				target_index_segment_start;
	CBSegNo		oldest_index_segment,
				newest_index_segment,
				index_segno;
	unsigned	lppis,
				segoff;
	uint64		index_segments_moved;

	Assert(mode == BUFFER_LOCK_EXCLUSIVE || mode == BUFFER_LOCK_SHARE);

	/*
	 * Lock the metapage and get all the information we need from it. Then
	 * drop the lock on the metapage, but retain the pin, so that neither the
	 * target payload page nor any index page we might need to access can be
	 * concurrently truncated away. See the README for futher details.
	 */
	metabuffer = ConveyorBeltRead(cb, CONVEYOR_METAPAGE, BUFFER_LOCK_SHARE);
	meta = cb_metapage_get_special(BufferGetPage(metabuffer));
	if (!cb_metapage_find_logical_page(meta, pageno, &payload_blkno))
	{
		/* Page number too old or too new. */
		UnlockReleaseBuffer(metabuffer);
		return InvalidBuffer;
	}
	if (payload_blkno != InvalidBlockNumber)
	{
		/* Index entry for payload page found on metapage. */
		LockBuffer(metabuffer, BUFFER_LOCK_UNLOCK);
		payload_buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork,
											payload_blkno, RBM_NORMAL,
											strategy);
		LockBuffer(payload_buffer, mode);
		ReleaseBuffer(metabuffer);
		return payload_buffer;
	}
	cb_metapage_get_index_info(meta, &index_start, &index_metapage_start,
							   &oldest_index_segment, &newest_index_segment,
							   &index_segments_moved);
	LockBuffer(metabuffer, BUFFER_LOCK_UNLOCK);

	/* Invalidate any obsolete cache entries. */
	cb_cache_invalidate(cb->cb_cache, index_start, index_segments_moved);

	/*
	 * It's convenient to identify index segments in terms of the first
	 * logical page for which that index segment contains the necessary index
	 * entry. So, take the page number that we were given, and back it up to
	 * the previous index-segment boundary.
	 */
	lppis = cb_logical_pages_per_index_segment(cb->cb_pages_per_segment);
	target_index_segment_start = pageno - (pageno - index_start) % lppis;

	/* Search the cache first. Try other strategies if that does not work. */
	index_segno = cb_cache_lookup(cb->cb_cache, target_index_segment_start);
	if (index_segno == CB_INVALID_SEGMENT)
	{
		if (index_start == target_index_segment_start)
		{
			/* Looks like it's the oldest index segment. */
			index_segno = oldest_index_segment;
		}
		else if (index_metapage_start - lppis == target_index_segment_start)
		{
			/*
			 * Looks like it's the newest index segment.
			 *
			 * It's worth adding a cache entry for this, because we might end
			 * up needing it again later, when it's no longer the newest
			 * entry.
			 */
			index_segno = newest_index_segment;
			cb_cache_insert(cb->cb_cache, index_segno,
							target_index_segment_start);
		}
		else
		{
			CBPageNo	index_segment_start;

			/*
			 * We don't know where it is and it's not the first or last index
			 * segment, so we have to walk the chain of index segments to find
			 * it.
			 *
			 * That's possibly going to be slow, especially if there are a lot
			 * of index segments. However, maybe we can make it a bit faster.
			 * Instead of starting with the oldest segment and moving forward
			 * one segment at a time until we find the one we want, search the
			 * cache for the index segment that most nearly precedes the one
			 * we want.
			 */
			index_segno = cb_cache_fuzzy_lookup(cb->cb_cache,
												target_index_segment_start,
												&index_segment_start);
			if (index_segno == CB_INVALID_SEGMENT)
			{
				/*
				 * Sadly, the cache is either entirely empty or at least has
				 * no entries for any segments older than the one we want, so
				 * we have to start our search from the oldest segment.
				 */
				index_segno = oldest_index_segment;
			}

			/*
			 * Here's where we actually search. Make sure to cache the
			 * results, in case there are more lookups later.
			 */
			while (index_segment_start < target_index_segment_start)
			{
				CHECK_FOR_INTERRUPTS();

				index_blkno = cb_segment_to_block(cb->cb_pages_per_segment,
												  index_segno, 0);
				index_buffer = ConveyorBeltRead(cb, index_blkno,
												BUFFER_LOCK_SHARE);
				index_segno =
					cb_indexpage_get_next_segment(BufferGetPage(index_buffer));
				UnlockReleaseBuffer(index_buffer);
				index_segment_start += lppis;
				cb_cache_insert(cb->cb_cache, index_segno, index_segment_start);
			}
		}
	}

	/*
	 * We know which index segment we need to read, so now figure out which
	 * page we need from that segment, and then which physical block we need.
	 */
	segoff = (pageno - target_index_segment_start) /
		cb_logical_pages_per_index_page(cb->cb_pages_per_segment);
	index_blkno = cb_segment_to_block(cb->cb_pages_per_segment,
									  index_segno, segoff);

	/* Read the required index entry. */
	index_buffer = ConveyorBeltRead(cb, index_blkno, BUFFER_LOCK_SHARE);
	payload_blkno = cb_indexpage_find_logical_page(BufferGetPage(index_buffer),
												   pageno,
												   cb->cb_pages_per_segment);
	UnlockReleaseBuffer(index_buffer);

	/* Now we can read and lock the actual payload block. */
	payload_buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork,
										payload_blkno, RBM_NORMAL,
										strategy);
	LockBuffer(payload_buffer, mode);

	/*
	 * Since we've now got the payload block locked, we can release the pin on
	 * the metapage.
	 */
	ReleaseBuffer(metabuffer);
	return payload_buffer;
}

/*
 * Find out which logical page numbers are currently valid.
 *
 * On return, *oldest_logical_page will be set to the smallest page number
 * that has not yet been removed by truncation, and *next_logical_page will
 * be set to the smallest page number that does not yet exist.
 *
 * Note that, unless the caller knows that there cannot be concurrent
 * truncations or insertions in progress, either value might be out of
 * date by the time it is used.
 */
void
ConveyorBeltGetBounds(ConveyorBelt *cb, CBPageNo *oldest_logical_page,
					  CBPageNo *next_logical_page)
{
	Buffer		metabuffer;
	CBMetapageData *meta;

	metabuffer = ConveyorBeltRead(cb, CONVEYOR_METAPAGE, BUFFER_LOCK_SHARE);
	meta = cb_metapage_get_special(BufferGetPage(metabuffer));
	cb_metapage_get_bounds(meta, oldest_logical_page, next_logical_page);
	UnlockReleaseBuffer(metabuffer);
}

/*
 * Update the conveyor belt's notion of the oldest logical page to be kept.
 *
 * This doesn't physically shrink the relation, nor does it even make space
 * available for reuse by future insertions. It just makes pages prior to
 * 'oldest_keeper' unavailable, thus potentially allowing the segments
 * containing those pages to be freed by a future call to ConveyorBeltVacuum.
 *
 * A call to this function shouldn't try to move the logical truncation point
 * backwards. That is, the value of 'oldest_keeper' should always be greater
 * than or equal to the value passed on the previous call for this conveyor
 * belt. It also shouldn't try to move the logical truncation point beyond
 * the current insertion point: don't try to throw away data that hasn't been
 * inserted yet!
 *
 * For routine cleanup of a conveyor belt, the recommended sequence of calls
 * is ConveyorBeltLogicalTruncate then ConveyorBeltVacuum then
 * ConveyorBeltPhysicalTruncate. For more aggressive cleanup options, see
 * ConveyorBeltCompact or ConveyorBeltRewrite.
 */
void
ConveyorBeltLogicalTruncate(ConveyorBelt *cb, CBPageNo oldest_keeper)
{
	Buffer		metabuffer;
	CBMetapageData *meta;
	CBPageNo	oldest_logical_page;
	CBPageNo	next_logical_page;
	RelFileNode *rnode;
	bool		needs_xlog;

	/*
	 * We must take a cleanup lock to adjust the logical truncation point,
	 * as per the locking protocols in src/backend/access/conveyor/README.
	 */
	metabuffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork, CONVEYOR_METAPAGE,
									RBM_NORMAL, NULL);
	LockBufferForCleanup(metabuffer);

	/* Sanity checks. */
	meta = cb_metapage_get_special(BufferGetPage(metabuffer));
	cb_metapage_get_bounds(meta, &oldest_logical_page, &next_logical_page);
	if (oldest_keeper < oldest_logical_page)
		elog(ERROR,
			 "can't move truncation point backwards from " UINT64_FORMAT " to " UINT64_FORMAT,
			 oldest_logical_page, oldest_keeper);
	if (oldest_keeper > next_logical_page)
		elog(ERROR,
			 "can't move truncation point to " UINT64_FORMAT " beyond insert point " UINT64_FORMAT,
			 oldest_keeper, next_logical_page);


	/* Do the real work. */
	rnode = &RelationGetSmgr(cb->cb_rel)->smgr_rnode.node;
	needs_xlog = RelationNeedsWAL(cb->cb_rel) || cb->cb_fork == INIT_FORKNUM;
	cb_logical_truncate(rnode, cb->cb_fork, metabuffer, oldest_keeper,
						needs_xlog);

	/* Release buffer lock. */
	UnlockReleaseBuffer(metabuffer);
}

/*
 * Recycle segments that are no longer needed.
 *
 * Payload segments all of whose pages precede the logical truncation point
 * can be deallocated. Index segments can be deallocated once they no longer
 * contain any pointers to payload segments.
 *
 * Only one backend should call this at a time for any given conveyor belt.
 */
void
ConveyorBeltVacuum(ConveyorBelt *cb)
{
	Buffer		metabuffer;
	BlockNumber	fsmblock = InvalidBlockNumber;
	Buffer		fsmbuffer = InvalidBuffer;
	CBSegNo		cleared_segno = CB_INVALID_SEGMENT;
	bool		needs_xlog;
	bool		cleaned_index_segments = false;

	/* Do any changes we make here need to be WAL-logged? */
	needs_xlog = RelationNeedsWAL(cb->cb_rel) || cb->cb_fork == INIT_FORKNUM;

	/* Read and pin the metapage. */
	metabuffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork, CONVEYOR_METAPAGE,
									RBM_NORMAL, NULL);
	LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Main loop.
	 *
	 * At the top of each loop iteration, the metabuffer is pinned and
	 * exclusively locked.  The lock and even the pin may be released by code
	 * inside this loop, but they must be reacquired before beginning the next
	 * iteration.
	 */
	while (1)
	{
		CBMetapageData	   *meta;
		CBMObsoleteState	obsolete_state;
		CBSegNo		oldest_index_segment;
		CBPageNo	index_vacuum_stop_point;
		CBSegNo		metapage_segno;
		unsigned	metapage_offset;

		/* Assess what kind of work needs to be done. */
		meta = cb_metapage_get_special(BufferGetPage(metabuffer));
		obsolete_state =
			cb_metapage_get_obsolete_state(meta, &oldest_index_segment,
										   &index_vacuum_stop_point,
										   &metapage_segno, &metapage_offset);

		/*
		 * If on the previous pass through the loop we concluded that we need
		 * to free a payload segment refrenced by the metapage and if that no
		 * longer seems like the thing we need to do, then release any lock and
		 * pin we may have acquired in preparation for freeing that payload
		 * segment.
		 */
		if ((obsolete_state != CBM_OBSOLETE_METAPAGE_ENTRIES ||
			metapage_segno != cleared_segno) && fsmblock != InvalidBlockNumber)
		{
			UnlockReleaseBuffer(fsmbuffer);
			fsmblock = InvalidBlockNumber;
			fsmbuffer = InvalidBuffer;
		}

		/*
		 * Attempt to do whatever useful work seems to be possible based on
		 * obsolete_state.
		 */
		if (obsolete_state == CBM_OBSOLETE_NOTHING)
		{
			/*
			 * There is nothing to vacuum.
			 */
			UnlockReleaseBuffer(metabuffer);
			return;
		}
		else if (obsolete_state == CBM_OBSOLETE_METAPAGE_START)
		{
			/*
			 * No real work to do, but there are some already-cleared entries
			 * at the start of the metapage which we should remove to make more
			 * space for new entries.
			 */
			cb_shift_metapage_index(&RelationGetSmgr(cb->cb_rel)->smgr_rnode.node,
									cb->cb_fork, metabuffer, metapage_offset, needs_xlog);
			UnlockReleaseBuffer(metabuffer);
			return;
		}
		else if (obsolete_state == CBM_OBSOLETE_METAPAGE_ENTRIES)
		{
			/*
			 * The metapage contains entries for one or more payload segments
			 * which can be deallocated.
			 */
			if (metapage_segno != cleared_segno)
			{
				/*
				 * We can only recycle a payload segment after clearing the
				 * pages in that segment. Since we have not done that yet,
				 * do it now. First release the buffer lock on the metapage,
				 * to avoid interefering with other use of the conveyor belt.
				 */
				LockBuffer(metabuffer, BUFFER_LOCK_UNLOCK);
				ConveyorBeltClearSegment(cb, metapage_segno, true);
				cleared_segno = metapage_segno;

				/*
				 * Lock the relevant FSM page, if it's not the metapage.
				 * Per src/backend/access/conveyor/README's locking rules,
				 * we must do this before relocking the metapage.
				 */
				fsmblock = ConveyorBeltFSMBlockNumber(cb, cleared_segno);
				if (fsmblock != InvalidBlockNumber)
					fsmbuffer = ConveyorBeltRead(cb, fsmblock,
												 BUFFER_LOCK_EXCLUSIVE);


				/*
				 * OK, now reacquire a lock on the metapage and loop around.
				 * Hopefully, the next pass will succeed in freeing a payload
				 * segment.
				 */
				LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);
			}
			else
			{
				/*
				 * The previous pass through the loop made preparations to
				 * free this payload segment, so now we can do it.
				 */
				cb_recycle_payload_segment(&RelationGetSmgr(cb->cb_rel)->smgr_rnode.node,
										   cb->cb_fork,
										   metabuffer,
										   InvalidBlockNumber, InvalidBuffer,
										   fsmblock, fsmbuffer,
										   cleared_segno, metapage_offset,
										   needs_xlog);
			}
		}
		else if (obsolete_state == CBM_OBSOLETE_SEGMENT_ENTRIES)
		{
			unsigned	empty_index_segments = 0;
			CBSegNo		index_segment = oldest_index_segment;

			/*
			 * Do this part just once. A single pass through the logic below
			 * should clean out the index segments as completely as possible,
			 * so if we end up here again, either the logical truncation point
			 * changed concurrently, or there's actually nothing to do. Even
			 * in the former case, it's OK to return without doing anything
			 * further, because this function only promises to clean up data
			 * that was no longer needed as of the time it was called. It makes
			 * no promises about cleaning up things that became obsolete once
			 * this function was already running.
			 */
			if (cleaned_index_segments)
			{
				UnlockReleaseBuffer(metabuffer);
				break;
			}
			cleaned_index_segments = true;

			/*
			 * Release lock on metapage before locking other pages, but keep
			 * the pin for efficiency and so that no index segments can
			 * disappear concurrently.
			 */
			LockBuffer(metabuffer, BUFFER_LOCK_UNLOCK);

			/*
			 * Clear as many obsolete index entries out of index segments as
			 * we can.
			 */
			while (index_segment != CB_INVALID_SEGMENT &&
				   ConveyorBeltClearIndexSegmentEntries(cb, metabuffer,
														index_segment,
														index_vacuum_stop_point,
														&index_segment))
				++empty_index_segments;

			/*
			 * Free old index segments.
			 *
			 * We might stop before freeing the requested number of index
			 * segments, due to concurrent locking. If that happens,
			 * give up on performing any further cleanup.
			 */
			while (empty_index_segments > 0)
			{
				oldest_index_segment =
					ConveyorBeltFreeOldestIndexSegment(cb, metabuffer,
													   oldest_index_segment,
													   index_vacuum_stop_point);
				--empty_index_segments;
				if (empty_index_segments > 0 &&
					oldest_index_segment == CB_INVALID_SEGMENT)
				{
					ReleaseBuffer(metabuffer);
					return;
				}
			}

			/*
			 * If we freed some but not all index segments, all the entries in
			 * the metapage are still needed, so there is no point in trying to
			 * clean it up.
			 */
			if (oldest_index_segment != CB_INVALID_SEGMENT)
			{
				ReleaseBuffer(metabuffer);
				return;
			}

			/*
			 * Relock the metapage prior to looping around. We may still be
			 * able to clear index entries from the metapage, or adjust the
			 * start of the metapage index.
			 */
			LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);
		}
	}
}

/*
 * Clear obsolete index entries from a segment.
 *
 * metabuffer should be pinned but not locked when this function is called,
 * and will be in the same state upon return.
 *
 * index_segment specifies the target index segment.
 *
 * index_vacuum_stop_point defines the point beyond which no index entries
 * may be removed. If an index entry is found all or part of which would cover
 * pages greater than or equal to this value, then this function does nothing
 * further and returns false. If this limit is not reached, this function
 * returns true.
 *
 * *next_index_segment is set to the segment number of the index segment
 * that follows the one specified by index_segment, or CB_INVALID_SEGMENT
 * if none.
 */
static bool
ConveyorBeltClearIndexSegmentEntries(ConveyorBelt *cb, Buffer metabuffer,
									 CBSegNo index_segment,
									 CBPageNo index_vacuum_stop_point,
									 CBSegNo *next_index_segment)
{
	bool		needs_xlog;
	bool		need_next_segment = true;
	unsigned	segoff;
	BlockNumber	fsmblock = InvalidBlockNumber;
	Buffer		fsmbuffer = InvalidBuffer;

	/* Do we need to write XLOG for operations on this conveyor belt? */
	needs_xlog = RelationNeedsWAL(cb->cb_rel) || cb->cb_fork == INIT_FORKNUM;

	for (segoff = 0; segoff < cb->cb_pages_per_segment; ++segoff)
	{
		BlockNumber	indexblock;
		Buffer		indexbuffer;
		Page		indexpage;
		unsigned	pageoffset = 0;
		CBSegNo		cleared_segno = CB_INVALID_SEGMENT;

		indexblock = cb_segment_to_block(cb->cb_pages_per_segment,
										 index_segment, segoff);
		indexbuffer = ConveyorBeltRead(cb, indexblock, BUFFER_LOCK_EXCLUSIVE);
		indexpage = BufferGetPage(indexbuffer);

		/*
		 * If an index segment page is not initialized, treat it the same
		 * way as if it is initialized but contains no entries.
		 */
		if (ConveyorBeltPageIsUnused(indexpage))
		{
			if (segoff == 0)
				elog(ERROR,
					 "conveyor belt index page at segno %u offset 0 should be initialied",
					 index_segment);
			if (*next_index_segment != CB_INVALID_SEGMENT)
				elog(ERROR,
					 "non-final index segment page at segno %u offset %u should be initialized",
					 index_segment, segoff);
			return true;
		}

		/*
		 * If this is the very first time we've locked an index page in this
		 * segment, it should be the first page, and it will tell us where to
		 * find the next segment once we finish with this one. Grab that
		 * information while we have the page lock.
		 */
		if (need_next_segment)
		{
			Assert(segoff == 0);
			*next_index_segment = cb_indexpage_get_next_segment(indexpage);
			need_next_segment = false;
		}

		/*
		 * Loop over the index entries in this page.
		 *
		 * At the top of each iteration of the loop, the index page is
		 * exclusively locked. The lock may be released and reacquired before
		 * beginning the next iteration.
		 */
		while (pageoffset < CB_INDEXPAGE_INDEX_ENTRIES)
		{
			CBSegNo		segno;
			CBPageNo	first_page;

			/* Find, or reconfirm, the location of the next obsolete entry. */
			segno = cb_indexpage_get_obsolete_entry(indexpage, &pageoffset,
													&first_page);
			if (segno == CB_INVALID_SEGMENT)
			{
				/* No items remain in this page. */
				UnlockReleaseBuffer(indexbuffer);
				break;
			}
			if (first_page + (cb->cb_pages_per_segment * pageoffset) +
				cb->cb_pages_per_segment > index_vacuum_stop_point)
			{
				/*
				 * At least one entry from this page is still needed, so no
				 * point in visiting future pages in this index segment, and
				 * no point in visiting any more index segments.
				 */
				UnlockReleaseBuffer(indexbuffer);
				return false;
			}

			/*
			 * If this is the first time we've considered clearing this
			 * particular payload segment, we'll need to release the buffer
			 * lock, do some necessary prep work, reacquire the buffer lock,
			 * and recheck to make sure nothing has changed.
			 */
			if (segno != cleared_segno)
			{
				BlockNumber	newfsmblock;

				/* Release lock on index page. */
				LockBuffer(indexbuffer, BUFFER_LOCK_UNLOCK);

				/*
				 * Clear the segment that we want to recycle.
				 *
				 * Note that we could crash or error out while or after doing
				 * this and before we actually recycle the segment. If so,
				 * we'll do it again the next time someone tries to vacuum
				 * this conveyor belt.  All of that is fine, because nobody
				 * can be looking at the data any more, and clearing the pages
				 * is idempotent.
				 */
				ConveyorBeltClearSegment(cb, segno, true);

				/*
				 * Make sure that we have the correct FSM buffer pinned.
				 *
				 * Often, any FSM buffer that we have pinned previously will
				 * still be the correct one, either because segment numbers
				 * allocated around the same time are likely to be close
				 * together numerically, or just because the conveyor belt may
				 * not be big enough to need lots of FSM pages.
				 *
				 * However, in the worst case, this can change every time.
				 */
				newfsmblock = cb_segment_to_fsm_block(cb->cb_pages_per_segment,
													  segno);
				if (fsmblock != newfsmblock)
				{
					ReleaseBuffer(fsmbuffer);
					fsmblock = newfsmblock;
					if (fsmblock == InvalidBlockNumber)
						fsmbuffer = InvalidBuffer;
					else
						fsmbuffer =
							ReadBufferExtended(cb->cb_rel, cb->cb_fork,
											   fsmblock, RBM_NORMAL, NULL);
				}

				/* Relock the index page and go around. */
				LockBuffer(indexbuffer, BUFFER_LOCK_EXCLUSIVE);
				cleared_segno = segno;
				continue;
			}

			/*
			 * Clear the index entry referrring to the payload segment, and
			 * mark the segment free. To do this, we have to grab the lock
			 * on whatever page contains the free/busy state, which could be
			 * either an FSM page or the metapage.
			 */
			if (fsmblock == InvalidBlockNumber)
			{
				LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);
				cb_recycle_payload_segment(&RelationGetSmgr(cb->cb_rel)->smgr_rnode.node,
										   cb->cb_fork,
										   metabuffer,
										   indexblock, indexbuffer,
										   InvalidBlockNumber, InvalidBuffer,
										   segno, pageoffset, needs_xlog);
				LockBuffer(metabuffer, BUFFER_LOCK_UNLOCK);
			}
			else
			{
				LockBuffer(fsmbuffer, BUFFER_LOCK_EXCLUSIVE);
				cb_recycle_payload_segment(&RelationGetSmgr(cb->cb_rel)->smgr_rnode.node,
										   cb->cb_fork,
										   InvalidBuffer,
										   indexblock, indexbuffer,
										   fsmblock, fsmbuffer,
										   segno, pageoffset, needs_xlog);
				LockBuffer(fsmbuffer, BUFFER_LOCK_UNLOCK);
			}

			/* No need to consider this page offset again. */
			++pageoffset;

			/* Now we're no longer prepared to clear any segment. */
			cleared_segno = CB_INVALID_SEGMENT;
		}
	}

	return true;
}

/*
 * Attempt to remve the oldest index segment.
 *
 * The return value is the segment number of the oldest index segment that
 * remains after the operation has been completed. If no index segments remain
 * after the operation or if the operation cannot be completed, the return
 * value is CB_INVALID_SEGMENT.
 */
static CBSegNo
ConveyorBeltFreeOldestIndexSegment(ConveyorBelt *cb, Buffer metabuffer,
								   CBSegNo oldest_index_segment,
								   CBPageNo index_vacuum_stop_point)
{
	BlockNumber	firstindexblock;
	Buffer		firstindexbuffer;
	BlockNumber	fsmblock;
	Buffer		fsmbuffer;
	bool		needs_xlog;
	CBSegNo		oldest_remaining_index_segment = CB_INVALID_SEGMENT;

	/*
	 * Clear all the blocks in the oldest index segment except for the first.
	 * We must keep the first one until the bitter end, so that it remains
	 * possible to walk the chain of index segments.
	 */
	ConveyorBeltClearSegment(cb, oldest_index_segment, false);

	/*
	 * Read and pin the first block of the index segment.
	 */
	needs_xlog = RelationNeedsWAL(cb->cb_rel) || cb->cb_fork == INIT_FORKNUM;
	firstindexblock = cb_segment_to_block(cb->cb_pages_per_segment,
										  oldest_index_segment, 0);
	firstindexbuffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork,
										  firstindexblock, RBM_NORMAL, NULL);

	/*
	 * Also read and pin the appropriate FSM page, unless the busy/free status
	 * of this segment is stored in the metapage.
	 */
	fsmblock = cb_segment_to_fsm_block(cb->cb_pages_per_segment,
									   oldest_index_segment);
	if (fsmblock == InvalidBlockNumber)
		fsmbuffer = InvalidBuffer;
	else
		fsmbuffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork,
									   fsmblock, RBM_NORMAL, NULL);

	/*
	 * The lock ordering described in the README requires the metapage lock
	 * to be taken last, but it also requires that freeing an index segment
	 * take a cleanup lock on the metapage. Since a concurrent reader will
	 * hold a pin on the metapage when trying to lock the first index page,
	 * we can't lock the first index page and then wait for a cleanup lock
	 * on the metapage, because that might deadlock.
	 *
	 * To get around that problem, we take the cleanup lock on the metabuffer
	 * conditionally. If we can't get it, we just skip freeing the oldest
	 * index segment. That's not great, but it's not obvious how we can do
	 * any better.
	 */
	LockBuffer(firstindexbuffer, BUFFER_LOCK_EXCLUSIVE);
	LockBuffer(fsmbuffer, BUFFER_LOCK_EXCLUSIVE);
	if (ConditionalLockBufferForCleanup(metabuffer))
	{
		oldest_remaining_index_segment =
			cb_recycle_index_segment(&RelationGetSmgr(cb->cb_rel)->smgr_rnode.node,
									 cb->cb_fork, metabuffer,
									 firstindexblock, firstindexbuffer,
									 fsmblock, fsmbuffer,
									 oldest_index_segment, needs_xlog);
		LockBuffer(metabuffer, BUFFER_LOCK_UNLOCK);
	}
	UnlockReleaseBuffer(fsmbuffer);
	UnlockReleaseBuffer(firstindexbuffer);

	return oldest_remaining_index_segment;
}

/*
 * Clear all pages in a segment, or alternatively all pages in a segment
 * except for the first one. The segment can be a payload segment that isn't
 * needed any more (in which case we should clear all the pages) or the oldest
 * index segment from which all index entries have been cleared (in which
 * case we should clear all pages but the first).
 *
 * This needs to leave each page in a state where ConveyorBeltPageIsUnused
 * would return true. Otherwise, if this is reused as a payload segment,
 * ConveyorBeltGetNewPage will get confused, as the pages it's trying to
 * allocate will seem to have been concurrently allocated by some other
 * backend.
 *
 * This needs to take a cleanup lock on each page to make sure that there are
 * no lingering locks or pins on the page.
 */
static void
ConveyorBeltClearSegment(ConveyorBelt *cb, CBSegNo segno,
						 bool include_first_page)
{
	BlockNumber	firstblkno;
	BlockNumber	stopblkno;
	BlockNumber	blkno;
	bool		needs_xlog;

	firstblkno = cb_segment_to_block(cb->cb_pages_per_segment, segno, 0);
	if (!include_first_page)
		firstblkno++;
	stopblkno = firstblkno + cb->cb_pages_per_segment;
	needs_xlog = RelationNeedsWAL(cb->cb_rel) || cb->cb_fork == INIT_FORKNUM;

	for (blkno = firstblkno; blkno < stopblkno; ++blkno)
	{
		Buffer	buffer;

		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork, blkno,
									RBM_NORMAL, NULL);
		LockBufferForCleanup(buffer);
		cb_clear_block(&RelationGetSmgr(cb->cb_rel)->smgr_rnode.node,
					   cb->cb_fork, blkno, buffer, needs_xlog);
		UnlockReleaseBuffer(buffer);
	}
}

/*
 * Pin and return the block indicated by 'blkno', extending if needed.
 *
 * On entry, *possibly_not_on_disk_blkno should be the smallest block number
 * not known to exist on disk. If this function discovers that later blocks
 * exist on disk, or extends the relation so that they do, this value will be
 * updated accordingly.
 *
 * If the relation would need to be extended by more than the number of pages
 * in a single segment, an error will occur. This shouldn't happen unless
 * something has gone wrong, because the first page of a segment is supposed to
 * exist on disk before it's allocated. Therefore, if we create segment N+1, at
 * least the first page of segment N should already be there, so we shouldn't
 * be extending by more than one segment. There is a special case when the
 * segments are separated by an FSM page, but there the FSM page should be
 * created on disk before allocating the segment which follows, so the same
 * rule applies.
 */
static Buffer
ConveyorBeltExtend(ConveyorBelt *cb, BlockNumber blkno,
				   BlockNumber *possibly_not_on_disk_blkno)
{
	BlockNumber	nblocks;
	Buffer		buffer;

	/* If the block we need is already known to be on disk, just pin it. */
	if (blkno < *possibly_not_on_disk_blkno)
		return ReadBufferExtended(cb->cb_rel, cb->cb_fork, blkno,
								  RBM_NORMAL, NULL);

	/*
	 * We may need to extend, but can't safely do that if someone else might be
	 * doing so. Since we don't currently have a concept of separate relation
	 * extension locks per fork, we just have to take the only and only
	 * relation-level lock.
	 */
	LockRelationForExtension(cb->cb_rel, ExclusiveLock);

	/* Check relation length, now that we have the extension lock. */
	nblocks = RelationGetNumberOfBlocksInFork(cb->cb_rel, cb->cb_fork);

	/* Complain if possibly_not_on_disk_blkno was a lie. */
	if (nblocks < *possibly_not_on_disk_blkno)
		ereport(ERROR,
				errcode(ERRCODE_DATA_CORRUPTED),
				errmsg_internal("expected at least %u blocks on disk, but found only %u blocks",
								*possibly_not_on_disk_blkno, blkno));

	/*
	 * If the block we need turns out to be on disk after all, we have no need
	 * to extend the relation, and can just read it. We do need to take care to
	 * update *possibly_not_on_disk_blkno to reduce the likelihood of needing
	 * to take the relation extension lock again in the future.
	 */
	if (blkno < nblocks)
	{
		*possibly_not_on_disk_blkno = nblocks;
		UnlockRelationForExtension(cb->cb_rel, ExclusiveLock);
		return ReadBufferExtended(cb->cb_rel, cb->cb_fork, blkno, RBM_NORMAL, NULL);
	}

	/*
	 * Complain if we'd have to extend the relation too far. See also the
	 * function header comments.
	 */
	if (nblocks + cb->cb_pages_per_segment < blkno)
		ereport(ERROR,
				errcode(ERRCODE_DATA_CORRUPTED),
						errmsg_internal("should not need to extend by more than %u blocks to reach block %u, but have only %u blocks",
										cb->cb_pages_per_segment, blkno, nblocks));

	/* Add any blocks that are needed prior to the requested block. */
	while (nblocks < blkno)
	{
		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork, P_NEW,
									RBM_NORMAL, NULL);
		Assert(BufferGetBlockNumber(buffer) == nblocks);
		ReleaseBuffer(buffer);
		++nblocks;
	}

	/* Add the requested block. */
	buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork, P_NEW,
								RBM_NORMAL, NULL);
	Assert(BufferGetBlockNumber(buffer) == blkno);

	/* Done extending relation. */
	UnlockRelationForExtension(cb->cb_rel, ExclusiveLock);

	/* Remember that the relation is now longer than it used to be. */
	*possibly_not_on_disk_blkno = blkno + 1;
	return buffer;
}

/*
 * Figure out where the FSM bit for a given segment number is located.
 *
 * Returns InvalidBlockNumber if the segment's FSM bit is in the metapage,
 * or otherwise the block number of the FSM page that contains that FSM bit.
 */
BlockNumber
ConveyorBeltFSMBlockNumber(ConveyorBelt *cb, CBSegNo segno)
{
	BlockNumber firstblkno;
	unsigned	stride;
	unsigned	whichfsmpage;

	if (segno < CB_FSM_SEGMENTS_FOR_METAPAGE)
		return InvalidBlockNumber;

	firstblkno = cb_first_fsm_block(cb->cb_pages_per_segment);
	stride = cb_fsm_block_spacing(cb->cb_pages_per_segment);
	whichfsmpage = (segno - CB_FSM_SEGMENTS_FOR_METAPAGE)
		/ CB_FSM_SEGMENTS_PER_FSMPAGE;

	return firstblkno + (stride * whichfsmpage);
}

/*
 * Convenience function to read and lock a block.
 */
static Buffer
ConveyorBeltRead(ConveyorBelt *cb, BlockNumber blkno, int mode)
{
	Buffer		buffer;

	Assert(blkno != P_NEW);
	Assert(mode == BUFFER_LOCK_SHARE || mode == BUFFER_LOCK_EXCLUSIVE);
	buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork, blkno,
								RBM_NORMAL, NULL);
	LockBuffer(buffer, mode);
	return buffer;
}

/*
 * We consider a page unused if it's either new (i.e. all zeroes) or if
 * neither pd_lower nor pd_upper have moved.
 */
static Buffer
ConveyorBeltPageIsUnused(Page page)
{
	PageHeader	ph = (PageHeader) page;

	if (PageIsNew(page))
		return true;

	return (ph->pd_lower <= SizeOfPageHeaderData && ph->pd_upper == BLCKSZ);
}

/*
 * Find a free segment by searching all of the FSM pages that currently exist,
 * and if that doesn't turn up anything, adding a new FSM page.
 *
 * Note that this doesn't search the metapage. That's because the caller needs
 * to do that before releasing the content lock on the metapage, whereas this
 * should be called while retaining only a pin on the metapage, since it needs
 * to read and lock other pages.
 *
 * 'next_segment' is the lowest-numbered segment that has not yet been
 * created.
 *
 * If any FSM page covers a segment that is not currently allocated, returns
 * the lowest such segment number. This might be equal to next_segment, but
 * should not be greater.
 *
 * If *fsmbuffer is not InvalidBuffer, it is assumed to be a pinned FSM
 * buffer and will be unpinned.
 *
 * On return, *fsmbuffer will be set to the buffer that contains the FSM page
 * covering the segment whose segment number was returned, and *fsmblock
 * will be set to the corresponding block number.
 */
static CBSegNo
ConveyorSearchFSMPages(ConveyorBelt *cb, CBSegNo next_segment,
					   BlockNumber *fsmblock, Buffer *fsmbuffer)
{
	bool		have_extension_lock = false;
	BlockNumber firstblkno;
	BlockNumber currentblkno;
	BlockNumber stopblkno;
	Buffer		buffer = InvalidBuffer;
	CBSegNo		segno;
	unsigned	stride;

	/*
	 * Release any previous buffer pin.
	 *
	 * We shouldn't ever return without setting *fsmblock and *fsmbuffer to
	 * some legal value, so these stores are just paranoia.
	 */
	if (BufferIsValid(*fsmbuffer))
	{
		ReleaseBuffer(*fsmbuffer);
		*fsmblock = InvalidBlockNumber;
		*fsmbuffer = InvalidBuffer;
	}

	/*
	 * Work out the locations of the FSM blocks.
	 *
	 * stopblkno doesn't need to be perfectly accurate, just good enough that
	 * we search all of the FSM pages that are guaranteed to exist and no more.
	 * If next_segment is greater than zero, we know that the segment prior to
	 * the next segment has to exist, and so any FSM pages which would precede
	 * that must also exist. However, if next_segment is 0, or really any value
	 * less than or equal to CB_FSM_SEGMENTS_FOR_METAPAGE, then there may be
	 * no FSM pages at all.
	 *
	 * NB: When next_segment points to the first segment covered by some FSM
	 * page, that FSM page doesn't have to exist yet. We have to be careful
	 * to assume only that the previous segment exists.
	 */
	firstblkno = cb_first_fsm_block(cb->cb_pages_per_segment);
	if (next_segment <= CB_FSM_SEGMENTS_FOR_METAPAGE)
		stopblkno = 0;
	else
		stopblkno = cb_segment_to_block(cb->cb_pages_per_segment,
										next_segment - 1, 0);
	stride = cb_fsm_block_spacing(cb->cb_pages_per_segment);

	/*
	 * Search the FSM blocks one by one.
	 *
	 * NB: This might be too expensive for large conveyor belts. Perhaps we
	 * should avoid searching blocks that this backend recently searched and
	 * found to be full, or perhaps the on-disk format should contain
	 * information to help us avoid useless searching.
	 */
	for (currentblkno = firstblkno; currentblkno < stopblkno;
		 currentblkno += stride)
	{
		Buffer		buffer;
		CBSegNo		segno;

		CHECK_FOR_INTERRUPTS();

		buffer = ConveyorBeltRead(cb, currentblkno, BUFFER_LOCK_SHARE);
		segno = cbfsmpage_find_free_segment(BufferGetPage(buffer));

		if (segno != CB_INVALID_SEGMENT)
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			Assert(segno <= next_segment);
			*fsmblock = currentblkno;
			*fsmbuffer = buffer;
			return segno;
		}

		UnlockReleaseBuffer(buffer);
	}

	/* Loop should have iterated to completion. */
	Assert(currentblkno >= stopblkno);

	/*
	 * We've searched every FSM page that covers an allocated segment number,
	 * so it's time to think about adding a new FSM page. However, it's
	 * possible that someone else already did that, but didn't actually
	 * allocate a segment. It's also possible that someone extended the
	 * relation with the intention of adding a new FSM page, but didn't manage
	 * to complete the operation. Figure out which it is.
	 */
	while (1)
	{
		bool		needs_init = false;
		BlockNumber blkno;
		BlockNumber nblocks;
		Page		page;

		CHECK_FOR_INTERRUPTS();

		nblocks = RelationGetNumberOfBlocksInFork(cb->cb_rel, cb->cb_fork);

		/* If the relation needs to be physically extended, do so. */
		if (nblocks <= currentblkno)
		{
			/*
			 * We don't currently have a concept of separate relation
			 * extension locks per fork, so for now we just have to take the
			 * only and only relation-level lock.
			 */
			if (!have_extension_lock)
			{
				LockRelationForExtension(cb->cb_rel, ExclusiveLock);
				have_extension_lock = true;

				/*
				 * Somebody else might have extended the relation while we
				 * were waiting for the relation extension lock, so recheck
				 * the length.
				 */
				nblocks =
					RelationGetNumberOfBlocksInFork(cb->cb_rel, cb->cb_fork);
			}

			/*
			 * If the relation would need to be extended by more than one
			 * segment to add this FSM page, something has gone wrong. Nobody
			 * should create a segment without extending the relation far
			 * enough that at least the first page exists physically.
			 */
			if (nblocks <= currentblkno - cb->cb_pages_per_segment)
				ereport(ERROR,
						errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("can't add conveyor belt FSM block at block %u with only %u blocks on disk",
							   currentblkno, currentblkno));

			/*
			 * If the previous segment wasn't fully allocated on disk, add
			 * empty pages to fill it out.
			 */
			while (nblocks <= currentblkno - 1)
			{
				CHECK_FOR_INTERRUPTS();

				buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork,
											P_NEW, RBM_NORMAL, NULL);
				ReleaseBuffer(buffer);
				++nblocks;
			}

			/*
			 * Now the relation should be of the correct length to add the new
			 * FSM page, unless someone already did it while we were waiting
			 * for the extension lock.
			 */
			if (nblocks <= currentblkno)
			{
				buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork,
											P_NEW, RBM_NORMAL, NULL);
				blkno = BufferGetBlockNumber(buffer);
				if (blkno != currentblkno)
					elog(ERROR,
						 "expected new FSM page as block %u but got block %u",
						 currentblkno, blkno);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);
				needs_init = true;
			}
		}

		/*
		 * If we physically extended the relation to make room for the new FSM
		 * page, then we already have a pin and a content lock on the correct
		 * page. Otherwise, we still need to read it, and also check whether
		 * it has been initialized.
		 */
		if (!BufferIsValid(buffer))
		{
			buffer = ConveyorBeltRead(cb, currentblkno, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buffer);

			if (PageIsNew(page))
			{
				/* Appears to need initialization, so get exclusive lock. */
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				/*
				 * It's possible that someone else initialized it after we we
				 * released our share-lock and before we got the exclusive
				 * lock, so retest whether initialization is required.
				 */
				if (PageIsNew(page))
					needs_init = true;
			}
		}

		/*
		 * If we found an FSM page that has already been initialized, we just
		 * need to search it. Often it will have no bits set, because it's
		 * beyond what we thought the last segment was, but if there's
		 * concurrent activity, things might have changed.
		 *
		 * If we found an empty page, or created a new empty page by
		 * physically extending the relation, then we need to initialize it.
		 */
		if (!needs_init)
			segno = cbfsmpage_find_free_segment(page);
		else
		{
			RelFileNode *rnode;
			bool		needs_xlog;

			rnode = &RelationGetSmgr(cb->cb_rel)->smgr_rnode.node;
			needs_xlog = RelationNeedsWAL(cb->cb_rel)
				|| cb->cb_fork == INIT_FORKNUM;
			segno = cb_create_fsmpage(rnode, cb->cb_fork, currentblkno, buffer,
									  cb->cb_pages_per_segment, needs_xlog);
		}

		/* Release our shared or exclusive buffer lock, but keep the pin. */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		/* Hopefully we found a segment and are done. */
		if (segno != CB_INVALID_SEGMENT)
			break;

		/*
		 * Somehow this FSM page, which at last check was beyond the last
		 * allocated segment, now has no bits free whatsoever.  Either we've
		 * been asleep for an extrordinarily long time while a huge amount of
		 * other work has happened, or the data on disk is corrupted, or
		 * there's a bug.
		 */
		elog(DEBUG1,
			 "no free segments in recently-new conveyor belt FSM page at block %u",
			 currentblkno);

		/* Try the next FSM block. */
		ReleaseBuffer(buffer);
		currentblkno += stride;
	}

	/* Finish up. */
	if (have_extension_lock)
		UnlockRelationForExtension(cb->cb_rel, ExclusiveLock);
	Assert(BufferIsValid(buffer));
	*fsmblock = currentblkno;
	*fsmbuffer = buffer;
	return segno;
}
