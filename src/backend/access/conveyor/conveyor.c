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
static Buffer ConveyorBeltRead(ConveyorBelt *cb, BlockNumber blkno, int mode);

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
 * buffer = ConveyorBeltPrepareInsert(cb, &pageno);
 * page = BufferGetPage(buffer);
 * START_CRIT_SECTION();
 * // set page contents
 * ConveyorBeltPerformInsert(cb, buffer, page_std);
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
 * In future, we might want to provide the caller with an alternative to
 * calling ConveyorBeltPerformInsert, because that just logs an FPI for
 * the new page, and some callers might prefer to manage their own xlog
 * needs.
 */
Buffer
ConveyorBeltGetNewPage(ConveyorBelt *cb, CBPageNo *pageno)
{
	BlockNumber fsmblock;
	Buffer		metabuffer;
	Buffer		fsmbuffer = InvalidBuffer;
	Buffer		buffer;
	CBPageNo	next_pageno;
	CBSegNo		free_segno = CB_INVALID_SEGMENT;
	CBSegNo		possibly_not_on_disk_segno = 0;
	bool		needs_xlog;
	int			mode = BUFFER_LOCK_SHARE;

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
		 * after all other buffer locks. If any of fsmbuffer, idxbuffer, and
		 * newidxbuffer are valid, they are also exclusively locked at this
		 * point.
		 */
		LockBuffer(metabuffer, mode);
		meta = cb_metapage_get_special(BufferGetPage(metabuffer));
		insert_state = cb_metapage_get_insert_state(meta, &next_blkno,
													&next_pageno, &next_segno);

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
		 * We cannot allocate a segment unless at least the first page of that
		 * segment is guaranteed to be on disk. This is certain to be true for
		 * any segment that's been allocated previously, but otherwise it's
		 * only true if we've verified that the size of the relation on disk
		 * is large enough.
		 */
		can_allocate_segment = (mode == BUFFER_LOCK_EXCLUSIVE)
			&& (free_segno != CB_INVALID_SEGMENT)
			&& (free_segno < next_segno ||
				free_segno < possibly_not_on_disk_segno);

		/*
		 * If the metapage says that we need a payload segment, and on a
		 * previous trip through this loop we identified a candidate segment,
		 * then see if we can allocate it.
		 */
		if (insert_state == CBM_INSERT_NEEDS_PAYLOAD_SEGMENT &&
			can_allocate_segment)
		{
			bool		segment_still_free;

			Assert(mode == BUFFER_LOCK_EXCLUSIVE);

			/* fsmbuffer, if valid, is already exclusively locked. */
			if (BufferIsValid(fsmbuffer))
				segment_still_free =
					cb_fsmpage_get_fsm_bit(BufferGetPage(fsmbuffer),
										   free_segno);
			else
				segment_still_free = cb_metapage_get_fsm_bit(meta, free_segno);

			/*
			 * If the target segment is still free, we can go ahead and
			 * allocate it now. After that, we know there is a non-full
			 * payload segment and can plan to try to grab the first page.
			 */
			if (segment_still_free)
			{
				cb_allocate_payload_segment(cb->cb_insert_relfilenode,
											cb->cb_fork, metabuffer,
											fsmblock, fsmbuffer, free_segno,
											free_segno >= next_segno,
											needs_xlog);
				insert_state = CBM_INSERT_OK;
				next_blkno = cb_segment_to_block(cb->cb_pages_per_segment,
												 free_segno, 0);
			}

			/*
			 * Whether we allocated or not, the segment we intended to
			 * allocate is no longer free.
			 */
			free_segno = CB_INVALID_SEGMENT;
		}

		/* Release buffer locks and, except for the metapage, also pins. */
		LockBuffer(metabuffer, BUFFER_LOCK_UNLOCK);
		if (BufferIsValid(fsmbuffer))
		{
			UnlockReleaseBuffer(fsmbuffer);
			fsmblock = InvalidBlockNumber;
			fsmbuffer = InvalidBuffer;
		}

		/*
		 * If, according to the metapage, there is a current payload segment
		 * that is not full, we can just lock the target buffer. If it's still
		 * new, we're done. Otherwise, someone else grabbed that page before
		 * we did, so we must retry.
		 */
		if (insert_state == CBM_INSERT_OK)
		{
			buffer = ConveyorBeltRead(cb, next_blkno, BUFFER_LOCK_SHARE);
			if (PageIsNew(BufferGetPage(buffer)))
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
		}
		else
		{
			/*
			 * Some sort of preparatory work will be needed in order to insert
			 * a new page, which will require modifying the metapage.
			 * Therefore, next time we lock it, we had better grab an
			 * exclusive lock.
			 */
			mode = BUFFER_LOCK_EXCLUSIVE;
		}

		/*
		 * If the metapage has no more space for index entries, we must move
		 * some of the existing entries to an index segment.
		 */
		if (insert_state == CBM_INSERT_NEEDS_INDEX_ENTRIES_RELOCATED)
		{
			elog(ERROR, "XXX relocating index entries is not implemented yet");
		}

		/* Do we need a new segment? */
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

			/*
			 * At least the first page of a segment must exist on disk before
			 * we're allowed to allocate it, so we may need to extend the
			 * relation.
			 */
			if (free_segno > next_segno)
			{
				/*
				 * If the FSM thinks that we ought to allocate a new segment,
				 * it should be the very next one. If not, something has gone
				 * horribly wrong - somehow we must have thought that a
				 * segment that has never been allocated isn't actually free!
				 */
				ereport(ERROR,
						errcode(ERRCODE_DATA_CORRUPTED),
						errmsg_internal("free segment is %u but next segment is only %u",
										free_segno, next_segno));

			}
			else if (free_segno == next_segno)
			{
				BlockNumber nblocks;
				BlockNumber free_block;

				/* Allocating the next segment so might need to extend. */
				nblocks = RelationGetNumberOfBlocksInFork(cb->cb_rel,
														  cb->cb_fork);
				free_block = cb_segment_to_block(cb->cb_pages_per_segment,
												 free_segno, 0);
				if (nblocks <= free_block)
				{
					/*
					 * We need to make sure that the first page of the new
					 * segment exists on disk before we allocate it, but the
					 * first page of the prior segment should already exist on
					 * disk, because the last allocation had to follow the
					 * same rule. Therefore we shouldn't need to extend by
					 * more than one segment.
					 */
					if (nblocks + cb->cb_pages_per_segment < free_block)
						ereport(ERROR,
								errcode(ERRCODE_DATA_CORRUPTED),
								errmsg_internal("free segment %u starts at block %u but relation has only %u blocks",
												free_segno, free_block, nblocks));


					/* Add empty blocks as required. */
					while (nblocks <= free_block)
					{
						CHECK_FOR_INTERRUPTS();

						buffer = ReadBufferExtended(cb->cb_rel, cb->cb_fork,
													P_NEW, RBM_NORMAL, NULL);
						ReleaseBuffer(buffer);
						++nblocks;
					}
				}

				/*
				 * The first page of this segment, at least, is now known to
				 * be on disk, either because we verified the length of the
				 * relation fork, or because we extended it.
				 */
				possibly_not_on_disk_segno = free_segno + 1;
			}
		}

		if (insert_state == CBM_INSERT_NEEDS_INDEX_SEGMENT)
		{
			elog(ERROR, "XXX creating index segments is not implemented yet");
		}

		/*
		 * Prepare for next attempt by reacquiring all relevant buffer locks,
		 * except for the one on the metapage, which is acquired at the top of
		 * the loop.
		 */
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
ConveyorBeltPerformInsert(ConveyorBelt *cb, Buffer buffer, bool page_std)
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
						   needs_xlog, page_std);

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

	/* Work out the locations of the FSM blocks. */
	firstblkno = cb_first_fsm_block(cb->cb_pages_per_segment);
	stopblkno = cb_segment_to_block(cb->cb_pages_per_segment, next_segment, 0);
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
		if (nblocks < currentblkno)
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
