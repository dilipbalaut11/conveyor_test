/*-------------------------------------------------------------------------
 *
 * cbcache.c
 *	  Conveyor belt index segment location cache.
 *
 * The conveyor belt metapage stores the segment numbers of the oldest and
 * newest index segments that currently exist, but the location of any
 * other index segment can only be discovered by reading the first page
 * of some index segment whose position is known and extracting from it
 * the segment number of the next index segment. That's potentially
 * expensive, especially if we have to traverse a whole bunch of index
 * segments to figure out the location of the one we need, so we maintain
 * a cache.
 *
 * The cache maps the oldest logical page number covered by an index
 * segment to the segment number where that index segment is located.
 * If older index segments are removed, the corresponding mappings become
 * obsolete, but nobody should be accessing those pages anyway. Still,
 * we're careful to purge such mappings to avoid wasting memory.
 *
 * If an index segment is moved, we invalidate the entire cache. This
 * is expected to be fairly rare, as it should only happen if someone is
 * trying to reduce the on-disk footprint of the conveyor belt. Moreover,
 * if someone is doing that, it is likely that multiple index segments
 * will be moved in relatively quick succession, so it's not clear that
 * a more granular invalidation strategy would help anything.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/backend/access/conveyor/cbcache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/cbcache.h"
#include "common/hashfn.h"

typedef struct cb_iseg_entry
{
	CBPageNo	index_segment_start;
	CBSegNo		segno;
	char		status;
} cb_iseg_entry;

#define SH_PREFIX cb_iseg
#define SH_ELEMENT_TYPE cb_iseg_entry
#define SH_KEY_TYPE CBPageNo
#define SH_KEY index_segment_start
#define SH_HASH_KEY(tb, key) tag_hash(&key, sizeof(CBPageNo))
#define SH_EQUAL(tb, a, b) a == b
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

struct CBCache
{
	uint64		index_segments_moved;
	CBPageNo	oldest_possible_start;
	cb_iseg_hash *iseg;
};

/*
 * Create a new cache.
 */
CBCache *
cb_cache_create(MemoryContext mcxt, uint64 index_segments_moved)
{
	CBCache    *cache = MemoryContextAlloc(mcxt, sizeof(CBCache));

	cache->index_segments_moved = index_segments_moved;
	cache->oldest_possible_start = 0;
	cache->iseg = cb_iseg_create(mcxt, 128, NULL);

	return cache;
}

/*
 * Invalidate cache entries as required.
 *
 * If index_segments_moved has changed, throw away everything we think we
 * know. Otherwise, if index_start has advanced, throw away any entries that
 * precede the new start.
 */
void
cb_cache_invalidate(CBCache *cache, CBPageNo index_start,
					uint64 index_segments_moved)
{
	if (index_segments_moved != cache->index_segments_moved)
	{
		cb_iseg_reset(cache->iseg);
		cache->index_segments_moved = index_segments_moved;
	}
	else if (index_start > cache->oldest_possible_start)
	{
		cb_iseg_iterator it;
		cb_iseg_entry *entry;

		cb_iseg_start_iterate(cache->iseg, &it);
		while ((entry = cb_iseg_iterate(cache->iseg, &it)) != NULL)
			if (entry->index_segment_start < index_start)
				cb_iseg_delete_item(cache->iseg, entry);
	}
}

/*
 * Search the cache for an index segment number, given the first logical page
 * number covered by that index segment.
 *
 * It is the caller's responsibility to make sure that pageno is the first
 * logical page number covered by some index segment, rather than any random
 * page number whose index entry might be anywhere in the segment. We don't
 * have enough information here to verify this, and just trust that the caller
 * knows what they are doing.
 */
CBSegNo
cb_cache_lookup(CBCache *cache, CBPageNo pageno)
{
	cb_iseg_entry *entry;

	entry = cb_iseg_lookup(cache->iseg, pageno);
	return entry != NULL ? entry->segno : CB_INVALID_SEGMENT;
}

/*
 * Search the cache for an index segment that precedes the one for which we
 * are searching by as little as possible.
 *
 * As with cb_cache_lookup, pageno should be the first logical page of the
 * index segment in which the caller is interested, although unlike that
 * function, this function would still work correctly if it were an arbitrary
 * page number, at least as presently implemented.
 *
 * If no segment with a starting segment number preceding pageno is found
 * in cache, the return value is CB_INVALID_SEGMENT and *index_segment_start
 * is set to CB_INVALID_LOGICAL_PAGE. Otherwise, the return value is the
 * segment number we found and *index_segment_start is set to the starting
 * logical page number of that segment.
 */
CBSegNo
cb_cache_fuzzy_lookup(CBCache *cache, CBPageNo pageno,
					  CBPageNo *index_segment_start)
{
	cb_iseg_iterator it;
	cb_iseg_entry *current;
	cb_iseg_entry *best = NULL;

	cb_iseg_start_iterate(cache->iseg, &it);
	while ((current = cb_iseg_iterate(cache->iseg, &it)) != NULL)
	{
		if (current->index_segment_start > pageno)
			continue;
		if (best == NULL ||
			best->index_segment_start < current->index_segment_start)
			best = current;
	}

	if (best == NULL)
	{
		*index_segment_start = CB_INVALID_LOGICAL_PAGE;
		return CB_INVALID_SEGMENT;
	}

	*index_segment_start = best->index_segment_start;
	return best->segno;
}

/*
 * Insert a cache entry.
 *
 * As in cb_cache_lookup, it's critical that index_segment_start is the first
 * logical page number covered by the index segment.
 */
void
cb_cache_insert(CBCache *cache, CBSegNo segno, CBPageNo index_segment_start)
{
	cb_iseg_entry *entry;
	bool		found;

	entry = cb_iseg_insert(cache->iseg, index_segment_start, &found);
	Assert(!found);
	entry->segno = segno;

	Assert(index_segment_start >= cache->oldest_possible_start);
}
