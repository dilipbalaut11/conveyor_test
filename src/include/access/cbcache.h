/*-------------------------------------------------------------------------
 *
 * cbcache.h
 *	  Conveyor belt index segment location cache.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBCACHE_H
#define CBCACHE_H

#include "access/cbdefs.h"

struct CBCache;
typedef struct CBCache CBCache;

extern CBCache *cb_cache_create(MemoryContext mcxt,
								uint64 index_segments_moved);
extern void cb_cache_invalidate(CBCache *cache, CBPageNo index_start,
								uint64 index_segments_moved);
extern CBSegNo cb_cache_lookup(CBCache *cache, CBPageNo pageno);
extern CBSegNo cb_cache_fuzzy_lookup(CBCache *cache, CBPageNo pageno,
							   CBPageNo *index_segment_start);
extern void cb_cache_insert(CBCache *cache, CBSegNo segno,
							CBPageNo index_segment_start);

#endif							/* CBCACHE_H */
