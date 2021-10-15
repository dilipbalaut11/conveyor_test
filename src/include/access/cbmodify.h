/*-------------------------------------------------------------------------
 *
 * cbmodify.h
 *	  Routines to make a change to a conveyor belt and XLOG it if needed.
 *
 * All of these routines assume that the required buffers have been
 * correctly identified by the called, and that all necessary pins and
 * locks have been acquired, and that the caller has verified that the
 * page is in the correct starting state for the proposed modification.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage. See src/backend/access/conveyor/cbxlog.c for
 * the corresponding REDO routines.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbmodify.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBMODIFY_H
#define CBMODIFY_H

#include "access/cbdefs.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/relfilenode.h"

extern void cb_create_metapage(RelFileNode *rnode,
							   ForkNumber fork,
							   Buffer metabuffer,
							   uint16 pages_per_segment,
							   bool needs_xlog);

extern CBSegNo cb_create_fsmpage(RelFileNode *rnode,
								 ForkNumber fork,
								 BlockNumber blkno,
								 Buffer buffer,
								 uint16 pages_per_segment,
								 bool needs_xlog);

extern void cb_insert_payload_page(RelFileNode *rnode,
								   ForkNumber fork,
								   Buffer metabuffer,
								   BlockNumber payloadblock,
								   Buffer payloadbuffer,
								   bool needs_xlog,
								   bool page_std);

extern void cb_allocate_payload_segment(RelFileNode *rnode,
										ForkNumber fork,
										Buffer metabuffer,
										BlockNumber fsmblock,
										Buffer fsmbuffer,
										CBSegNo segno,
										bool is_extend,
										bool needs_xlog);

extern void cb_allocate_index_segment(RelFileNode *rnode,
									  ForkNumber fork,
									  Buffer metabuffer,
									  BlockNumber indexblock,
									  Buffer indexbuffer,
									  BlockNumber prevblock,
									  Buffer prevbuffer,
									  BlockNumber fsmblock,
									  Buffer fsmbuffer,
									  CBSegNo segno,
									  CBPageNo pageno,
									  bool is_extend,
									  bool needs_xlog);

extern void cb_allocate_index_page(RelFileNode *rnode,
								   ForkNumber fork,
								   BlockNumber indexblock,
								   Buffer indexbuffer,
								   CBPageNo pageno,
								   bool needs_xlog);

extern void cb_relocate_index_entries(RelFileNode *rnode,
									  ForkNumber fork,
									  Buffer metabuffer,
									  BlockNumber indexblock,
									  Buffer indexbuffer,
									  unsigned pageoffset,
									  unsigned num_index_entries,
									  CBSegNo *index_entries,
									  CBPageNo index_page_start,
									  bool needs_xlog);

extern void cb_logical_truncate(RelFileNode *rnode,
								ForkNumber fork,
								Buffer metabuffer,
								CBPageNo oldest_keeper,
								bool needs_xlog);

#endif							/* CBMODIFY_H */
