/*
 * Mid-level operations know about pages, buffers, and xlog; and they
 * can touch multiple kinds of things - e.g. allocating a payload segment
 * touches the segment itself and the index entry that points to it.
 * But, if they are write operations, they should only write one XLOG
 * record, not multiple records. And if they are read operations, they
 * should do only one kind of thing. So for example "look for a segment
 * to allocate" could be a mid-level operation but not "look for a payload
 * segment to allocate, and then try to allocate it if you find one, and if
 * you don't find one then try to allocate a new freespace map segment first
 * and then retry."
 *
 * cb_initialize: Initialize conveyor belt.
 *
 * cb_lock_new_page: Lock next logical page.
 * cb_log_new_page: Like log_newpage, but also bump next-page counter.
 * cb_append_new_page: Just bump next-page counter.
 *
 * cb_allocate_payload_segment: Zero, add index entry, mark used.
 * cb_allocate_index_segment: Initialize, add pointers to it, mark used.
 * cb_allocate_fsm_segment: Initialize, add pointer to it, mark used.
 * cb_deallocate_payload_segment: Remove index entry + mark free.
 * cb_deallocate_index_segment: Change incoming link(s) + mark free.
 * cb_deallocate_fsm_segment: Change incoming link + mark free.
 * cb_find_unused_segment: Check the freespace map for a segment to allocate.
 *
 * cb_relocate_index_entries: Move metapage index entries to index segment.
 * cb_trim_index_entries: Zap some unused index entries.
 *
 * cb_lookup_page: Find the physical position of a logical page, searching
 *    the metapage and index pages as required.
 *
 * cb_truncate: Update oldest logical page.
 *
 * Eventually:
 *    cb_move_payload_segment: Overwrite index entry.
 *    cb_move_index_segment: Overwrite incoming link(s).
 *    cb_move_fsm_segment: Overwrite incoming link.
 * and whatever infrastructure we need for a full rewrite.
 *
 * xlog operations:
 *  0x00 XLOG_CONVEYOR_NEWPAGE
 *  0x10 XLOG_CONVEYOR_ALLOCATE_PAYLOAD
 *  0x20 XLOG_CONVEYOR_ALLOCATE_INDEX
 *  0x30 XLOG_CONVEYOR_ALLOCATE_FSM
 *  0x40 XLOG_CONVEYOR_DEALLOCATE_PAYLOAD
 *  0x50 XLOG_CONVEYOR_DEALLOCATE_INDEX
 *  0x60 XLOG_CONVEYOR_DEALLOCATE_FSM
 *  0x70 XLOG_CONVEYOR_RELOCATE_INDEX_ENTRIES
 *  0x80 XLOG_CONVEYOR_TRIM_INDEX_ENTRIES
 *  0xA0 XLOG_CONVEYOR_MOVE_PAYLOAD_SEGMENT
 *  0xB0 XLOG_CONVEYOR_MOVE_INDEX_SEGMENT
 *  0xC0 XLOG_CONVEYOR_MOVE_FSM_SEGMENT
 */

/* Log the addition of a new logical page to the conveyor belt. */
extern void cb_xlog_newpage(RelFileNode *rnode, ForkNumber fork,
							BlockNumber blkno, Page page, bool page_std,
							Page metapage);

extern void cb_initialize(Relation rel, ForkNumber fork,
						  uint16 pages_per_segment, XLogRecPtr lsn);

if not in recovery
- check that the relation has 0 blocks
- extend it by one page
- write a metapage and xlog the change

if in recovery
- not reached because we would use log_newpage

extern void cb_allocate_payload_segment(Relation rel, ForkNumber fork,
										CBSegNo segno, Buffer metabuffer);
- always: add entry to metapage and MarkBufferDirty
- !recovery: XLogInsert to set lsn
- PageSetLSN
- so the idea is: caller has to give us a locked buffer that is in a known
  good state for the operation we want to perform, and all of the details
  needed to make and log the changes

LOCKING ORDER
  metapage
  index segment or freespace map segment page
  payload page

PSEUDOCODE FOR INSERTING A PAGE

pin metapage
while (!done)
{
	lock metapage
	decide: insert page, relocate entries, or insert index segment
	if action = relocate entries:
		if we've already got the target page pinned, lock it and move stuff
		then action = insert index segment
	if action = insert index segment:
		if we've already got a segment ready to go, do the insert
		then action = insert page
	if action = insert page:
		lock target page, extending relation if required
			(maybe boom if not new)
		done (keep locks on target page + metapage)
	unlock metapage

	if action = relocate entries:
		release any pin on what used to be the last index page
		pin and lock the current last index page
		if it's full, we'll have to add a new one
		otherwise, unlock and loop

	if action = insert index segment:
		identify a possible target segment - conditional cleanup lock 1st page
		if we can't, we'll have to extend the freespace map first
		initialize target segment, keeping a pin on the first page
		loop
}

if we are holding onto a pin on an index page that we didn't end up
using, release it

if we are holding onto a pin on a proposed new index segment that we
didn't up using, release it

MOVING PAGES

we need to copy all of the pages to new locations, and then adjust the
entries that point to that segment. we have trouble if, after we copy
a page, somebody modifies it.

if the client code doesn't modify payload pages after insertion, then
this can't be a problem for anything but the current insertion segment

if we don't allow concurrent moves, then it can't be a problem for
anything but the last index segment page, which could have stuff
added to it - but otherwise entries can't be modified.

it doesn't seem like we have any big problems if moving pages just
requires that the relation fork is read-only. nearly every read and
write operation requires locking the metapage; off-hand, it seems like
the only possible exception is allocating or freeing a freespace map
segment whose used/free status is stored on some other freespace map page.

that case probably needs to be made to modify the metapage, too,
if anyone is going to cache any state.

/*
 * Low-level operations for index pages
 *
 * INDEX PAGES
 * cb_index_page_initialize: initialize index page
 * cb_index_page_get_entry: get N'th entry from page if in range
 * cb_index_page_first_free_entry: get first free entry and
 *    # of free entries
 * cb_index_page_append_entries: add N entries to page starting at position P
 *    (must be first free)
 * cb_index_page_first_used_entry: get first used entry offset and value
 * cb_index_page_clear_entry: clear N'th entry
 * cb_index_page_get_next_segment: get next segment
 * cb_index_Page_set_next_segment: set next segment
 * (All operations should cross-check expected starting logical page.)
 *
 * PAYLOAD PAGES
 * cb_clear_payload_pages: clear pages
 */

