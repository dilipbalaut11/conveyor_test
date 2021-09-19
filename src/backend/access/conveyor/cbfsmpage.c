/*-------------------------------------------------------------------------
 *
 * cbfsmpage.c
 *	  APIs for accessing conveyor belt FSM pages.
 *
 * Similar to cbmetapage.c, this file abstracts accesses to conveyor
 * belt FSM pages, and should be the only backend code that understands
 * their internal structure.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/backend/access/conveyor/cbfsmpage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/cbfsmpage.h"
#include "access/cbfsmpage_format.h"
#include "access/cbmetapage.h"

static CBFSMPageData *cb_fsmpage_get_special(Page page);

/*
 * Initialize FSM page.
 *
 * Returns the first segment number that will be covered by the new page.
 */
CBSegNo
cb_fsmpage_initialize(Page page, BlockNumber blkno, uint16 pages_per_segment)
{
	CBFSMPageData *fsmp;
	BlockNumber	first_fsm_block = cb_first_fsm_block(pages_per_segment);
	unsigned	fsm_block_spacing = cb_fsm_block_spacing(pages_per_segment);

	/* Sanity checks. */
	Assert(blkno >= first_fsm_block);
	Assert((blkno - first_fsm_block) % fsm_block_spacing == 0);

	/* Initialize page. PageInit will zero the payload bits for us. */
	PageInit(page, BLCKSZ, sizeof(CBFSMPageData));
	fsmp = (CBFSMPageData *) PageGetSpecialPointer(page);
	fsmp->cbfsm_magic = CB_FSMPAGE_MAGIC;
	fsmp->cbfsm_start = cb_first_segment_for_fsm_page(blkno, pages_per_segment);

	return fsmp->cbfsm_start;
}

/*
 * Get the allocation status of a segment from an FSM page.
 */
bool
cb_fsmpage_get_fsm_bit(Page page, CBSegNo segno)
{
	CBFSMPageData *fsmp = cb_fsmpage_get_special(page);
	uint8		byte;
	uint8		mask;
	uint32		bitno;

	if (segno < fsmp->cbfsm_start ||
		segno >= fsmp->cbfsm_start + CB_FSM_SEGMENTS_PER_FSMPAGE)
		elog(ERROR, "segment %u out of range for fsm page starting at segment %u",
			 segno, fsmp->cbfsm_start);

	bitno = segno - fsmp->cbfsm_start;
	byte = fsmp->cbfsm_state[bitno / BITS_PER_BYTE];
	mask = 1 << (bitno % BITS_PER_BYTE);
	return (segno & mask) != 0;
}

/*
 * Set the allocation status of a segment in an FSM page.
 *
 * new_state should be true if the bit is currently clear and should be set,
 * and false if the bit is currently set and should be cleared. Don't call
 * this unless you know that the bit actually needs to be changed.
 */
void
cb_fsmpage_set_fsm_bit(Page page, CBSegNo segno, bool new_state)
{
	CBFSMPageData *fsmp = cb_fsmpage_get_special(page);
	uint8	   *byte;
	uint8		mask;
	uint8		old_state;
	uint32		bitno;

	if (segno < fsmp->cbfsm_start ||
		segno >= fsmp->cbfsm_start + CB_FSM_SEGMENTS_PER_FSMPAGE)
		elog(ERROR, "segment %u out of range for fsm page starting at segment %u",
			 segno, fsmp->cbfsm_start);

	bitno = segno - fsmp->cbfsm_start;
	byte = &fsmp->cbfsm_state[bitno / BITS_PER_BYTE];
	mask = 1 << (segno % BITS_PER_BYTE);
	old_state = (*byte & mask) != 0;

	if (old_state == new_state)
		elog(ERROR, "fsm bit for segment %u already has value %d",
			 segno, old_state ? 1 : 0);

	if (new_state)
		*byte |= mask;
	else
		*byte &= ~mask;
}

/*
 * Returns the lowest unused segment number covered by the supplied FSM page,
 * or CB_INVALID_SEGMENT if none.
 */
CBSegNo
cbfsmpage_find_free_segment(Page page)
{
	CBFSMPageData *fsmp = cb_fsmpage_get_special(page);
	unsigned	i;

	StaticAssertStmt(CB_FSMPAGE_FREESPACE_BYTES % sizeof(uint64) == 0,
					 "CB_FSMPAGE_FREESPACE_BYTES should be a multiple of 8");

	for (i = 0; i < CB_FSMPAGE_FREESPACE_BYTES; i += sizeof(uint64))
	{
		uint64	word = * (uint64 *) &fsmp->cbfsm_state[i];

		if (word != PG_UINT64_MAX)
		{
			uint64		flipword = ~word;
			int			b = fls((int) flipword);

			if (b == 0)
				b = 32 + fls((int) (flipword >> 32));

			Assert(b >= 1 && b <= 64);
			return fsmp->cbfsm_start + (i * BITS_PER_BYTE) + (b - 1);
		}
	}

	return CB_INVALID_SEGMENT;
}

/*
 * Given a page that is known to be a conveyor belt free space map page,
 * return a pointer to the CBFSMPageData, after checking the magic number.
 */
static CBFSMPageData *
cb_fsmpage_get_special(Page page)
{
	CBFSMPageData *fsmp = (CBFSMPageData *) PageGetSpecialPointer(page);

	if (fsmp->cbfsm_magic != CB_FSMPAGE_MAGIC)
		elog(ERROR, "bad magic number in conveyor belt fsm page: %08X",
			 fsmp->cbfsm_magic);

	return fsmp;
}
