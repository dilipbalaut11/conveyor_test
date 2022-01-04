/*-------------------------------------------------------------------------
 *
 * cbmetapage_format.h
 *	  Actual on-disk format for a conveyor-belt FSM page.
 *
 * Backend code should not typically include this file directly, even if
 * it's code that is part of the conveyor belt implementation. Instead, it
 * should use the interface routines defined in cbfsmpage.h.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbfsmpage_format.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBFSMPAGE_FORMAT_H
#define CBFSMPAGE_FORMAT_H

#include "access/cbmetapage.h"

/* Magic number for the FSM page. */
#define	CB_FSMPAGE_MAGIC	0x30263162

/*
 * A conveyor belt FSM page will store a struct of this type in the page's
 * special space.
 */
typedef struct CBFSMPageData
{
	uint32		cbfsm_magic;	/* always CB_FSMPAGE_MAGIC */
	CBSegNo		cbfsm_start;	/* first segment this page describes */
	uint8		cbfsm_state[CB_FSMPAGE_FREESPACE_BYTES];
} CBFSMPageData;

#endif
