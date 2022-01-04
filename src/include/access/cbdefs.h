/*-------------------------------------------------------------------------
 *
 * cbdefs.h
 *	  Commonly-used conveyor-belt definitions.
 *
 * It's a little annoying to have a separate header file for just these
 * few definitions - but the alternatives all seem worse.
 *
 * See src/backend/access/conveyor/README for a general overview of
 * conveyor belt storage.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * src/include/access/cbdefs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CBDEFS_H
#define CBDEFS_H

/* Logical page numbers are unsigned, 64-bit integers. */
typedef uint64 CBPageNo;
#define	CB_INVALID_LOGICAL_PAGE ((CBPageNo) -1)

/* Segment numbers are unsigned, 32-bit integers. */
typedef uint32 CBSegNo;
#define CB_INVALID_SEGMENT		((CBSegNo) -1)

#endif							/* CBDEFS_H */
