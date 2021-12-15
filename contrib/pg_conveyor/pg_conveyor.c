/*-------------------------------------------------------------------------
 *
 * pg_conveyor.c
 *
 * provide APIs over the conveyor belt infrastructure to create, insert and
 * fetch the data from the conveyor belt.
 *
 * Copyright (c) 2016-2021, PostgreSQL Global Development Group
 *
 * contrib/pg_conveyor/pg_conveyor.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/conveyor.h"
#include "access/relation.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_conveyor_init);
PG_FUNCTION_INFO_V1(pg_conveyor_insert);
PG_FUNCTION_INFO_V1(pg_conveyor_read);
PG_FUNCTION_INFO_V1(pg_conveyor_truncate);
PG_FUNCTION_INFO_V1(pg_conveyor_vacuum);

static ConveyorBelt*
OpenConveyorBeltForRel(Relation rel)
{
	SMgrRelation	reln;

	/* Open the relation at smgr level. */
	reln = RelationGetSmgr(rel);

	if (!smgrexists(reln, DEADTID_FORKNUM))
		elog(ERROR, "conveyor belt not initialized for relid %u", RelationGetRelid(rel));

	/* Open the conveyor belt. */
	return ConveyorBeltOpen(rel, DEADTID_FORKNUM, CurrentMemoryContext);
}

/*
 * Initialize a new conveyor belt for input relid.
 */
Datum
pg_conveyor_init(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	int	block_per_seg = PG_GETARG_INT32(1);
	SMgrRelation	reln;
	Relation		rel;

	rel = relation_open(relid, AccessShareLock);

	/* Open the relation at smgr level. */
	reln = RelationGetSmgr(rel);

	/*
	 * If the dead_tid fork doesn't exist then create it and initialize the
	 * conveyor belt, otherwise just open the conveyor belt.
	 */
	if (!smgrexists(reln, DEADTID_FORKNUM))
	{
		smgrcreate(reln, DEADTID_FORKNUM, false);
		ConveyorBeltInitialize(rel, DEADTID_FORKNUM, block_per_seg,
							   CurrentMemoryContext);
	}

	relation_close(rel, AccessShareLock);

	/* Nothing to return. */
	PG_RETURN_VOID();
}

/*
 * Insert input buffer data into the conveyor belt.
 */
Datum
pg_conveyor_insert(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	char   *data = text_to_cstring(PG_GETARG_TEXT_PP(1));
	Relation	rel;
	ConveyorBelt   *cb;
	CBPageNo		pageno;
	Buffer			buffer;
	PageHeader		phdr;
	Page			page;
	char		   *pagedata;
	int				len = strlen(data);

	rel = relation_open(relid, AccessExclusiveLock);

	cb = OpenConveyorBeltForRel(rel);

	buffer = ConveyorBeltGetNewPage(cb, &pageno);
	page = BufferGetPage(buffer);
	pagedata = PageGetContents(page);
	PageInit(page, BLCKSZ, 0);

	if (len > (BLCKSZ) - MAXALIGN(SizeOfPageHeaderData))
		elog(ERROR, "data too large");

	phdr = (PageHeader) page;

	START_CRIT_SECTION();
	memcpy(pagedata, data, strlen(data));
	phdr->pd_lower += strlen(data);
	ConveyorBeltPerformInsert(cb, buffer);
	END_CRIT_SECTION();

	ConveyorBeltCleanupInsert(cb, buffer);

	relation_close(rel, AccessExclusiveLock);

	/* Nothing to return. */
	PG_RETURN_VOID();
}

/*
 * Read data from the conveyor belt's logical page .
 */
Datum
pg_conveyor_read(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	CBPageNo		pageno = PG_GETARG_INT64(1);
	CBPageNo		oldest_page;
	CBPageNo		next_page;
	Relation		rel;
	ConveyorBelt   *cb;
	Buffer			buffer;
	char			pagedata[BLCKSZ];

	rel = relation_open(relid, AccessShareLock);

	cb = OpenConveyorBeltForRel(rel);

	ConveyorBeltGetBounds(cb, &oldest_page, &next_page);
	if (pageno < oldest_page || pageno >= next_page)
		elog(ERROR, "conveyor belt pageno is out of bound");

	buffer = ConveyorBeltReadBuffer(cb, pageno, BUFFER_LOCK_SHARE, NULL);
	if (BufferIsInvalid(buffer))
		elog(ERROR, "could not read data");

	memcpy(pagedata, BufferGetPage(buffer), BLCKSZ);
	UnlockReleaseBuffer(buffer);

	relation_close(rel, AccessShareLock);

	PG_RETURN_DATUM(CStringGetTextDatum((char *) PageGetContents((char *) pagedata)));
}

/*
 * Truncate the conveyor belt wrapper.
 */
Datum
pg_conveyor_truncate(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	CBPageNo		pageno = PG_GETARG_INT64(1);
	CBPageNo		oldest_page;
	CBPageNo		next_page;
	Relation		rel;
	ConveyorBelt   *cb;

	rel = relation_open(relid, AccessExclusiveLock);

	cb = OpenConveyorBeltForRel(rel);

	ConveyorBeltGetBounds(cb, &oldest_page, &next_page);
	if (pageno < oldest_page || pageno >= next_page)
		elog(ERROR, "conveyor belt pageno is out of bound");

	ConveyorBeltLogicalTruncate(cb, pageno);
	relation_close(rel, AccessExclusiveLock);

	/* Nothing to return. */
	PG_RETURN_VOID();
}

/*
 * Vacuum conveyor belt wrapper.
 */
Datum
pg_conveyor_vacuum(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	Relation		rel;
	ConveyorBelt   *cb;

	rel = relation_open(relid, AccessExclusiveLock);

	cb = OpenConveyorBeltForRel(rel);

	ConveyorBeltVacuum(cb);
	relation_close(rel, AccessExclusiveLock);

	/* Nothing to return. */
	PG_RETURN_VOID();
}
