#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "access/htup_details.h"
#include <stdio.h>
#include <stdlib.h>

PG_MODULE_MAGIC;

/* Declare the original ExecutorStart function pointer */  
static ExecutorStart_hook_type prev_ExecutorStart = NULL;

/* Flags */
static char *pre_cache_flag = NULL;

void _PG_init(void);
void _PG_fini(void);

typedef struct
{
    char *query;
    TupleTableSlot **rows;
    int nrows;
    int ncols;
    int nalloc;
    TupleDesc tupdesc;
} CacheEntry;

//TODO: Eventually these need to be dynamic
#define MAX_CACHE 128
static CacheEntry cache[MAX_CACHE];
static int cache_count = 0;

static MemoryContext CacheContext = NULL;

static DestReceiver *CreateCacheDestReceiver(CacheEntry *entry, _Bool is_capture, DestReceiver *origDest);
typedef struct 
{
    DestReceiver pub;
    CacheEntry *entry;
    bool is_capture;
    int emit_index;
    DestReceiver *origDest;
} CacheDestReceiver;

void myReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
     CacheDestReceiver *rec = (CacheDestReceiver *) self;

    if (rec->is_capture)
    {
        /* Copy the tuple into the cache context */
        MemoryContext old = MemoryContextSwitchTo(CacheContext);
        TupleTableSlot *copy = ExecCopySlot(slot, CacheContext);

        if (rec->entry->nrows >= rec->entry->nalloc)
        {
            rec->entry->nalloc = rec->entry->nalloc ? rec->entry->nalloc * 2 : 16;
            rec->entry->rows = repalloc(rec->entry->rows,
                                        rec->entry->nalloc * sizeof(TupleTableSlot *));
        }

        rec->entry->rows[rec->entry->nrows++] = copy;
        MemoryContextSwitchTo(old);
    }
    else
    {
        /* Emit cached tuple to client */
        if (rec->emit_index < rec->entry->nrows)
        {
            TupleTableSlot *cached_slot = rec->entry->rows[rec->emit_index++];
            TupleTableSlot *out_slot = ExecCopySlot(cached_slot, CurrentMemoryContext);

            /* Send to client via original DestReceiver */
            rec->origDest->receiveSlot(out_slot, rec->origDest);
        }
    }
}

static void
myStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    CacheDestReceiver *rec = (CacheDestReceiver *) self;
    rec->entry->ncols = typeinfo->natts;
}

static void
myShutdownReceiver(DestReceiver *self)
{
    /* no-op */
}

static void
myDestroyReceiver(DestReceiver *self)
{
    pfree(self);
}

/* Create a DestReceiver instance */
static DestReceiver *
CreateCacheDestReceiver(CacheEntry *entry, bool is_capture, DestReceiver *origDest)
{
    CacheDestReceiver *rec = palloc0(sizeof(CacheDestReceiver));
    rec->pub.receiveSlot = myReceiveSlot;
    rec->pub.rStartup = myStartupReceiver;
    rec->pub.rShutdown = myShutdownReceiver;
    rec->pub.rDestroy = myDestroyReceiver;
    rec->entry = entry;
    rec->is_capture = is_capture;
    rec->emit_index = 0;
    rec->origDest = origDest;
    return (DestReceiver *) rec;
}

/* GCU hook to listen for when pg_cache.pre_cache is updated
 * on = pg_cache.pre_cache = TRUE => 'true'
 * off = pg_cache.pre_cache = '' => void
static void pre_cache_flag_hook(const char *newval, void *extra)
{
    if (newval && *newval)
        ereport(LOG, (errmsg("[pg_cache] flag set to '%s'", newval)));
    else
        ereport(LOG, (errmsg("[pg_cache] flag cleared")));
}
 */

/*
void PreCacheQuery(QueryDesc *queryDesc)
{
    ereport(LOG, (errmsg("[pg_cache] pre_cache_flag is enabled %s", queryDesc->sourceText)));
}
*/

void MyExecutorStart(QueryDesc *queryDesc, int eflags)
{
    if (strcmp(pre_cache_flag, "true") == 0) {
        /* Only handle SELECT queries */
        if (queryDesc->operation != CMD_SELECT)
        {
            if (prev_ExecutorStart)
                prev_ExecutorStart(queryDesc, eflags);
            else
                standard_ExecutorStart(queryDesc, eflags);
            return;
        }

        CacheEntry *entry = NULL;

        /* Check if query is already cached */
        for (int i = 0; i < cache_count; i++)
        {
            if (strcmp(cache[i].query, queryDesc->sourceText) == 0)
            {
                entry = &cache[i];
                break;
            }
        }

        if (entry)
        {
            /* Serve cached results */
            queryDesc->dest = CreateCacheDestReceiver(entry, false, queryDesc->dest);
            ereport(LOG, (errmsg("[myext] serving cached results")));
        }
        else
        {
            /* Capture new query */
            if (cache_count < MAX_CACHE)
            {
                entry = &cache[cache_count++];
                entry->query = pstrdup(queryDesc->sourceText);
                entry->rows = NULL;
                entry->nrows = 0;
                entry->nalloc = 0;
                entry->tupdesc = queryDesc->tupDesc;
                queryDesc->dest = CreateCacheDestReceiver(entry, true, queryDesc->dest);
                ereport(LOG, (errmsg("[pg_cache] capturing results for: %s", queryDesc->sourceText)));
            }
        }
    }

    /* Call the original ExecutorStart function */  
    if (prev_ExecutorStart)  
    {  
        prev_ExecutorStart(queryDesc, eflags);  
    }  
    else  
    {  
        standard_ExecutorStart(queryDesc, eflags);  
    }
}

void _PG_init(void)
{
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = MyExecutorStart;

    if (!CacheContext)
        CacheContext = AllocSetContextCreate(TopMemoryContext,
                                             "pg_cache context",
                                             ALLOCSET_DEFAULT_SIZES);

    ereport(LOG, (errmsg("[pg_cache] Loaded")));

    /* Setting up GUC flags */
    DefineCustomStringVariable(
        "pg_cache.pre_cache",
        "This flag is used to prepare a set of queries to be pre_cached",
        NULL,
        &pre_cache_flag,
        "",
        PGC_USERSET,
        0,
        NULL,
        NULL, //pre_cache_flag_hook,   /* <-- called when SET changes it */
        NULL
    );
}

void _PG_fini(void)
{
    ereport(LOG, (errmsg("[pg_cache] Unloaded")));
    ExecutorStart_hook = prev_ExecutorStart;
}
