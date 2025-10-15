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
} CacheEntry;

//TODO: Eventually these need to be dynamic
#define MAX_CACHE 128
static CacheEntry cache[MAX_CACHE];
static int cache_count = 0;

static DestReceiver *CreateCacheDestReceiver(CacheEntry *entry, _Bool is_capture);
typedef struct 
{
    DestReceiver pub;
    CacheEntry *entry;
    bool is_capture;
} CacheDestReceiver;

void myReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
    CacheDestReceiver *rec = (CacheDestReceiver *) self;

    if (!slot || TupIsNull(slot))
        return;

    if (rec->is_capture)
    {
        // Can limit the size of a cached item
        if (rec->entry->nrows >= 1024)
            return;
        TupleTableSlot **arr = rec->entry->rows;
        arr[rec->entry->nrows] = slot;
        rec->entry->nrows++;
    }
    else
    {
        standard_ExecutorRun(NULL, ForwardScanDirection, 1, false);
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
CreateCacheDestReceiver(CacheEntry *entry, bool is_capture)
{
    CacheDestReceiver *rec = palloc0(sizeof(CacheDestReceiver));
    rec->pub.receiveSlot = myReceiveSlot;
    rec->pub.rStartup = myStartupReceiver;
    rec->pub.rShutdown = myShutdownReceiver;
    rec->pub.rDestroy = myDestroyReceiver;
    rec->entry = entry;
    rec->is_capture = is_capture;
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
            queryDesc->dest = CreateCacheDestReceiver(entry, false);
            ereport(LOG, (errmsg("[myext] serving cached results")));
        }
        else
        {
            /* Capture results */
            if (cache_count < MAX_CACHE)
            {
                entry = &cache[cache_count++];
                entry->query = pstrdup(queryDesc->sourceText);
                entry->rows = palloc0(sizeof(TupleTableSlot*) * 1024);
                entry->nrows = 0;
                queryDesc->dest = CreateCacheDestReceiver(entry, true);
                ereport(LOG, (errmsg("[myext] capturing results for new query")));
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
