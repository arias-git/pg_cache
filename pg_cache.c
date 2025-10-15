#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include <stdio.h>
#include <stdlib.h>

PG_MODULE_MAGIC;

/* Declare the original ExecutorStart function pointer */  
static ExecutorStart_hook_type prev_ExecutorStart = NULL;

/* Flags */
static char *pre_cache_flag = NULL;

void _PG_init(void);
void _PG_fini(void);
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

void MyExecutorStart(QueryDesc *queryDesc, int eflags)
{
    if (strcmp(pre_cache_flag, "true") == 0) {
        ereport(LOG, (errmsg("[pg_cache] pre_cache_flag is enabled")));
    }

    if (queryDesc && queryDesc->sourceText)
        ereport(LOG, (errmsg("[pg_cache] Query: %s", queryDesc->sourceText)));
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
