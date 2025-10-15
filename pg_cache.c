#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include <stdio.h>
#include <stdlib.h>

PG_MODULE_MAGIC;

/* Declare the original ExecutorStart function pointer */  
static ExecutorStart_hook_type prev_ExecutorStart = NULL;


void _PG_init(void);
void _PG_fini(void);

void MyExecutorStart(QueryDesc *queryDesc, int eflags)
{
    ereport(LOG, (errmsg("[pg_cache] query was run")));

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
}

void _PG_fini(void)
{
    ereport(LOG, (errmsg("[pg_cache] Unloaded")));
    ExecutorStart_hook = prev_ExecutorStart;
}
