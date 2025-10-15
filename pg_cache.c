#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include <stdio.h>
#include <stdlib.h>

PG_MODULE_MAGIC;
void _PG_init(void);

void _PG_init(void)
{
    ereport(WARNING,
            (errmsg("WARNING GINGINGINGINGINGIGNIGNINGINGINGINGIGNGINGIN"),
            errdetail("it worked"),
            errhint("this is a hint")));
}
