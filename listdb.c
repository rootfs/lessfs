/*
 *   Lessfs: A data deduplicating filesystem.
 *   Copyright (C) 2008 Mark Ruijter <mruijter@lessfs.com>
 *
 *   This program is free software.
 *   You can redistribute lessfs and/or modify it under the terms of either
 *   (1) the GNU General Public License; either version 3 of the License,
 *   or (at your option) any later version as published by
 *   the Free Software Foundation; or (2) obtain a commercial license
 *   by contacting the Author.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY;  without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See
 *   the GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program;  if not, write to the Free Software
 *   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <fuse.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <tcutil.h>
#include <tchdb.h>
#include <tcbdb.h>
#include <stdbool.h>
#include "lib_safe.h"
#include "lib_cfg.h"
#include "lib_str.h"
#include "lib_repl.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_common.h"
#ifndef BERKELEYDB
#include "lib_tc.h"
#endif
#include "file_io.h"
#include "commons.h"
#ifdef BERKELEYDB
#include <db.h>
#include "lib_bdb.h"
#endif

#ifdef i386
#define ITERATIONS 30
#else
#define ITERATIONS 500
#endif

u_int32_t db_flags, env_flags;
unsigned long working;

#define die_dataerr(f...) { LFATAL(f); exit(EXIT_DATAERR); }
#define die_syserr() { LFATAL("Fatal system error : %s",strerror(errno)); exit(EXIT_SYSTEM); }

struct option_info {
    char *configfile;
    int listspace;
};
struct option_info mkoptions;


void usage(char *fname)
{
    fprintf(stderr,"Valid options are:\n");
    fprintf(stderr, "%s /path_to_lessfs.cfg\n", fname);
    fprintf(stderr, "%s -c /path_to_lessfs.cfg\n", fname);
    fprintf(stderr, "%s -c /path_to_lessfs.cfg -s : Display available space (holes) in the file_io file\n", fname);
    exit(0);
}

int get_opts(int argc, char *argv[])
{

    int c;

    mkoptions.listspace = 0;
    mkoptions.configfile = NULL;
    while ((c = getopt(argc, argv, "sc:")) != -1)
        switch (c) {
        case 'c':
            if (optopt == 'c')
                printf
                    ("Option -%c requires a lessfs configuration file as argument.\n",
                     optopt);
            else
                mkoptions.configfile = optarg;
            break;
        case 's':
            mkoptions.listspace = 1;
            break;
        case 'h':
            usage(argv[0]);
            break;
        default:
            usage(argv[0]);
        }
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc < 2)
        usage(argv[0]);

    get_opts(argc, argv);
    if (NULL == mkoptions.configfile)
        mkoptions.configfile = argv[1];
    if (-1 == r_env_cfg(mkoptions.configfile))
        usage(argv[0]);
    parseconfig(3, 0);
    fuse_get_context()->uid = 0;
    fuse_get_context()->gid = 0;
    if ( !mkoptions.listspace ) {
       printf("\n\ndbp\n");
       listdbp();
       printf("\n\ndbu\n");
       if (NULL != config->blockdatabs) {
           listdbu();
           printf("\n\ndbdta\n");
           listdta();
       } else {
           flistdbu();
       }
       printf("\n\nfreelist\n");
       listfree(mkoptions.listspace);
       printf("\n\ndbb\n");
       listdbb();
       printf("\n\ndbdirent\n");
       listdirent();
       printf("\n\ndbs (symlinks)\n");
       list_symlinks();
       printf("\n\ndbl (hardlinks)\n");
       list_hardlinks();
    } else {
       printf("\n\nfreelist\n");
       listfree(mkoptions.listspace);
    }
    db_close(1);
    exit(0);
}
