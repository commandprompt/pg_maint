#!/usr/bin/env python
#!/usr/bin/python
###############################################################################
### COPYRIGHT NOTICE FOLLOWS.  DO NOT REMOVE
###############################################################################
### Copyright (c) 2016 COMMAND PROMPT, INC.
###
### Permission to use, copy, modify, and distribute this software and its 
### documentation for any purpose, without fee, and without a written agreement
### is hereby granted, provided that the above copyright notice and this paragraph 
### and the following two paragraphs appear in all copies.
###
### IN NO EVENT SHALL COMMAND PROMPT, INC. BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, 
### INDIRECT SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, 
### ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
### COMMAND PROMPT, INC. HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
###
### COMMAND PROMPT, INC. SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT 
### LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
### PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS,
### AND COMMAND PROMPT, INC. HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, 
### ENHANCEMENTS, OR MODIFICATIONS.
###
###############################################################################
#
# Original Author: Michael Vitale, michael@commandprompt.com
#
# Description: This python utility program performs PostgreSQL maintenance tasks.
#
# Inputs: all fields are optional except database and action.
# -h <hostname or IP address> -d <database> -n <schema> -p <PORT> -t <type> -u <db user> -l <load threshold> 
# -w <max rows> -a [action: ANALYZE, VACUUM_ANALYZE, VACUUM_FREEZE, REPORT] -r [dry run] -s [smart mode] -v [verbose output]
#
# Examples:
# -- vacuum analyze for all user tables in the database but only if load is less than 20% and rows < 1 mil
# ./pg_maint.py -h localhost -d test -p 5433 -u postgres -a vacuum_analyze -l 20 -w 1000000
# 
# -- smart analyze for all user tables in specific schema, but only if load is less than 40% and rows < 1 mil
# ./pg_maint.py -h localhost -d test -n public -p 5433 -s -u postgres -a analyze -l 40 -w 1000000 
#
# -- run report on entire test database:
# ./pg_maint.py -d test -a report
#
# Requirements:
#   1. psql client 
#   2. psutil (apt-get install python-psutil or yum install python-psutil). For windows: https://pypi.python.org/pypi?:action=display&name=psutil#downloads
#
# Assumptions:
# 1. db user defaults to postgres if not provided as parameter.
# 2. Max rows defaults to 10 million if not provided as parameter 
# 3. Password must be in local .pgpass file or client authentication changed to trust or peer
# 4. psql must be in the user's path
# 6. Load detection assumes that you are running this script from the database host.
# 7. SMART type will only consider tables whose pg_class.reltuples value is greater than zero. 
#    This value can be zero even if a few rows are in the table, because pg_class.reltuples is also a close estimate.
# 8. For analyze, vacuum_analyze, and vacuum_freeze actions, tables with over MAXROWS rows are not 
#    refreshed and are output in file, /tmp/PROGRAMPID_stats_deferred.sql
#
# -s (smart mode) dictates a filter algorithm to determine what tables will qualify for the maintenance commands.
# For analyze and vacuum analyze:
#     1. Refresh tables with no recent analyze or autovacuum_analyze in the last 30 days.
#     2. Refresh tables where pg_stat_user_tables.n_live_tup is less than half of pg_class.reltuples
# For vacuum freeze:
#     1. Refresh tables where current high XID age divided by autovacuum_freeze_max_age > 70%.
# 
# 
# Cron Job Info:
#    View cron job output: view /var/log/cron
#    source the database environment: source ~/db_catalog.ksh
#    Example cron job that does smart analyze commands for entire database every month at midnight:
#    0 0 1 * * /usr/bin/python /path to file/python pg_maint.py -t smart -a analyze >> /my log path/pg_maint_`/bin/date +'\%Y\%m\%d'`.log
#
# NOTE: You may have to source the environment variables file in the crontab to get this program to work.
#          #!/bin/bash
#          source /home/user/.bash_profile
#
# Report logic:
#  1. Get database conflicts, deadlocks, and temp_files.
#  1. Unused indexes are identified where there are less than 20 index scans and thee size of the table is > 100 MB.
#  2. Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 1GB.
#  3. See if archiving is getting behind by more than 1000 WAL files.
#  4. Contrast PG memory configuration to recommended ones
#  5. Identify orphaned large objects.
#  6. List tables getting close to transaction wraparound (more than halfway to max freeze threshold).
#  7. list tables that have not been analyzed or vacuumed in the last 60 days or whose size has grown significantly. 
#
# TODOs:
#    1. Handle pg_stat_activity to be compatible across pg versions. 
#       9.1 uses procpid, current_query, but 9.2+ uses pid, query respectively
#
# History:
# who did it            Date            did what
# ==========            =========       ==============================
# Michael Vitale        01/12/2016      Original coding using python 2.7.x on windows 8.1 and ubuntu 14.04 (pg 9.4)
# Michael Vitale        01/13/2016      Finished porting code from bash script, pg_refreshstats.sh
# Michael Vitale        01/14/2016      First crach at incorporated logic for report action.
################################################################################################################
import string, sys, os, time, datetime, exceptions
from decimal import *
import smtplib
import subprocess
from subprocess import Popen, PIPE
from optparse  import OptionParser
import getpass

# global defs
import maint_globals

# main supporting functions
from maint_funcs import *

#############################################################################################
def setupOptionParser():
    parser = OptionParser(add_help_option=False, description=maint_globals.DESCRIPTION)
    parser.add_option("-a", "--action",         dest="action",   help="Action to perform. Values are: ANALYZE, VACUUM_ANALYZE, VACUUM_FREEZE",  default="",metavar="ACTION")
    parser.add_option("-h", "--dbhost",         dest="dbhost",   help="DB Host Name or IP",                     default="",metavar="DBHOST")
    parser.add_option("-p", "--port",           dest="dbport",   help="db host port",                           default="",metavar="DBPORT")
    parser.add_option("-u", "--dbuser",         dest="dbuser",   help="db host user",                           default="",metavar="DBUSER")
    parser.add_option("-d", "--database",       dest="database", help="database name",                          default="",metavar="DATABASE")
    parser.add_option("-n", "--schema",         dest="schema", help="schema name",                              default="",metavar="SCHEMA")
    parser.add_option("-s", "--smart_mode",     dest="smart_mode", help="Smart Mode",                           default=False, action="store_true")    
    parser.add_option("-l", "--load_threshold", dest="load_threshold", help="Load Threshold",                   default="",metavar="LOAD_THRESHOLD")        
    parser.add_option("-w", "--max_rows",       dest="max_rows", help="Max Rows",                               default="",metavar="MAX_ROWS")            
    parser.add_option("-r", "--dry_run",        dest="dry_run", help="Dry Run Only",                            default=False, action="store_true")
    parser.add_option("-v", "--verbose",        dest="verbose", help="Verbose Output",                          default=False, action="store_true")                   
        
    return parser

#############################################################################################

#################################################################
#################### MAIN ENTRY POINT ###########################
#############################################@###################

optionParser   = setupOptionParser()
(options,args) = optionParser.parse_args()

# load the instance
pg = maint()

# Load and validate parameters
rc, errors = pg.set_dbinfo(options.action, options.dbhost, options.dbport, options.dbuser, options.database, options.schema, options.smart_mode, options.load_threshold, options.max_rows, options.dry_run, options.verbose, sys.argv)
if rc <> maint_globals.SUCCESS:
    print errors
    optionParser.print_help()
    sys.exit(1)

# returns value like 9.4
rc, results = pg.get_pgversion()
# print "pg version: %.1f" % Decimal(results)

rc, results = pg.check_load()
if rc <> maint_globals.SUCCESS:
    print results
    sys.exit(1)

rc, results = pg.do_vac_and_analyze()
if rc < maint_globals.SUCCESS:
    print results
    sys.exit(1)

rc, results = pg.do_report()
if rc < maint_globals.SUCCESS:
    print results
    sys.exit(1)

pg.cleanup()

sys.exit(0)


