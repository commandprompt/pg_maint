# pg_maint
This python program performs PostgreSQL maintenance tasks and provides reporting capabilities.  This program is compatible on linux and windows.  You can get it here:
`git clone https://github.com/commandprompt/pg_maint.git pg_maint`

You can see a sample report here:
@ https://rawgit.com/commandprompt/pg_maint/master/SampleReport.html

(c) 2016, Command Prompt, Inc.

Bugs can be reported @ https://public.commandprompt.com/projects/pgmaint

## Requirements
1. python 2.6 or 2.7
2. psql client 
3. psutil for windows only: https://pypi.python.org/pypi?:action=display&name=psutil#downloads

## TODOS
1. Handle pg_stat_activity to be compatible across pg versions. 
9.1 uses procpid, current_query, but 9.2+ uses pid, query respectively

## Inputs
All fields are optional except database and action. The verbose flag is only intended as a debugging feature.

`-h <hostname or IP address> -d <database> -n <schema> -p <PORT> -t <type> -u <db user> -l <load threshold> -w <max rows> -a [action: ANALYZE, VACUUM_ANALYZE, VACUUM_FREEZE, REPORT] -r [dry run] -s [smart mode] -v [verbose output]`

## Examples
vacuum analyze for all user tables in the database but only if load is less than 20% and rows < 1 mil

`./pg_maint.py -h localhost -d test -p 5433 -u postgres -a vacuum_analyze -l 20 -w 1000000`

Same thing as previous one, but do a dry run.  This is useful to see wht commands will be executed, or is also useful for generating DDL so you can run it manually.

`./pg_maint.py -h localhost -d test -p 5433 -u postgres -a vacuum_analyze -l 20 -w 1000000 -r`

 
Smart analyze for all user tables in specific schema, but only if load is less than 40% and rows < 1 mil

`./pg_maint.py -h localhost -d test -n public -p 5433 -s -u postgres -a analyze -l 40 -w 1000000 `


Run report on entire test database and output to html format for web browser viewing:

`./pg_maint.py -d test -a report --html`


## Assumptions
1. db user defaults to postgres if not provided as parameter.
2. Max rows defaults to 10 million if not provided as parameter 
3. Password must be in local .pgpass file or client authentication changed to trust or peer
4. psql must be in the user's path
5. Load detection assumes that you are running this script from the database host.
6. SMART type will only consider tables whose pg_class.reltuples value is greater than zero. This value can be zero even if a few rows are in the table, because pg_class.reltuples is also a close estimate.
7. For analyze, vacuum_analyze, and vacuum_freeze actions, tables with over MAXROWS rows are not refreshed and are output in file, /tmp/PROGRAMPID_stats_deferred.sql


-s (smart mode) dictates a filter algorithm to determine what tables will qualify for the maintenance commands.
For analyze and vacuum analyze:

1. Refresh tables with no recent analyze or autovacuum_analyze in the last 60 days.
2. Refresh tables where pg_stat_user_tables.n_live_tup is less than half of pg_class.reltuples

For vacuum freeze:

1. Refresh tables where current high XID age divided by autovacuum_freeze_max_age > 70%.

## Report logic
1. Get database conflicts, deadlocks, and temp_files.
2. Unused indexes are identified where there are no index scans and the size of the index > 8KB.
3. Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 10 GB.
4. See if archiving is getting behind by more than 1000 WAL files.
5. Contrast PG memory configuration to recommended ones
6. Identify orphaned large objects.
7. List tables getting close to transaction wraparound (more than halfway to max freeze threshold).
8. list tables that have not been analyzed or vacuumed in the last 60 days or whose size has grown significantly. 
