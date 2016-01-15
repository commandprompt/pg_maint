# pg_maint
This python program performs PostgreSQL maintenance tasks and provides reporting capabilities

(c) 2016, Michael Vitale

Bugs can be reported @ https://public.commandprompt.com/projects/pgmaint

## Requirements
1. psql client 
2. psutil (apt-get install python-psutil or yum install python-psutil). 
For windows: https://pypi.python.org/pypi?:action=display&name=psutil#downloads

## TODOS
1. Handle pg_stat_activity to be compatible across pg versions. 
9.1 uses procpid, current_query, but 9.2+ uses pid, query respectively

## Inputs
All fields are optional except database and action:

`-h <hostname or IP address> -d <database> -n <schema> -p <PORT> -t <type> -u <db user> -l <load threshold> -w <max rows> -a [action: ANALYZE, VACUUM_ANALYZE, VACUUM_FREEZE, REPORT] -r [dry run] -s [smart mode] -v [verbose output]`

## Examples:
-- vacuum analyze for all user tables in the database but only if load is less than 20% and rows < 1 mil
`./pg_maint.py -h localhost -d test -p 5433 -u postgres -a vacuum_analyze -l 20 -w 1000000`
 
-- smart analyze for all user tables in specific schema, but only if load is less than 40% and rows < 1 mil
`./pg_maint.py -h localhost -d test -n public -p 5433 -s -u postgres -a analyze -l 40 -w 1000000 `
#
-- run report on entire test database:
`./pg_maint.py -d test -a report`
