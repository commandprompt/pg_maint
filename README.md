# pg_maint
This python program performs PostgreSQL maintenance tasks and provides reporting capabilities.  This program is compatible on linux and windows.  You can get it here:
`git clone https://github.com/commandprompt/pg_maint.git pg_maint`

(c) 2016, Command Prompt, Inc.

Bugs can be reported @ https://public.commandprompt.com/projects/pgmaint

## Assumptions
1. db user defaults to postgres if not provided as parameter.
2. Max rows defaults to 10 million if not provided as parameter 
3. Password must be in local .pgpass file or client authentication changed to trust or peer
4. psql must be in the user's path
5. Load detection assumes that you are running this script from the database host.
6. SMART type will only consider tables whose pg_class.reltuples value is greater than zero. This value can be zero even if a few rows are in the table, because pg_class.reltuples is also a close estimate.
7. For analyze, vacuum_analyze, and vacuum_freeze actions, tables with over MAXROWS rows are not refreshed and are output in file, /tmp/PROGRAMPID_stats_deferred.sql

