# pg_maint
This python program performs PostgreSQL maintenance tasks and provides reporting capabilities

(c) 2016, Michael Vitale

Bugs can be reported @ https://public.commandprompt.com/projects/pgmaint

## Requirements
1. psql client 
2. psutil (apt-get install python-psutil or yum install python-psutil). 
For windows: https://pypi.python.org/pypi?:action=display&name=psutil#downloads

## TODOs
1. Handle pg_stat_activity to be compatible across pg versions. 
  9.1 uses procpid, current_query, but 9.2+ uses pid, query respectively
