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

###############################################################################
#
# Original Author: Michael Vitale, michael@commandprompt.com
#
# Description: This python utility program performs PostgreSQL maintenance tasks.
#
# History:
# who did it            Date            did what
# ==========            =========       ==============================
# Michael Vitale        01/12/2016      Original coding
###############################################################################
import string, sys, os, time, datetime, exceptions, math, tempfile
from decimal import *
import smtplib
import subprocess
from subprocess import Popen, PIPE
import psutil
from psutil import virtual_memory
# import psycopg2

# custom globals file
import maint_globals

class maint:
    def __init__(self):
        self.action            = ''    
        self.dbhost            = ''
        self.dbport            = 5432
        self.dbuser            = ''
        self.database          = ''
        self.smart_mode        = False
        self.load_threshold    = -1
        self.max_rows          = -1
        self.dry_run           = False
        self.verbose           = False
        self.connected         = False

        self.fout              = ''
        self.connstring        = ''

        self.actstring         = ''
        self.schemaclause      = ' '
        self.pid               = os.getpid()
        self.opsys             = ''        
        self.tempdir           = tempfile.gettempdir()
        self.workfile          = ''
        self.workfile_deferred = ''
        self.tempfile          = ''
        self.dir_delim         = ''
        self.totalmem          = -1
        self.totalmem_prettyGB = -1
        

        # db config stuff
        self.archive_mode      = ''
        self.max_connections   = -1
        self.datadir           = ''        
        self.shared_buffers    = -1
        self.work_mem          = -1
        self.maint_work_mem    = -1
        self.eff_cache_size    = -1

    ###########################################################
    def set_dbinfo(self, action, dbhost, dbport, dbuser, database, schema, smart_mode, load_threshold, max_rows, dry_run, verbose, argv):
        self.action         = action.upper()
        self.dbhost         = dbhost
        self.dbport         = dbport
        self.dbuser         = dbuser
        self.database       = database
        self.schema         = schema
        self.smart_mode     = smart_mode
        self.load_threshold = -1 if load_threshold == "" else int(load_threshold)
        self.max_rows       = 10000000 if max_rows == "" else int(max_rows)
        self.dry_run        = dry_run
        self.verbose        = verbose
         
        # process the schema or table elements
        total   = len(argv)
        cmdargs = str(argv)

        if os.name == 'posix':
            self.opsys = 'POSIX'
            self.dir_delim = '/'
        elif os.name == 'nt':
            self.opsys = 'NT'
            self.dir_delim = '\\'
        else:
            return maint_globals.ERROR, "Unsupported platform."

        self.workfile          = "%s%s%s_stats.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.workfile_deferred = "%s%s%s_stats_deferred.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.tempfile          = "%s%s%s_temp.sql" % (self.tempdir, self.dir_delim, self.pid)
        
        # construct the connection string that will be used in all database requests        
        # do not provide host name and/or port if not provided
        if self.dbhost <> '':
            self.connstring = " -h %s " % self.dbhost
        if self.database <> '':
            self.connstring += " -d %s " % self.database
        if self.dbport <> '':
            self.connstring += " -p %s " % self.dbport            
        if self.dbuser <> '':
            self.connstring += " -U %s " % self.dbuser                    
        if self.schema <> '':
            self.schemaclause = " and n.nspname = '%s' " % self.schema

        if self.verbose:
            print ("The total numbers of args passed to the script: %d " % total)
            print ("Args list: %s " % cmdargs)
            print ("connection string: %s" % self.connstring)

        # Make sure psql is in the path
        if self.opsys == 'POSIX':
            cmd = "which psql"
        else:
            # assume windows
            cmd = "where psql"

        rc, results = self.executecmd(cmd, True)
        if rc <> maint_globals.SUCCESS:
            errors = "Unable to determine if psql is in path. rc=%d results=%s" % (rc,results)
	    return rc, errors       
	if 'psql' not in results:    
	    msg = "psql must be in the path. rc=%d, results=%s" % (rc, results)    
	    return maint_globals.ERROR, msg 

        rc, results = self.get_configinfo()
        if rc <> maint_globals.SUCCESS:
            errors = "rc=%d results=%s" % (rc,results)
	    return rc, errors               

        # get total memory  total memory is in bytes
        mem = virtual_memory()
        self.totalmem = mem.total
        self.totalmem_prettyGB = mem.total / (1024*1024*1024) 	        

	# Validate parameters
        rc, errors = self.validate_parms()	
        if rc <> maint_globals.SUCCESS:
	    return rc, errors               

        if self.action == 'ANALYZE':
            self.actstring = 'ANALYZE VERBOSE '
        elif self.action == 'VACUUM_ANALYZE':
            self.actstring = 'VACUUM ANALYZE VERBOSE '
        elif self.action == 'VACUUM_FREEZE':
            self.actstring = 'VACUUM FREEZE ANALYZE VERBOSE '
	
        return maint_globals.SUCCESS, ''

    ###########################################################
    def validate_parms(self):
        
        if self.database == '':      
            return maint_globals.ERROR, "Database not provided."
        if self.action == '':
            return maint_globals.ERROR, "Action not provided."

        if self.action not in ('ANALYZE', 'VACUUM_ANALYZE', 'VACUUM_FREEZE', 'REPORT'):    
            return maint_globals.ERROR, "Invalid Action.  Valid actions are: ANALYZE, VACUUM_ANALYZE, VACUUM_FREEZE, or REPORT."                
 
        return maint_globals.SUCCESS, ""

    ###########################################################
    def cleanup(self):
        if self.connected:
            # do something here later if we enable a db driver
            self.connected = false
        return

    ###########################################################
    def getnow(self):
        now = datetime.datetime.now()
        adate = str(now)
        parts = adate.split('.')
        return parts[0]

    ###########################################################
    def getfilelinecnt(self, afile):
        return sum(1 for line in open(afile))

    ###########################################################
    def convert_humanfriendly_to_MB(self, humanfriendly):
    
        # assumes input in form: 10GB, 500 MB, 200 KB, 1TB
        # returns value in megabytes
        hf = humanfriendly.upper()
        valueMB = -1
        if 'TB' in (hf):
            pos = hf.find('TB')        
            valueMB = int(hf[0:pos]) * (1024*1024)
        elif 'GB' in (hf):
            pos = hf.find('GB')
            value = hf[0:pos]
            valueMB = int(hf[0:pos]) * 1024    
        elif 'MB' in (hf):
            pos = hf.find('MB')
            valueMB = int(hf[0:pos]) * 1
        elif 'KB' in (hf):        
            pos = hf.find('KB')        
            valueMB = round(float(hf[0:pos]) / 1024, 2)

        valuefloat = "%.2f" % valueMB
        return Decimal(valuefloat)

    ###########################################################
    def do_report_pgmemory(self):

        totalmemGB = self.totalmem / (1024*1024*1024)
        
        # shared_buffers:        
        # primitive logic: make shared buffers minimum 4GB or maximum 12GB or 25% of total memory
        # newer versions of PG seem to be more efficient with higher values, so logic is:
        # if pg 9.3 or lower max is 8GB, if pg 9.4 or higher 12 GB max
        rc, results = self.get_pgversion()
        if rc <> maint_globals.SUCCESS:
            return rc, results        
        version = Decimal(results)
        if version < 9.13:
           MAXGB = 8
        else:
           MAXGB = 12
        MINGB = 2
        percent25GB = totalmemGB * 0.25
        shared_buffersGB = self.shared_buffers / 1024
        
        if percent25GB > MAXGB:
            recommended_shared_buffers = MAXGB
        elif percent25GB < MINGB:
            recommended_shared_buffers = percent25GB
        else:
            recommended_shared_buffers = percent25GB
        if self.verbose:
            print "shared_buffers = %d percent25GB=%d  recommended=%d  totalmemGB=%d" % (self.shared_buffers, percent25GB, recommended_shared_buffers, totalmemGB)
        
        # maintenance_work_mem
        # current pg versions dont perform better with high values, since there is a hard-coded limit of the this memory that will be used,
        # effectively making memory here unavailable for usage elsewhere, so general rule:
        # MIN = 0.128GB, MAX 8 GB
        MIN = 0.128
        MAX = 8
        if totalmemGB < 4:
            recommended_maintenance_work_mem = MIN
        elif totalmemGB < 8:
            recommended_maintenance_work_mem = 0.256            
        elif totalmemGB < 16:
            recommended_maintenance_work_mem = 0.512
        elif totalmemGB < 32:
            recommended_maintenance_work_mem = 1            
        elif totalmemGB < 64:
            recommended_maintenance_work_mem = 2                        
        elif totalmemGB < 96:
            recommended_maintenance_work_mem = 4
        else:
            recommended_maintenance_work_mem = MAX
            
        # work_mem
        # need knowledge of SQL workload to do this effectivly, so for now, consider max connections and total memory
        if self.max_connections < 200:
            if totalmemGB < 4:
                recommended_work_mem = 0.016
            elif totalmemGB < 8:
                recommended_work_mem = 0.032            
            elif totalmemGB < 16:
                recommended_work_mem = 0.064
            elif totalmemGB < 32:
                recommended_work_mem = 0.128        
            elif totalmemGB < 64:
                recommended_work_mem = 0.256                        
            else:
                recommended_work_mem = 0.512
        else:
            if totalmemGB < 8:
                recommended_work_mem = 0.016            
            elif totalmemGB < 16:
                recommended_work_mem = 0.032
            elif totalmemGB < 32:
                recommended_work_mem = 0.064        
            elif totalmemGB < 64:
                recommended_work_mem = 0.128                        
            else:
                recommended_work_mem = 0.256
        
        # effective_cache_size: settings shows it in 8kb chunks
        # set it to 75% of memory
        recommended_effective_cache_size = .75 * totalmemGB

        print "Current and recommended PG Memory configuration settings."        
        print "*** Consider changing these values if they differ significantly ***"        
        print "effective_cache_size: %04d GB  recommended: %04d GB" % (self.eff_cache_size  / 1024, recommended_effective_cache_size)                        
        if self.shared_buffers < 1000:
           # show in MB instead of GB
           print "shared_buffers:       %04d MB  recommended: %04d MB" % (self.shared_buffers, recommended_shared_buffers * 1024)
        else:   
            print "shared_buffers:       %04d GB  recommended: %04d GB" % (self.shared_buffers / 1024, recommended_shared_buffers)
        print "maintenance_work_mem: %04d MB  recommended: %04d MB" % (self.maint_work_mem, recommended_maintenance_work_mem * 1024)
        print "work_mem:             %04d MB  recommended: %04d MB" % (self.work_mem, recommended_work_mem * 1000)

        
        return maint_globals.SUCCESS, ""


    ###########################################################
    def writeout(self,aline):
        if self.fout <> '':
            aline = aline + "\r\n"
            self.fout.write(aline)
        else: 
            # default to standard output
            print aline
        return        

    ###########################################################
    def get_configinfo(self):

        sql = "show all"
       
        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    # let calling function report the error
	    errors = "Unable to get config info: %d %s\nsql=%s\n" % (rc, results, sql)
            #aline = "%s" % (errors)         
            #self.writeout(aline)
            return rc, errors     

        f = open(self.tempfile, "r")    
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            aline = line.strip()
            if len(aline) < 1:
                continue

            fields = aline.split('|')
            name = fields[0].strip()
            setting = fields[1].strip()
            if self.verbose:
                print "name=%s  setting=%s" % (name, setting)

            if name == 'data_directory':
                self.datadir = setting
            elif name == 'archive_mode':
                self.archive_mode = setting           
            elif name == 'max_connections':
                self.max_connections = int(setting)
            elif name == 'shared_buffers':
                # shared_buffers in 8kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                # self.shared_buffers = int(rcsetting) / 8192
                rc = self.convert_humanfriendly_to_MB(setting)
                self.shared_buffers = rc
            elif name == 'maintenance_work_mem':
                # maintenance_work_mem in kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)            
                # self.maint_work_mem = int(setting) / 1024
                rc = self.convert_humanfriendly_to_MB(setting)                
                self.maint_work_mem = rc                
            elif name == 'work_mem':
                # work_mem in kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                #self.work_mem = int(setting) / 1024
                rc = self.convert_humanfriendly_to_MB(setting)                
                self.work_mem = rc                
            elif name == 'effective_cache_size':
                # effective_cache_size in 8 kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                rc = self.convert_humanfriendly_to_MB(setting)                
                self.eff_cache_size = rc                

        f.close()

        if self.verbose:
            print "shared_buffers = %d  maint_work_mem = %d  work_mem = %d" % (self.shared_buffers, self.maint_work_mem, self.work_mem)
            
        return maint_globals.SUCCESS, results

    ###########################################################
    # psql -p 5432 -t veda -c "select count(*)from pg_stat_activity"
    def executecmd(self, cmd, expect):
        if self.verbose:
            print "executing --> %s" % cmd
            
        # NOTE: try and catch does not work for Popen    
        try:
            # p = Popen(cmd, shell=True, stdout=PIPE)
            # p = subprocess.Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            values, err = p.communicate()            
            
        except exceptions.OSError as e:
            print "exceptions.OSError Error",e
            return maint_globals.ERROR, "Error(1)"         
        except BaseException:
            print "BaseException Error"
            return maint_globals.ERROR, "Error(2)"            
        except OSError:
            print "OSError Error"
            return maint_globals.ERROR, "Error(3)"    
        except RuntimeError:
            print "RuntimeError"
            return maint_globals.ERROR, "Error(4)"      
        except ValueError:
            print "Value Error"
            return maint_globals.ERROR, "Error(5)"              
        except Exception:
            print "General Exception Error"
            return maint_globals.ERROR, "Error(6)"    
        except:
            print "Unexpected error:", sys.exc_info()[0]
            return maint_globals.ERROR, "Error(7)"    
        
        if err is None:
            err = ""
        if values is None:
            values = ""
            
        values = values.strip()
        err    = err.strip()
        rc = p.returncode
        if self.verbose:
            print "rc=%d  values=***%s***  errors=***%s***" % (rc, values, err)        
        
        if rc == 2:
            return maint_globals.ERROR2, err
        elif err <> "":
            # do nothing since INFO information is returned here for analyze commands
            # return maint_globals.ERROR, err
            return maint_globals.SUCCESS, err
        elif values == "" and expect == True:
            return maint_globals.ERROR2, values
        elif rc <> maint_globals.SUCCESS:
            # print or(stderr_data)
            return rc, err
        elif values == "" and expect:
            return maint_globals.ERROR3, 'return set is empty'        
        else:
    	    return maint_globals.SUCCESS, values


    ###########################################################
    def get_pgversion(self):
        
        sql = "select substring(foo.version from 12 for 3) from (select version() as version) foo"
       
        # do not provide host name and/or port if not provided
        cmd = "psql %s -t -c \"%s\" " % (self.connstring, sql)
        
        rc, results = self.executecmd(cmd, True)
        if rc <> maint_globals.SUCCESS:
	    errors = "%s\n" % (results)
            aline = "%s" % (errors)         
            
            self.writeout(aline)
            return rc, errors
                
        return maint_globals.SUCCESS, str(results)
  
    ###########################################################
    def get_readycnt(self):
        
        sql = "select count(*) from (select pg_ls_dir from pg_ls_dir('%s') where pg_ls_dir ~ E'^[0-9A-F]{24}.ready$') as foo" % self.datadir
       
        # do not provide host name and/or port if not provided
        cmd = "psql %s -t -c \"%s\" " % (self.connstring, sql)
        
        rc, results = self.executecmd(cmd, True)
        if rc <> maint_globals.SUCCESS:
	    errors = "%s\n" % (results)
            aline = "%s" % (errors)         
            
            self.writeout(aline)
            return rc, errors
                
        return maint_globals.SUCCESS, str(results)

    ###########################################################
    def get_datadir(self):
        
        sql = "show data_directory"
       
        # do not provide host name and/or port if not provided
        cmd = "psql %s -t -c \"%s\" " % (self.connstring, sql)
        
        rc, results = self.executecmd(cmd, True)
        if rc <> maint_globals.SUCCESS:
	    errors = "%s\n" % (results)
            aline = "%s" % (errors)         
            
            self.writeout(aline)
            return rc, errors
                
        return maint_globals.SUCCESS, str(results)

    ###########################################################
    def get_load(self):
        
        if os.name == 'posix':
            cmd = "cat /proc/cpuinfo | grep processor | wc -l"
            rc, results = self.executecmd(cmd, True)
            if rc <> maint_globals.SUCCESS:
    	        errors = "%s\n" % (results)
                aline = "%s" % (errors)         
                self.writeout(aline)
                return rc, errors
            CPUs = int(results)

            cmd = "uptime | grep -ohe 'load average[s:][: ].*' | awk '{ print $5 }'"
            rc, results = self.executecmd(cmd, True)
            if rc <> maint_globals.SUCCESS:
	        errors = "%s\n" % (results)
                aline = "%s" % (errors)         
                self.writeout(aline)
                return rc, errors
            LOAD15=Decimal(results)

            LOADR= round(LOAD15/CPUs * 100,2)
            if self.verbose:
                print "LOAD15 = %.2f  CPUs=%d LOAD = %.2f%%" % (LOAD15, CPUs, LOADR)
        
        else:
            # assume windows
            cmd = "wmic cpu get loadpercentage"
            rc, results = self.executecmd(cmd, True)
            if rc <> maint_globals.SUCCESS:
	        errors = "%s\n" % (results)
                aline = "%s" % (errors)         
                self.writeout(aline)
                return rc, errors
            if self.verbose:
                print "windows load: %d %s" % (rc, results)        
            LOAD = results.split('\n')
            LOADR = int(LOAD[1])
        
        return maint_globals.SUCCESS, LOADR

    ###########################################################
    def check_load(self):

        if self.load_threshold == -1:
            return maint_globals.SUCCESS, ""        

        rc, results = self.get_load()
        if rc <> maint_globals.SUCCESS:
            return rc, results
            
        load = Decimal(results)
        if load > self.load_threshold:
            return maint_globals.WARNING, "Current load (%.2f%%) > Threshold load (%d%%).  Program will terminate." % (load, self.load_threshold)
        else:
            return maint_globals.SUCCESS, "Current load (%.2f%%) < Threshold load (%d%%)" % (load, self.load_threshold)

    ###########################################################
    def do_vac_and_analyze(self):

        rc, results = self.check_load()
        if rc == maint_globals.WARNING:
            return rc, results

        if self.action not in ('ANALYZE','VACUUM_ANALYZE', 'VACUUM_FREEZE'):
            return maint_globals.NOTICE, "N/A"
            
        # find tables whose row size is greater than threshold and thus will be deferred, i.e., work file created, but not executed.
        sql = "select '%s ' || n.nspname || '.' || c.relname || ';' as ddl from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname %s and n.nspname not in ('information_schema','pg_catalog') and c.reltuples > %d order by n.nspname, c.relname" \
        % (self.actstring, self.schemaclause, self.max_rows)
        
        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.workfile_deferred)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get deferred sql: %d %s\n" % (rc,results)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors        

        lines = self.getfilelinecnt(self.workfile_deferred) - 1
        if lines > 0:
            print "%d command(s) deferred due to exceeded max rows threshold (%d). See %s" % (lines, self.max_rows, self.workfile_deferred)

        adate = self.getnow()

        if self.smart_mode:
            if self.action == 'ANALYZE':
                sql="select '%s ' || n.nspname || '.' || c.relname || ';' as ddl from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname %s and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples between 1 and %d and round((u.n_live_tup::float / c.reltuples::float) * 100) < 50)) OR ((last_analyze is null and last_autoanalyze is null) or (now()::date  - last_analyze::date > 60 OR now()::date - last_autoanalyze::date > 60))) order by n.nspname, c.relname" \
                % (self.actstring, self.schemaclause, self.max_rows)
                
            elif self.action == 'VACUUM_ANALYZE':
                sql="select '%s ' || n.nspname || '.' || c.relname || ';' as ddl from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname %s and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples between 1 and %d and round((u.n_live_tup::float / c.reltuples::float) * 100) < 50)) OR ((last_vacuum is null and last_autovacuum is null) or (now()::date  - last_vacuum::date > 60 OR now()::date - last_autovacuum::date > 60))) order by n.nspname, c.relname" \
                % (self.actstring, self.schemaclause, self.max_rows) 
 
            elif self.action == 'VACUUM_FREEZE':
                # testing SQL -->
                # WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select s.setting, n.nspname as schema, c.relname as table, age(c.relfrozenxid) as xid_age, pg_size_pretty(pg_table_size(c.oid)) as table_size, round((age(c.relfrozenxid)::float / s.setting::float) * 100) as pct from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' and pg_table_size(c.oid) > 1073741824 ORDER BY age(c.relfrozenxid) DESC LIMIT 20;
                sql="WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select '%s ' || n.nspname || '.' || c.relname || ';' as ddl from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' %s and c.reltuples < %d and pg_table_size(c.oid) > 1073741824 and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 70  ORDER BY age(c.relfrozenxid) desc" \
                % (self.actstring, self.schemaclause, self.max_rows) 

        else:
            sql="select '%s ' || n.nspname || '.' || c.relname || ';' as ddl from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname %s and n.nspname not in ('information_schema','pg_catalog') and c.reltuples between 1 and %d order by n.nspname, c.relname" \
            % (self.actstring, self.schemaclause, self.max_rows)

        if self.verbose:
            print sql

        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.workfile)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to extract refresh commands from database: %d %s\n" % (rc,results)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        adate = self.getnow()
        lines = self.getfilelinecnt(self.workfile) - 1
        if lines > 0:
            print "%d table(s) qualify for maintenance: %s" % (lines, self.workfile)
        else:
            print "No tables qualify for maintenance."

        if self.schema == '':
             print "%s %s database-wide %s in progress..." % (adate, "Extensive" if self.smart_mode else "Smart", self.action)
        else:
             print "%s %s schema-wide %s in progress..." % (adate, "Extensive" if self.smart_mode else "Smart", self.action)    

        if self.dry_run == 1:
            print "Dry Run ended."             
            return maint_globals.SUCCESS, ""    
            
        
        # now execute the analyze commands in the workfile
        cmd = "psql %s < %s" % (self.connstring, self.workfile)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to execute work file commands: %d --> %s  %s\n" % (rc, self.workfile, results)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        print "%s %s\n" % (adate, results)     
        adate = self.getnow()
        print "%s work file commands completed" % (adate)    
        
        return maint_globals.SUCCESS, ""

    ###########################################################
    def do_report(self):
        if self.action not in ('REPORT'):
            return maint_globals.NOTICE, "N/A"    

        # get database conflicts if applicable
        rc, results = self.do_report_conflicts()
        if rc <> maint_globals.SUCCESS:
            return rc, results
        print ""

        # get bloated tables and indexes
        rc, results = self.do_report_bloated()
        if rc <> maint_globals.SUCCESS:
            return rc, results

        print ""
        
        # get unused indexes
        rc, results = self.do_report_unusedindexes()
        if rc <> maint_globals.SUCCESS:
            return rc, results

        print ""
        
        # get archiving status
        rc, results = self.do_report_archivingstatus()
        if rc <> maint_globals.SUCCESS:
            return rc, results

        print ""
 
        # get pg memory settings
        rc, results = self.do_report_pgmemory()
        if rc <> maint_globals.SUCCESS:
            return rc, results

        print ""
        
        # get count of orphaned large objects
        rc, results = self.do_report_orphanedlargeobjects()
        if rc <> maint_globals.SUCCESS:
            return rc, results

        print ""
       
        # get count of orphaned large objects
        rc, results = self.do_report_tablemaintenance()
        if rc <> maint_globals.SUCCESS:
            return rc, results

        print ""
        
        return maint_globals.SUCCESS, ""

    ###########################################################
    def do_report_bloated(self):

        sql = "SELECT current_database(), schemaname, tablename, ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,  iname,   ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat, CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes FROM (SELECT  schemaname, tablename, cc.reltuples, cc.relpages, bs,  CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,  COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta FROM ( SELECT   ma,bs,schemaname,tablename,   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,   (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum,  hdr+( SELECT 1+COUNT(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::NUMERIC) AS bs, CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants  GROUP BY 1,2,3,4,5 ) AS foo) AS rs  JOIN pg_class cc ON cc.relname = rs.tablename  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid) AS sml ORDER BY wastedbytes DESC"

        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get table/index bloat: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        print "Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 1GB."
        f = open(self.tempfile, "r")    
        lineno = 0
        bloated = 0
        for line in f:
            lineno = lineno + 1
            if self.verbose:
                print "%d line=%s" % (lineno,line)
            aline = line.strip()
            if len(aline) < 1:
                continue
                
            fields = aline.split('|')
            adatabase     = fields[0].strip()
            aschema       = fields[1].strip()
            atable        = fields[2].strip()
            pctbloated    = Decimal(fields[3].strip())
            wastedbytes   = long(fields[4].strip())
            index         = fields[5].strip()
            pctbloatedix  = Decimal(fields[6].strip())
            wastedbytesix = long(fields[7].strip())
            if self.verbose:
                print "db=%s schema=%s table=%s bloated=%.2f wasted=%d" % (adatabase, aschema, atable, pctbloated, wastedbytes)

            if pctbloated > 20.0 or wastedbytes > 1073741824:
                bloated = bloated + 1
                print "Bloated table: %s.%s.%s %.2f%% bloated   wasted bytes=%d" % (adatabase, aschema, atable, pctbloated, wastedbytes)               
            if pctbloatedix > 20.0 or wastedbytesix > 1073741824:
                bloated = bloated + 1
                print "Bloated index: %s.%s.%s.%s %.2f%% bloated   wasted bytes=%d" % (adatabase, aschema, atable, index, pctbloatedix, wastedbytesix)                               

        f.close() 
    
        if bloated == 0:
            print "No bloated tables were found."
            
        return maint_globals.SUCCESS, ""

    ###########################################################
    def do_report_conflicts(self):
    
        # NOTE: only applies to PG versions greater or equal to 9.1.  9.2 has additional fields of interest: deadlocks and temp_files
        rc, results = self.get_pgversion()
        if rc <> maint_globals.SUCCESS:
            return rc, results        
        version = Decimal(results)
        if version < 9.1:
            print "No database conflicts found."
            return maint_globals.SUCCESS, ""        
    
        if version == 9.1:
            sql="select datname, conflicts from pg_stat_database"
        else:
            sql="select datname, conflicts, deadlocks, temp_files from pg_stat_database"        

        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get database stats: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        f = open(self.tempfile, "r")    
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            if self.verbose:
                print "%d line=%s" % (lineno,line)
            aline = line.strip()
            if len(aline) < 1:
                continue

            deadlocks = -1
            temp_files = -1
            fields = aline.split('|')
            if version == 9.1:
                adatabase   = fields[0].strip()
                conflicts   = int(fields[1].strip())
            else:
                adatabase   = fields[0].strip()
                conflicts   = int(fields[1].strip())
                deadlocks   = int(fields[2].strip())
                temp_files  = int(fields[3].strip())                

            if self.verbose:
                print "db=%s  conflicts=%d  deadlocks=%d  temp_files=%d" % (adatabase, conflicts, deadlocks, temp_files)

            if conflicts > 0 or deadlocks > 0 or temp_files > 0:
                count = count + 1
                print "Database Conflicts found: db=%s  conflicts=%d  deadlocks=%d  temp_files=%d" % (adatabase, conflicts, deadlocks, temp_files)

        f.close() 
    
        if count == 0:
            print "No database conflicts were found."
    
        return maint_globals.SUCCESS, ""    
        
    ###########################################################
    def do_report_unusedindexes(self):
    
        # NOTE: no version problems identified yet
        rc, results = self.get_pgversion()
        if rc <> maint_globals.SUCCESS:
            return rc, results        
        version = Decimal(results)
    
        # Criteria is indexes that are used less than 20 times and whose table size is > 100MB
        sql="SELECT schemaname || '.' || relname AS table, indexrelname AS index, pg_size_pretty(pg_relation_size(relid)) as table_size, pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size, idx_scan as index_scans FROM pg_stat_user_indexes ui JOIN pg_index i ON ui.indexrelid = i.indexrelid WHERE NOT indisunique AND idx_scan < 20 AND pg_relation_size(relid) > 1024 * 1024 * 100 ORDER BY pg_relation_size(i.indexrelid) / nullif(idx_scan, 0) DESC NULLS FIRST, pg_relation_size(i.indexrelid) DESC"

        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get unused indexes: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        print "Unused indexes are identified where there are less than 20 index scans and thee size of the table is > 100 MB."

        f = open(self.tempfile, "r")    
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            if self.verbose:
                print "%d line=%s" % (lineno,line)
            aline = line.strip()
            if len(aline) < 1:
                continue

            fields = aline.split('|')
            atable     = fields[0].strip()
            aindex     = fields[1].strip()
            tablesize  = fields[2].strip()
            indexsize  = fields[3].strip()
            indexscans = int(fields[4].strip())

            if self.verbose:
                print "table=%s  index=%s  tablesize=%s  indexsize=%s  indexscans=%d" % (atable, aindex, tablesize, indexsize, indexscans)

            if conflicts > 0 or deadlocks > 0 or temp_files > 0:
                count = count + 1
                print "Unused index candidate: table=%s  index=%s  table_size=%s  index_size=%s index_scans=%d" % (atable, aindex, tablesize, indexsize, indexscans)

        f.close() 
    
        if count == 0:
            print "No unused index candidates were found."
    
        return maint_globals.SUCCESS, ""    
        
    ###########################################################
    def do_report_archivingstatus(self):
        
        rc, results = self.get_readycnt()
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get unused indexes: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        readycnt = int(results)
        
        if self.verbose:
            print "Ready Count = %d" % readycnt
        
        if readycnt > 1000:
            print "Archiving is behind more than 1000 WAL files. Current count - %d" % readycnt
        elif readycnt == 0:
            if self.archive_mode == 'on':
                print "Archiving is on but currently up-to-date."
            else:
                print "Archiving is off and not applicable."    
        else:
            print "Archiving is working and not too far behind. WALs waiting to be ardchived=%d" % readycnt        

        return maint_globals.SUCCESS, ""            
    
    ###########################################################
    def do_report_orphanedlargeobjects(self):

        if self.dbuser == '':
            user_clause = " "
        else:
            user_clause = " -U %s " % self.dbuser
            
        cmd = "vacuumlo -n %s %s" % (user_clause, self.database)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get unused indexes: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
        
        if self.verbose:
            print "vacuumlo results: rc=%d  %s" % (rc,results)
        
        # expecting substring like this --> "Would remove 35 large objects from database "agmednet.core.image"."
        numobjects = (results.split("Would remove"))[1].split("large objects")[0]

        if int(numobjects) == 0:
            print "No orphaned large objects were found."
        else:
            print "%d orphaned large objects were found.  Consider running vacuumlo to remove them."        
            
        return maint_globals.SUCCESS, ""            
        
    ###########################################################
    def do_report_tablemaintenance(self):        

        sql = "WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select s.setting, n.nspname as schema, c.relname as table, age(c.relfrozenxid) as xid_age, pg_size_pretty(pg_table_size(c.oid)) as table_size, round((age(c.relfrozenxid)::float / s.setting::float) * 100) as pct from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' and pg_table_size(c.oid) > 1073741824 and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 50 ORDER BY age(c.relfrozenxid)"

        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get user table stats: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        print "List of tables that are past the midway point of going into transaction wraparound mode and therefore candidates for manual vacuum freeze." 
        
        f = open(self.tempfile, "r")    
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            if self.verbose:
                print "%d line=%s" % (lineno,line)
            aline = line.strip()
            if len(aline) < 1:
                continue

            fields     = aline.split('|')
            setting    = fields[0].strip()
            schema     = fields[1].strip()
            table      = fields[2].strip()
            xid_age    = fields[3].strip()
            table_size = fields[4].strip()
            pct        = fields[5].strip()

            count = count + 1
            print "%s.%s  max freeze age=%s  xid_age=%s  table_size=%s  pct=%s" % (schema, atable, setting, xid_age, table_size, pct)

        f.close() 
    
        if count == 0:
            print "No vacuum freeze candidates were found."        
       
        print ""
        
        # handle vacuum analyze candidates     
        sql = "select n.nspname || '.' || c.relname as ddl, last_analyze, last_autoanalyze, last_vacuum, last_autovacuum, u.n_live_tup::bigint, c.reltuples::bigint, round((u.n_live_tup::float / CASE WHEN c.reltuples = 0 THEN 1.0 ELSE c.reltuples::float  END) * 100) as pct from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples > 0 and round((u.n_live_tup::float / c.reltuples::float) * 100) < 50)) OR ((last_vacuum is null and last_autovacuum is null and last_analyze is null and last_autoanalyze is null ) or (now()::date  - last_vacuum::date > 60 AND now()::date - last_autovacuum::date > 60 AND now()::date  - last_analyze::date > 60 AND now()::date  - last_autoanalyze::date > 60))) order by n.nspname, c.relname"

        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get user table stats: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        print "List of tables that have not been analyzed or vacuumed (manual and auto) in the last 60 days or whose size has changed significantly (n_live_tup/reltuples * 100 < 50)"
        print "and therefore candidates for manual vacuum analyze."

        f = open(self.tempfile, "r")    
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            if self.verbose:
                print "%d line=%s" % (lineno,line)
            aline = line.strip()
            if len(aline) < 1:
                continue

            fields     = aline.split('|')
            table            = fields[0].strip()
            last_analyze     = fields[1].strip()
            last_autoanalyze = fields[2].strip()
            last_vacuum      = fields[3].strip()
            last_autovacuum  = fields[4].strip()
            n_live_tup       = fields[5].strip()
            reltuples        = fields[6].strip()
            pct              = fields[7].strip()

            count = count + 1
            print "%s last_analyze=%s last_autoanalyze=%s last_vacuum=%s last_autovacuum=%s  n_live_tup=%s  reltuples=%s  pct=%s"  \
            % (table, last_analyze, last_autoanalyze, last_vacuum, last_autovacuum, n_live_tup, reltuples, pct)

        f.close() 
    
        if count == 0:
            print "No vacuum analyze candidates were found."        
        
        return maint_globals.SUCCESS, ""            
        
                
################################################################################################################

'''
other things to consider for reporting, although they are redundant with zabbix
1. Idle in transaction connections
2. Long running queries 
3. Lock waits
4. checkpoints too frequent or infrequent (depends on logging)

'''
