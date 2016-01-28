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
import platform
# import psycopg2
# import psutil
#from psutil import virtual_memory

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
        self.reportfile        = ''
        self.dir_delim         = ''
        self.totalmemGB        = -1
        self.pgbindir          = ''
        self.pgversion         = Decimal('0.0')
        self.html_format       = False
        self.programdir        = ''
        self.imageURL          = "https://cloud.githubusercontent.com/assets/339156/12404356/c5c9f374-be08-11e5-8cfb-2ab6df0eb4b0.jpg"
        self.slaves            = []
        self.bloatedtables     = False
        self.unusedindexes     = False
        self.freezecandidates  = False
        self.analyzecandidates = False
        self.workwindowmins    = 180  # default max is 3 hours
        self.max_ready_files   = 1000
        self.timestartmins     = time.time() / 60
       
        # db config stuff
        self.archive_mode      = ''
        self.max_connections   = -1
        self.datadir           = ''        
        self.shared_buffers    = -1
        self.work_mem          = -1
        self.maint_work_mem    = -1
        self.eff_cache_size    = -1
        

    ###########################################################
    def set_dbinfo(self, action, dbhost, dbport, dbuser, database, schema, smart_mode, load_threshold, max_rows, work_window, max_ready_files, html_format, dry_run, verbose, argv):
        self.action          = action.upper()
        self.dbhost          = dbhost
        self.dbport          = dbport
        self.dbuser          = dbuser
        self.database        = database
        self.schema          = schema
        self.smart_mode      = smart_mode
        self.load_threshold  = -1 if load_threshold == "" else int(load_threshold)
        self.max_rows        = 10000000 if max_rows == "" else int(max_rows)
        self.workwindowmins  = 7200 if work_window == "" else int(work_window)
        self.max_ready_files = 1000 if max_ready_files == "" else int(max_ready_files)
        self.html_format     = html_format
        self.dry_run         = dry_run
        self.verbose         = verbose
         
        # process the schema or table elements
        total   = len(argv)
        cmdargs = str(argv)

        if os.name == 'posix':
            self.opsys = 'posix'
            self.dir_delim = '/'
        elif os.name == 'nt':
            self.opsys = 'nt'
            self.dir_delim = '\\'
        else:
            return maint_globals.ERROR, "Unsupported platform."

        self.workfile          = "%s%s%s_stats.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.workfile_deferred = "%s%s%s_stats_deferred.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.tempfile          = "%s%s%s_temp.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.reportfile        = "%s%s%s_report.html" % (self.tempdir, self.dir_delim, self.pid)
        
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

        self.programdir = sys.path[0]

        # Make sure psql is in the path
        if self.opsys == 'posix':
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
       
        pos = results.find('psql')
        if pos > 0:
            self.pgbindir = results[0:pos]

        rc, results = self.get_configinfo()
        if rc <> maint_globals.SUCCESS:
            errors = "rc=%d results=%s" % (rc,results)
	    return rc, errors               

        # get total memory  total memory is in bytes
        self.totalmemGB = self.get_physicalmem()

        # get pg bind directory from pg_config
        rc, results = self.get_pgbindir()
        if rc <> maint_globals.SUCCESS:
            errors = "rc=%d results=%s" % (rc,results)
	    return rc, errors                  

        rc, results = self.get_pgversion()
        if rc <> maint_globals.SUCCESS:
            return rc, results        
        self.pgversion = Decimal(results)

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
    def get_physicalmem(self):

        if self.opsys == 'posix':
            cmd = "free -g | grep Mem: | /usr/bin/awk '{ total=$2; } END { print \"total=\" total  }'"
            rc, results = self.executecmd(cmd, True)
            if rc <> maint_globals.SUCCESS:
    	        errors = "unable to get Total Physical Memory.  rc=%d %s\n" % (rc, results)
                aline = "%s" % (errors)         
                self.writeout(aline)
                return rc, errors
            #print "rc=%d results=%s" % (rc,results)
            results = results.split('=')
            totalmem_prettyGB = int(results[1].strip())
        else:
            # must be windows, nt
            from psutil import virtual_memory            
            mem = virtual_memory()
            totalmem_prettyGB = mem.total / (1024*1024*1024) 	            

        if self.verbose:
            print " total physical memory: %s GB" % totalmem_prettyGB
        
        return totalmem_prettyGB


    ###########################################################
    def cleanup(self):
        if self.connected:
            # do something here later if we enable a db driver
            self.connected = false
        # print "deleting temp file: %s" % self.tempfile
        try:
            os.remove(self.tempfile)
        except OSError:
            pass            
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
            # print "name=%s  setting=%s" % (name, setting)

            if name == 'data_directory':
                self.datadir = setting
            elif name == 'archive_mode':
                self.archive_mode = setting           
            elif name == 'max_connections':
                self.max_connections = int(setting)
            elif name == 'shared_buffers':
                # shared_buffers in 8kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                # self.shared_buffers = int(setting) / 8192
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
    def executecmd(self, cmd, expect):
        if self.verbose:
            print "executecmd --> %s" % cmd
            
        # NOTE: try and catch does not work for Popen    
        try:
            # Popen(args, bufsize=0, executable=None, stdin=None, stdout=None, stderr=None, preexec_fn=None, close_fds=False, shell=False, cwd=None, env=None, universal_newlines=False, startupinfo=None, creationflags=0)        
            if self.opsys == 'posix':
                p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, executable="/bin/bash")
            else:
                p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            values, err = p.communicate()            
            
        except exceptions.OSError as e:
            print "exceptions.OSError Error",e
            return maint_globals.ERROR, "Error(1)"         
        except BaseException as e:
            print "BaseException Error",e
            return maint_globals.ERROR, "Error(2)"            
        except OSError as e:
            print "OSError Error", e
            return maint_globals.ERROR, "Error(3)"    
        except RuntimeError as e:
            print "RuntimeError", e
            return maint_globals.ERROR, "Error(4)"      
        except ValueError as e:
            print "Value Error", e
            return maint_globals.ERROR, "Error(5)"              
        except Exception as e:
            print "General Exception Error", e
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
        
        if rc == 1 or rc == 2:
            return maint_globals.ERROR2, err
        elif rc == 127:
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
        
        xlogdir = "%s/pg_xlog/archive_status" % self.datadir
        sql = "select count(*) from (select pg_ls_dir from pg_ls_dir('%s') where pg_ls_dir ~ E'^[0-9A-F]{24}.ready$') as foo" % xlogdir
       
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
    def get_pgbindir(self):

        if self.opsys == 'posix':
            cmd = "pg_config | grep BINDIR"
        else:
            cmd = "pg_config | find \"BINDIR\""

        rc, results = self.executecmd(cmd, True)
        if rc <> maint_globals.SUCCESS:
            # don't consider failure unless bindir not already populated by "which psql" command that executed earlier
            if self.pgbindir == "":
   	        errors = "unable to get PG Bind Directory.  rc=%d %s\n" % (rc, results)
                aline = "%s" % (errors)         
                self.writeout(aline)
                return rc, errors
            else:
                return maint_globals.SUCCESS, self.pgbindir
        
        results = results.split('=')
        self.pgbindir   = results[1].strip()
        
        if self.verbose:
            print "PG Bind Directory = %s" % self.pgbindir
            
        return maint_globals.SUCCESS, str(results)

    ###########################################################
    def get_load(self):
        
        if self.opsys == 'posix':
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
        
        return maint_globals.SUCCESS, str(LOADR)

    ###########################################################
    def check_load(self):

        if self.load_threshold == -1:
            return maint_globals.SUCCESS, ""        

        rc, results = self.get_load()
        if rc <> maint_globals.SUCCESS:
            return rc, results
            
        load = Decimal(results)
        
        if load > self.load_threshold:
            return maint_globals.HIGHLOAD, "Current load (%.2f%%) > Threshold load (%d%%)" % (load, self.load_threshold)
        else:
            return maint_globals.SUCCESS,  "Current load (%.2f%%) < Threshold load (%d%%)" % (load, self.load_threshold)

    ###########################################################
    def get_slaves(self):
    
        if self.pgversion < Decimal('9.1'):
            # pg_stat_replication table does not exist
            return maint_globals.SUCCESS, ""            
    
        sql = "select client_addr from pg_stat_replication where state = 'streaming' order by 1"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get table/index bloat count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
        self.slaves = results.split('\n')
            
        return maint_globals.SUCCESS, ""        



    ###########################################################
    def initreport(self):

        # get the host name
        if self.dbhost == '':
            # tuples = os.uname()
            tuples = platform.uname()
            hostname = tuples[1]
        else:
            hostname = self.dbhost
            
        now = str(time.strftime("%c"))

        f = open(self.reportfile, "w")
        
        contextline = "<H2><p>Host: %s</p><p>Database: %s</p><p>Generated %s</p></H2>\n" % (hostname, self.database, now)
        info = \
            "<!DOCTYPE html>\n" + \
	    "<HTML>\n" + \
	    "<HEAD>\n" + \
	    "<TITLE>pg_maint Maintenance Report</TITLE>\n" + \
	    "</HEAD>\n" + \
	    "<style>" + \
            ".table1 {font-size:16px; border:1px solid black; border-collapse:collapse; }" + \
            ".table1 th { color:#000;    text-align:left; border:1px solid black; padding: 5px;}" + \
            ".table1 td { color:#000099; text-align:left; border:1px solid black; padding: 5px;}" + \
            "caption { text-align:left; caption-side: left; }" + \
            "</style>" + \
	    "<BODY BGCOLOR=\"FFFFFF\">\n" + \
	    "<div id='container'>\n" + \
	    "<img src='" + self.imageURL + "' style='float: left;'/>\n" + \
	    "<p><H1>pg_maint Maintenance Report</H1></p>\n" + \
	    "</div>\n" + contextline + \
	    "<a href=\"https://github.com/commandprompt/pg_maint\">pg_maint</a>  is available on github.\n" + \
	    "Send me mail at <a href=\"mailto:michael@commandprompt.com\"> support@commandprompt.com</a>.\n" + \
            "<HR>\n"
        f.write(info)
        f.close()
    
        return maint_globals.SUCCESS, ""    

    ###########################################################
    def finalizereport(self):
        f = open(self.reportfile, "a")
        info = "</BODY>\n</HTML>"
        f.write(info)
        f.close()
        
        return maint_globals.SUCCESS, ""            

    ###########################################################
    def appendreport(self, astring):
        f = open(self.reportfile, "a")
        f.write(astring)
        f.close()
        
        return maint_globals.SUCCESS, ""            

    ###########################################################
    def do_report(self):
        if self.action not in ('REPORT'):
            return maint_globals.NOTICE, "N/A"    

        if self.html_format:
            rc,results = self.initreport()
            if rc <> maint_globals.SUCCESS:
                return rc, results

        rc, results = self.get_slaves()
        if rc <> maint_globals.SUCCESS:
            return rc, results        

        # do health checks
        rc, results = self.do_report_healthchecks()
        if rc <> maint_globals.SUCCESS:
            return rc, results
        print ""

        # get pg memory settings
        rc, results = self.do_report_pgmemory()
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
        
        # See what tables need to be analyzed, vacuumed, etc
        rc, results = self.do_report_tablemaintenance()
        if rc <> maint_globals.SUCCESS:
            return rc, results
        print ""

        if self.html_format:
            rc,results = self.finalizereport()
            if rc <> maint_globals.SUCCESS:
                return rc, results

            print "html report file generated: %s" % self.reportfile
            
        return maint_globals.SUCCESS, ""

    ###########################################################
    def do_report_pgmemory(self):

        # shared_buffers:        
        # primitive logic: make shared buffers minimum 4GB or maximum 12GB or 25% of total memory
        # newer versions of PG seem to be more efficient with higher values, so logic is:
        # if pg 9.3 or lower max is 8GB, if pg 9.4 or higher 12 GB max
        if self.pgversion < Decimal('9.3'):
           MAXGB = 8
        else:
           MAXGB = 12
        MINGB = 2
        percent25GB = self.totalmemGB * 0.25
        shared_buffersGB = self.shared_buffers / 1024
        
        if percent25GB > MAXGB:
            recommended_shared_buffers = MAXGB
        elif percent25GB < MINGB:
            recommended_shared_buffers = percent25GB
        else:
            recommended_shared_buffers = percent25GB
        if self.verbose:
            print "shared_buffers = %d percent25GB=%d  recommended=%d  totalmemGB=%d" % (self.shared_buffers, percent25GB, recommended_shared_buffers, self.totalmemGB)
        
        # maintenance_work_mem
        # current pg versions dont perform better with high values, since there is a hard-coded limit of the this memory that will be used,
        # effectively making memory here unavailable for usage elsewhere, so general rule:
        # MIN = 0.128GB, MAX 8 GB
        MIN = 0.128
        MAX = 8
        if self.totalmemGB < 4:
            recommended_maintenance_work_mem = MIN
        elif self.totalmemGB < 8:
            recommended_maintenance_work_mem = 0.256            
        elif self.totalmemGB < 16:
            recommended_maintenance_work_mem = 0.512
        elif self.totalmemGB < 32:
            recommended_maintenance_work_mem = 1            
        elif self.totalmemGB < 64:
            recommended_maintenance_work_mem = 2                        
        elif self.totalmemGB < 96:
            recommended_maintenance_work_mem = 4
        else:
            recommended_maintenance_work_mem = MAX
            
        # work_mem
        # need knowledge of SQL workload to do this effectivly, so for now, consider max connections and total memory
        if self.max_connections < 200:
            if self.totalmemGB < 4:
                recommended_work_mem = 0.016
            elif self.totalmemGB < 8:
                recommended_work_mem = 0.032            
            elif self.totalmemGB < 16:
                recommended_work_mem = 0.064
            elif self.totalmemGB < 32:
                recommended_work_mem = 0.128        
            elif self.totalmemGB < 64:
                recommended_work_mem = 0.256                        
            else:
                recommended_work_mem = 0.512
        else:
            if self.totalmemGB < 8:
                recommended_work_mem = 0.016            
            elif self.totalmemGB < 16:
                recommended_work_mem = 0.032
            elif self.totalmemGB < 32:
                recommended_work_mem = 0.064        
            elif self.totalmemGB < 64:
                recommended_work_mem = 0.128                        
            else:
                recommended_work_mem = 0.256
        
        # effective_cache_size: settings shows it in 8kb chunks
        # set it to 75% of memory
        recommended_effective_cache_size = .75 * self.totalmemGB

        print "Current and recommended PG Memory configuration settings. Total Memory = %s GB" % self.totalmemGB        
        print "*** Consider changing these values if they differ significantly ***"        
        totalf = "PG Memory Values are based on a dedicated PG Server and total physical memory available: %04d GB" % (self.totalmemGB)
        print totalf
        totalf = "<H4>" + totalf + "</H4>"
        if self.html_format:
            self.appendreport(totalf)        
        effective_cache_size_f = "%04d GB" % (self.eff_cache_size  / 1024)
        recommended_effective_cache_size_f = "%04d GB" % recommended_effective_cache_size
        print "effective_cache_size:    %s  recommended: %s" % (effective_cache_size_f, recommended_effective_cache_size_f)        

        if self.shared_buffers < 1000:
            # show in MB instead of GB
            shared_buffers_f             = "%04d MB" % self.shared_buffers
            recommended_shared_buffers_f = "%04d MB" % (recommended_shared_buffers * 1024)
            print "shared_buffers:          %s  recommended: %s" % (shared_buffers_f, recommended_shared_buffers_f)
        else:  
            shared_buffers_f             = "%04d GB" % (self.shared_buffers / 1024)
            recommended_shared_buffers_f = "%04d GB" %  recommended_shared_buffers    
            print "shared_buffers:          %s  recommended: %s" % (shared_buffers_f, recommended_shared_buffers_f)

        maintenance_work_mem_f              = "%04d MB" % self.maint_work_mem
        recommended_maintenance_work_mem_f  = "%04d MB" % (recommended_maintenance_work_mem * 1000)
        work_mem_f                          = "%04d MB" % self.work_mem
        recommended_work_mem_f              = "%04d MB" % (recommended_work_mem * 1000)
        print "maintenance_work_mem:    %s  recommended: %s" % (maintenance_work_mem_f,  recommended_maintenance_work_mem_f )
        print "work_mem:                %s  recommended: %s" % (work_mem_f, recommended_work_mem_f )            
 
        if self.html_format:            
            html = "<table border=\"1\">\n" + "<tr>" + "<th align=\"center\">field</th>\n" + "<th align=\"center\">current value</th>\n" + "<th align=\"center\">recommended value</th>\n" + "</tr>\n"
            html += "<tr valign=\"top\">\n" + "<td align=\"left\">effective_cache_size</td>\n" + "<th align=\"center\">" + str(effective_cache_size_f) + "</th>\n" + "<th align=\"center\">" + str(recommended_effective_cache_size_f) + "</th>\n" + "</tr>\n" 
            html +="<tr valign=\"top\">\n" + "<td align=\"left\">shared_buffers</td>\n" + "<th align=\"center\">" + str(shared_buffers_f) + "</th>\n" + "<th align=\"center\">" + str(recommended_shared_buffers_f) + "</th>\n" + "</tr>\n" 
            html +="<tr valign=\"top\">\n" + "<td align=\"left\">maintenance_work_mem</td>\n" + "<th align=\"center\">" + str(maintenance_work_mem_f) + "</th>\n" + "<th align=\"center\">" + str(recommended_maintenance_work_mem_f) + "</th>\n" + "</tr>\n" 
            html +="<tr valign=\"top\">\n" + "<td align=\"left\">work_mem</td>\n" + "<th align=\"center\">" + str(work_mem_f) + "</th>\n" + "<th align=\"center\">" + str(recommended_work_mem_f) + "</th>\n" + "</tr>\n" + "</table>" 
            self.appendreport(html)
            self.appendreport("<p><br></p>")            
            

        return maint_globals.SUCCESS, ""

    ###########################################################
    def do_report_bloated(self):
        '''
         SELECT schemaname, tablename, ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,  iname,   ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat, CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes FROM (SELECT  schemaname, tablename, cc.reltuples, cc.relpages, bs,  CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,  COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta FROM ( SELECT   ma,bs,schemaname,tablename,   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,   (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum,  hdr+( SELECT 1+COUNT(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::NUMERIC) AS bs, CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants  GROUP BY 1,2,3,4,5 ) AS foo) AS rs  JOIN pg_class cc ON cc.relname = rs.tablename  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml where ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) > 20 OR ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) > 20 or CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END > 10737418240 OR CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END > 10737418240 ORDER BY wastedbytes DESC;
        '''

        if self.bloatedtables == False:
            return maint_globals.SUCCESS, ""

        sql = "SELECT schemaname, tablename, ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,  iname,   ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat, CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes FROM (SELECT  schemaname, tablename, cc.reltuples, cc.relpages, bs,  CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,  COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta FROM ( SELECT   ma,bs,schemaname,tablename,   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,   (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum,  hdr+( SELECT 1+COUNT(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::NUMERIC) AS bs, CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants  GROUP BY 1,2,3,4,5 ) AS foo) AS rs  JOIN pg_class cc ON cc.relname = rs.tablename  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml where ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) > 20 OR ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) > 20 or CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END > 10737418240 OR CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END > 10737418240 ORDER BY wastedbytes DESC"
        if self.html_format:                
            cmd = "psql %s --html -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        else:
            cmd = "psql %s -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)                
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get table/index bloat: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
        
        if self.html_format:
            self.appendreport("<H4>Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 10 GB.</H4>\n")        
        print "Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 10 GB."
        
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
            elif '(0 rows)' in aline:
                continue
                
            # bloated table or index
            bloated = bloated + 1
            if self.html_format:
                msg = "%s" % aline
                self.appendreport(msg)
            print "%s\n" % (aline)

        if self.html_format:
            self.appendreport("<p><br></p>")
        f.close() 
    
        return maint_globals.SUCCESS, ""
        
    ###########################################################
    def do_report_unusedindexes(self):
    
        # NOTE: no version problems identified yet
     
        if self.unusedindexes == False:
            return maint_globals.SUCCESS, ""     

        # See if this cluster has dependent slaves and if so give information warning
        slavecnt = len(self.slaves)
        if slavecnt > 0:
            msg = "%d slave(s) are dependent on this cluster.  Make sure these unused indexes are also unused on the slave(s) before considering them as index drop candidates." % slavecnt
            if self.html_format:
                msg = "<H4><p style=\"color:red;\">" + msg + "</p><H4>"
                self.appendreport(msg)                
            print msg

        # Criteria is indexes that are used less than 20 times and whose table size is > 100MB
        sql="SELECT relname as table, schemaname||'.'||indexrelname AS fqindexname, pg_size_pretty(pg_relation_size(indexrelid)) as total_size, pg_relation_size(indexrelid) as raw_size, idx_scan as index_scans FROM pg_stat_user_indexes JOIN pg_index USING(indexrelid) WHERE idx_scan = 0 AND idx_tup_read = 0 AND idx_tup_fetch = 0 AND NOT indisprimary AND NOT indisunique AND NOT indisexclusion AND indisvalid AND indisready AND pg_relation_size(indexrelid) > 8192 ORDER BY 4 DESC"

        if self.html_format:        
            cmd = "psql %s --html -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)                        
        else:
            cmd = "psql %s -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)                
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get unused indexes: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        if self.html_format:
            self.appendreport("<H4>Unused indexes are identified where there are no index scans and the size of the index is > 8 KB.</H4>\n")        
        print "Unused indexes are identified where there are no index scans and the size of the index  is > 8 KB."

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
            elif '(0 rows)' in aline:
                continue                

            if lineno == 1:
                if self.html_format:
                    self.appendreport(aline)    
                print "                 %s" % aline
            else:
                print "%s" % aline
                if self.html_format:
                    self.appendreport(aline)
        if self.html_format:
            self.appendreport("<p><br></p>")
        f.close() 
        return maint_globals.SUCCESS, ""            
        
    ###########################################################
    def do_report_tablemaintenance(self):        

        if self.freezecandidates == True:
            sql = "WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select s.setting, n.nspname as schema, c.relname as table, age(c.relfrozenxid) as xid_age, pg_size_pretty(pg_table_size(c.oid)) as table_size, round((age(c.relfrozenxid)::float / s.setting::float) * 100) as pct from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' and pg_table_size(c.oid) > 1073741824 and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 50 ORDER BY age(c.relfrozenxid)"
            if self.html_format:        
                cmd = "psql %s --html -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)                        
            else:
                cmd = "psql %s -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)                
            rc, results = self.executecmd(cmd, False)
            if rc <> maint_globals.SUCCESS:
       	        errors = "Unable to get user table stats: %d %s\ncmd=%s\n" % (rc, results, cmd)
                aline = "%s" % (errors)         
                self.writeout(aline)
                return rc, errors     

            if self.html_format:
                self.appendreport("<H4>List of tables that are past the midway point of going into transaction wraparound mode and therefore candidates for manual vacuum freeze.</H4>")        
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
                elif '(0 rows)' in aline:
                    continue                             
                elif '(0 rows)' in aline:
                    continue                

                if lineno == 1:
                    if self.html_format:
                         self.appendreport(aline)
                    print "%s" % aline
                else:
                    if self.html_format:
                         self.appendreport(aline)
                    print "%s" % aline
            f.close() 
            if self.html_format:
                self.appendreport("<p><br></p>")            

        print ""

        if self.analyzecandidates == False:
            return maint_globals.SUCCESS, ""                    
        
        sql = "select n.nspname || '.' || c.relname as table, last_analyze, last_autoanalyze, last_vacuum, last_autovacuum, u.n_live_tup::bigint, c.reltuples::bigint, round((u.n_live_tup::float / CASE WHEN c.reltuples = 0 THEN 1.0 ELSE c.reltuples::float  END) * 100) as pct from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = t.schemaname and t.tablename = c.relname and t.schemaname = u.schemaname and t.tablename = u.relname and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples > 0 and round((u.n_live_tup::float / c.reltuples::float) * 100) < 50)) OR ((last_vacuum is null and last_autovacuum is null and last_analyze is null and last_autoanalyze is null ) or (now()::date  - last_vacuum::date > 60 AND now()::date - last_autovacuum::date > 60 AND now()::date  - last_analyze::date > 60 AND now()::date  - last_autoanalyze::date > 60))) order by n.nspname, c.relname"

        if self.html_format:                
            cmd = "psql %s --html -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        else:
            cmd = "psql %s -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)        
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get user table stats: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        if self.html_format:
            self.appendreport("<H4>List of tables that have not been analyzed or vacuumed (manual and auto) in the last 60 days or whose size has changed significantly (n_live_tup/reltuples * 100 < 50) and therefore candidates for manual vacuum analyze.</H4>")        
        print "List of tables that have not been analyzed or vacuumed (manual and auto) in the last 60 days or whose size has changed significantly (n_live_tup/reltuples * 100 < 50) and therefore candidates for manual vacuum analyze."
        
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
            elif '(0 rows)' in aline:
                continue                

            if lineno == 1:
                if self.html_format:
                    self.appendreport(aline)
                print "                      %s" % aline
            else:
                if self.html_format:
                    self.appendreport(aline)                
                print "%s" % aline

        if self.html_format:
            self.appendreport("<p><br></p>")
        f.close() 
    
        return maint_globals.SUCCESS, ""            

    #############################################################################################
    def do_report_healthchecks(self):

        # setup special table format
        html = "<table class=\"table1\" style=\"width:100%\"> <caption><h3>Health Checks</h3></caption>" + \
               "<tr> <th>Status</th> <th>Category</th> <th>Analysis</th> </tr>" 
        self.appendreport(html)    
        
        '''
        slavecnt = len(self.slaves)
        # still need to check for empty string as first slot
        if slavecnt == 1 and  self.slaves[0].strip() == '':
            slavecnt = 0
            
        if self.verbose:
            print "slavecnt=%d  slaves=%s" % (slavecnt, self.slaves)        
        msg = "%d slave(s) were detected." % slavecnt
        print msg
        if self.html_format:
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Number of Slaves</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"            
            self.appendreport(html)            
        '''
        
        ##################### 
        # get cache hit ratio
        #####################
        # SELECT datname, blks_read, blks_hit, round((blks_hit::float/(blks_read+blks_hit+1)*100)::numeric, 2) as cachehitratio FROM pg_stat_database ORDER BY datname, cachehitratio
        sql = "SELECT blks_read, blks_hit, round((blks_hit::float/(blks_read+blks_hit+1)*100)::numeric, 2) as cachehitratio FROM pg_stat_database where datname = '%s' ORDER BY datname, cachehitratio" % self.database
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)    
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
            errors = "Unable to get database cache hit ratio: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
	cols = results.split('|')
	blks_read   = int(cols[0].strip())
	blks_hit    = int(cols[1].strip())
	cache_ratio = Decimal(cols[2].strip())
        if cache_ratio < Decimal('70.0'):
            msg = "low cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10004;</font></td><td width=\"20%\"><font color=\"red\">Cache Hit Ratio</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        elif cache_ratio < Decimal('90.0'):
            msg = "Moderate cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Cache Hit Ratio</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            msg = "High cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Cache Hit Ratio</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        if self.html_format:            
            self.appendreport(html)
        print msg


        ######################################################
        # get connection counts and compare to max connections
        ######################################################
        sql = "select count(*) from pg_stat_activity"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)    
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
            errors = "Unable to get count of current connections: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
        conns = int(results)
        result = float(conns) / self.max_connections
        percentconns = int(math.floor(result * 100))    
        if self.verbose:
            print "Max connections = %d   Current connections = %d   PctConnections = %d" % (self.max_connections, conns, percentconns)

        if percentconns > 80:
            # 80 percent is the hard coded threshold
            msg = "Current connections (%d) are greater than 80%% of max connections (%d) " % (conns, self.max_connections)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Connections</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        else:
            msg = "Current connections (%d) are not too close to max connections (%d) " % (conns, self.max_connections)
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Connections</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        if self.html_format:
            self.appendreport(html)            
        print msg

        
        #######################################################################
        # get existing "idle in transaction" connections longer than 10 minutes
        #######################################################################        
        # NOTE: 9.1 uses procpid, current_query, and no state column, but 9.2+ uses pid, query and state columns respectively.  Also idle is <IDLE> in current_query for 9.1 and less
        #       <IDLE> in transaction for 9.1 but idle in transaction for state column in 9.2+        
        if self.pgversion < Decimal('9.2'):
            # select substring(current_query,1,50), round(EXTRACT(EPOCH FROM (now() - query_start))), now(), query_start  from pg_stat_activity;
            sql = "select count(*) from pg_stat_activity where current_query ilike \'<IDLE> in transaction%\' and round(EXTRACT(EPOCH FROM (now() - query_start))) > 10"
        else:
            # select substring(query,1,50), round(EXTRACT(EPOCH FROM (now() - query_start))), now(), query_start, state  from pg_stat_activity;
            sql = "select count(*) from pg_stat_activity where state = \'idle in transaction\' and round(EXTRACT(EPOCH FROM (now() - query_start))) > 10"      
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)    
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
            errors = "Unable to get count of idle in transaction connections: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
        idle_in_transaction_cnt = int(results)
            
        if idle_in_transaction_cnt == 0:
            msg = "No \"idle in transaction\" longer than 10 minutes were detected."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Idle In Transaction</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            msg = "%d \"idle in transaction\" longer than 15 minutes were detected." % idle_in_transaction_cnt
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Idle In Transaction</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            
        if self.html_format:            
            self.appendreport(html)
        print msg


        ######################################
        # Get long running queries > 5 minutes
        ######################################
        # NOTE: 9.1 uses procpid, current_query, and no state column, but 9.2+ uses pid, query and state columns respectively.  Also idle is <IDLE> in current_query for 9.1 and less
        #       <IDLE> in transaction for 9.1 but idle in transaction for state column in 9.2+
        if self.pgversion < Decimal('9.2'):
            # select procpid,datname,usename, client_addr, now(), query_start, substring(current_query,1,100), now() - query_start as duration from pg_stat_activity where current_query not ilike '<IDLE%' and current_query <> ''::text and now() - query_start > interval '5 minutes';
            sql = "select count(*) from pg_stat_activity where current_query not ilike '<IDLE%' and current_query <> ''::text and now() - query_start > interval '5 minutes'"
        else:
            # select pid,datname,usename, client_addr, now(), state, query_start, substring(query,1,100), now() - query_start as duration from pg_stat_activity where state not ilike 'idle%' and query <> ''::text and now() - query_start > interval '5 minutes';
            sql = "select count(*) from pg_stat_activity where state not ilike 'idle%' and query <> ''::text and now() - query_start > interval '5 minutes'"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)    
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
            errors = "Unable to get count of long running queries: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
        long_queries_cnt = int(results)
            
        if long_queries_cnt == 0:
            msg = "No \"long running queries\" longer than 5 minutes were detected."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Long Running Queries</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            msg = "%d \"long running queries\" longer than 5 minutes were detected." % long_queries_cnt
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Long Running Queries</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            
        if self.html_format:            
            self.appendreport(html)
        print msg

        
        ##########################################################
        # Get lock waiting transactions where wait is > 30 seconds
        ##########################################################
        if self.pgversion < Decimal('9.2'):
          # select procpid, datname, usename, client_addr, now(), query_start, substring(current_query,1,100), now() - query_start as duration from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds';
            sql = "select count(*) from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds'"
        else:
          # select pid, datname, usename, client_addr, now(), query_start, substring(query,1,100), now() - query_start as duration from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds';
            sql = "select count(*) from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds'"        
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)    
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
            errors = "Unable to get count of blocked queries: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
        blocked_queries_cnt = int(results)
            
        if long_queries_cnt == 0:
            msg = "No \"Waiting/Blocked queries\" longer than 30 seconds were detected."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Waiting/Blocked queries</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            msg = "%d \"Waiting/Blocked queries\" longer than 30 seconds were detected." % blocked_queries_cnt
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Waiting/Blocked queries</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            
        if self.html_format:            
            self.appendreport(html)
        print msg


        #################################
        # get archiving info if available
        #################################
        rc, results = self.get_readycnt()
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get archiving status: %d %s\n" % (rc, results)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        readycnt = int(results)
        
        if self.verbose:
            print "Ready Count = %d" % readycnt
        
        if readycnt > 1000:
            if self.html_format:
                msg = "Archiving is behind more than 1000 WAL files. Current count: %d" % readycnt
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Archiving Status</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
            
        elif readycnt == 0:
            if self.archive_mode == 'on':
                if self.html_format:
                    msg = "Archiving is on and currently up-to-date."
                    html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Archiving Status</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
            else:
                if self.html_format:
                    msg = "Archiving is off so nothing to analyze."
                    html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Archiving Status</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            if self.html_format:
                msg = "Archiving is working and not too far behind. WALs waiting to be archived=%d" % readycnt
                html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Archiving Status</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        
        print msg
        if self.html_format:
            self.appendreport(html)

        ########################################################################################################################################### 
        # database conflicts: only applies to PG versions greater or equal to 9.1.  9.2 has additional fields of interest: deadlocks and temp_files
        ###########################################################################################################################################
        if self.pgversion < Decimal('9.1'):
            msg = "No database conflicts found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Database Conflicts</font></td><td width=\"75%\"><font color=\"blue\">N/A</font></td></tr>"            
            return maint_globals.SUCCESS, ""        
            print msg
            if self.html_format:
                self.appendreport(html)
            return maint_globals.SUCCESS, ""        

        if self.pgversion < Decimal('9.2'):
            sql="select datname, conflicts from pg_stat_database where datname = '%s'" % self.database
        else:
            sql="select datname, conflicts, deadlocks, temp_files from pg_stat_database where datname = '%s'" % self.database
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
	rc, results = self.executecmd(cmd, False)
	if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get database conflicts: %d %s\nsql=%s\n" % (rc, results, sql)
	    aline = "%s" % (errors)         
	    self.writeout(aline)
	    return rc, errors     

	cols = results.split('|')
	database   = cols[0].strip()
	conflicts  = int(cols[1].strip())
	deadlocks  = -1
	temp_files = -1
	if len(cols) > 2:
	    deadlocks  = int(cols[2].strip())
	    temp_files = int(cols[3].strip())

        if self.verbose:
            print 
                
        if conflicts > 0 or deadlocks > 0 or temp_files > 0:
            msg = "Database conflicts found: database=%s  conflicts=%d  deadlocks=%d  temp_files=%d" % (database, conflicts, deadlocks, temp_files)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Database Conflicts</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        else:
            msg = "No database conflicts found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Database Conflicts</font></td><td width=\"75%\"><font color=\"blue\">No database conflicts found.</font></td></tr>"            

        print msg
        if self.html_format:
            self.appendreport(html)
       
       
        ###############################################################################################################
        # Check for checkpoint frequency
        # NOTE: Checkpoints should happen every few minutes, not less than 5 minutes and not more than 15-30 minutes
        #       unless recovery time is not a priority and High I/O SQL workload is in which case 1 hour is reasonable.
        ###############################################################################################################
        sql = "SELECT total_checkpoints, seconds_since_start / total_checkpoints / 60 AS minutes_between_checkpoints, checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time FROM (SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) AS seconds_since_start, (checkpoints_timed+checkpoints_req) AS total_checkpoints, checkpoints_timed, checkpoints_req, checkpoint_write_time / 1000 as checkpoint_write_time, checkpoint_sync_time / 1000 as checkpoint_sync_time FROM pg_stat_bgwriter) AS sub"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
	rc, results = self.executecmd(cmd, False)
	if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get checkpoint frequency: %d %s\nsql=%s\n" % (rc, results, sql)
	    aline = "%s" % (errors)         
	    self.writeout(aline)
	    return rc, errors     

	cols = results.split('|')
	total_checkpoints     = int(cols[0].strip())
	minutes               = Decimal(cols[1].strip())
	checkpoints_timed     = int(cols[2].strip())
	checkpoints_req       = int(cols[3].strip())
        checkpoint_write_time = int(float(cols[4].strip()))
        checkpoint_sync_time  = int(float(cols[5].strip()))        \
        # calculate average checkpoint time
        avg_checkpoint_seconds = ((checkpoint_write_time + checkpoint_sync_time) / (checkpoints_timed + checkpoints_req)) 
	
        if minutes < Decimal('5.0'):
            msg = "Checkpoints are occurring too fast, every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Checkpoint Frequency</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            
        elif minutes > Decimal('60.0'):
            msg = "Checkpoints are occurring too infrequently, every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Checkpoint Frequency</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                        
        else:
            msg = "Checkpoints are occurring every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Checkpoint Frequency</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"   
            
        print msg
        if self.html_format:
            self.appendreport(html)

        ############################################################
        # Check checkpoints, background writers, and backend writers
        ############################################################
        sql = "select checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc, checkpoint_write_time / 1000 as checkpoint_write_time, checkpoint_sync_time / 1000 as checkpoint_sync_time, (100 * checkpoints_req) / (checkpoints_timed + checkpoints_req) AS checkpoints_req_pct,    pg_size_pretty(buffers_checkpoint * block_size / (checkpoints_timed + checkpoints_req)) AS avg_checkpoint_write,  pg_size_pretty(block_size * (buffers_checkpoint + buffers_clean + buffers_backend)) AS total_written,  100 * buffers_checkpoint / (buffers_checkpoint + buffers_clean + buffers_backend) AS checkpoint_write_pct,    100 * buffers_clean / (buffers_checkpoint + buffers_clean + buffers_backend) AS background_write_pct, 100 * buffers_backend / (buffers_checkpoint + buffers_clean + buffers_backend) AS backend_write_pct from pg_stat_bgwriter, (SELECT cast(current_setting('block_size') AS integer) AS block_size) bs"

        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
	rc, results = self.executecmd(cmd, False)
	if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get background/backend writers: %d %s\nsql=%s\n" % (rc, results, sql)
	    aline = "%s" % (errors)         
	    self.writeout(aline)
	    return rc, errors     

	cols = results.split('|')
        checkpoints_timed     = int(cols[0].strip())
        checkpoints_req       = int(cols[1].strip())
        buffers_checkpoint    = int(cols[2].strip())
        buffers_clean         = int(cols[3].strip())
        maxwritten_clean      = int(cols[4].strip())
        buffers_backend       = int(cols[5].strip())
        buffers_backend_fsync = int(cols[6].strip())
        buffers_alloc         = int(cols[7].strip())
        checkpoint_write_time = int(float(cols[8].strip()))
        checkpoint_sync_time  = int(float(cols[9].strip()))
        checkpoints_req_pct   = int(cols[10].strip())
        avg_checkpoint_write  = cols[11].strip()
        total_written         = cols[12].strip()
        checkpoint_write_pct  = int(cols[13].strip())
        background_write_pct  = int(cols[14].strip())
        backend_write_pct     = int(cols[15].strip())        
        
        # calculate average checkpoint time
        avg_checkpoint_seconds = ((checkpoint_write_time + checkpoint_sync_time) / (checkpoints_timed + checkpoints_req)) 

        if self.verbose:
            msg = "chkpt_time=%d chkpt_req=%d  buff_chkpt=%d  buff_clean=%d  maxwritten_clean=%d  buff_backend=%d  buff_backend_fsync=%d  buff_alloc=%d, chkpt_req_pct=%d avg_chkpnt_write=%s total_written=%s chkpnt_write_pct=%d background_write_pct=%d  backend_write_pct=%d avg_checkpoint_time=%d seconds" \
            % (checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc, checkpoints_req_pct, avg_checkpoint_write, total_written, checkpoint_write_pct, background_write_pct, backend_write_pct, avg_checkpoint_seconds)
            print msg        
        
        msg = ''
        if buffers_backend_fsync > 0:
            msg = "bgwriter fsync request queue is full. Backend using fsync.  "
        if backend_write_pct > (checkpoint_write_pct + background_write_pct):
            msg += "backend writer doing most of the work.  Consider decreasing \"bgwriter_delay\" by 50% or more to make background writer do more of the work.  "
        if maxwritten_clean > 500000:            
            # for now just use a hard coded value of 500K til we understand the math about this better
            msg += "background writer stopped cleaning scan %d times because it had written too many buffers.  Consider increasing \"bgwriter_lru_maxpages\".  " % maxwritten_clean
        if checkpoints_timed > (checkpoints_req * 5):
            msg += "\"checkpoint_timeout\" contributing to a lot more checkpoints (%d) than \"checkpoint_segments\" (%d).  Consider increasing \"checkpoint_timeout\".  " % (checkpoints_timed, checkpoints_req)
        if msg <> '':
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Checkpoint/Background/Backend Writers</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            
        else:
            msg = "No problems detected with checkpoint, background, or backend writers."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Checkpoint/Background/Backend Writers</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"   

        print msg
        if self.html_format:
            self.appendreport(html)

        ########################
        # orphaned large objects	    
        ########################
        if self.dbuser == '':
            user_clause = " "
        else:
            user_clause = " -U %s " % self.dbuser
            
        cmd = "%s/vacuumlo -n %s %s" % (self.pgbindir, user_clause, self.database)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get orphaned large objects: %d %s\ncmd=%s\n" % (rc, results, cmd)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     
        
        # expecting substring like this --> "Would remove 35 large objects from database "agmednet.core.image"."
        numobjects = (results.split("Would remove"))[1].split("large objects")[0]

        if int(numobjects) == 0:
            msg = "No orphaned large objects were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Orphaned Large Objects</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"            
        else:
            msg = "%d orphaned large objects were found.  Consider running vacuumlo to remove them." % int(numobjects)        
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Orphaned Large Objects</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            	        

        print msg            
        if self.html_format:
            self.appendreport(html)

        ##################################
        # Check for bloated tables/indexes
        ##################################
        sql = "SELECT count(*) FROM (SELECT  schemaname, tablename, cc.reltuples, cc.relpages, bs,  CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,  COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta FROM ( SELECT   ma,bs,schemaname,tablename,   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,   (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum,  hdr+( SELECT 1+COUNT(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::NUMERIC) AS bs, CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants  GROUP BY 1,2,3,4,5 ) AS foo) AS rs  JOIN pg_class cc ON cc.relname = rs.tablename  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml where ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) > 20 OR ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) > 20 or CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END > 10737418240 OR CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END > 10737418240"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get table/index bloat count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        if int(results) == 0:
            self.bloatedtables = False
            msg = "No bloated tables/indexes were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Bloated Tables and/or Indexes</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"            
        else:
            self.bloatedtables = True
            msg = "%d bloated tables/indexes were found (See table list below)." % int(results)        
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Bloated Tables and/or Indexes</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            	        

        print msg            
        if self.html_format:
            self.appendreport(html)
        

        ##########################
        # Check for unused indexes
        ##########################
        sql="SELECT count(*) FROM pg_stat_user_indexes JOIN pg_index USING(indexrelid) WHERE idx_scan = 0 AND idx_tup_read = 0 AND idx_tup_fetch = 0 AND NOT indisprimary AND NOT indisunique AND NOT indisexclusion AND indisvalid AND indisready AND pg_relation_size(indexrelid) > 8192"    
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get unused indexes count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        if int(results) == 0:
            self.unusedindexes = False
            msg = "No unused indexes were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Unused Indexes</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"            
        else:
            self.unusedindexes = True
            msg = "%d unused indexes were found (See table list below)." % int(results)        
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Unused Indexes</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            	        

        print msg            
        if self.html_format:
            self.appendreport(html)

        
        ####################################
        # Check for vacuum freeze candidates
        ####################################
        sql="WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select count(c.*) from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' and pg_table_size(c.oid) > 1073741824 and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 50"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get vacuum freeze candidate count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        if int(results) == 0:
            self.freezecandidates = False
            msg = "No vacuum freeze candidates were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Vacuum Freeze Candidates</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"            
        else:
            self.freezecandidates = True
            msg = "%d vacuum freeze candidates were found (See table list below)." % int(results)        
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Vacuum Freeze Candidates</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            	        

        print msg            
        if self.html_format:
            self.appendreport(html)


        ##############################
        # Check for analyze candidates
        ##############################
        sql="select count(*) from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = t.schemaname and t.tablename = c.relname and t.schemaname = u.schemaname and t.tablename = u.relname and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples > 0 and round((u.n_live_tup::float / c.reltuples::float) * 100) < 50)) OR ((last_vacuum is null and last_autovacuum is null and last_analyze is null and last_autoanalyze is null ) or (now()::date  - last_vacuum::date > 60 AND now()::date - last_autovacuum::date > 60 AND now()::date  - last_analyze::date > 60 AND now()::date  - last_autoanalyze::date > 60)))"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc <> maint_globals.SUCCESS:
	    errors = "Unable to get vacuum analyze candidate count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)         
            self.writeout(aline)
            return rc, errors     

        if int(results) == 0:
            self.analyzecandidates = False
            msg = "No vacuum analyze candidates were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Vacuum Analyze Candidates</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"            
        else:
            self.analyzecandidates = True
            msg = "%d vacuum analyze candidate(s) were found (See table list below)." % int(results)        
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Vacuum Analyze Candidates</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            	        

        print msg            
        if self.html_format:
            self.appendreport(html)


        #######################################
        # network health checks begin here
        # ONLY applies to linux at current time
        #######################################
        
        if self.opsys == 'posix':


            # Check for high number of standby connections, i.e., TIME_WAIT states
            # This usually indicates a lot of short-lived connections and the absence of a connection pooler
            '''
            Due to the way TCP/IP works, connections can not be closed immediately. Packets may arrive out of order or be retransmitted after the connection has been closed. CLOSE_WAIT indicates that the remote endpoint (other side of the connection) has closed the connection. TIME_WAIT indicates that local endpoint (this side) has closed the connection. The connection is being kept around so that any delayed packets can be matched to the connection and handled appropriately. The connections will be removed when they time out within four minutes.  Basically the "WAIT" states mean that one side closed the connection but the final confirmation of the close is pending.
	    
	    ss -an state time-wait | wc -l
	    netstat -nat | egrep 'TIME_WAIT' | wc -l
	    netstat -ntu  | grep TIME_WAIT | wc -l
	    
	    Now, let's see why this state can be annoying on a server handling a lot of connections. There are three aspects of the problem:
	    the slot taken in the connection table preventing new connections of the same kind,
	    the memory occupied by the socket structure in the kernel, and
	    the additional CPU usage.
	    A connection in the TIME-WAIT state is kept for one minute in the connection table. This means, another connection with the same quadruplet (source address, source port, destination address, destination port) cannot exist.
	    The result of ss -tan state time-wait | wc -l is not a problem per se!
            '''
            
            cmd =  "ss -an state time-wait | wc -l"
            rc, results = self.executecmd(cmd, False)
            if rc <> maint_globals.SUCCESS:
    	        errors = "Unable to get network standby connections count: %d %s\nsql=%s\n" % (rc, results, sql)
                aline = "%s" % (errors)         
                self.writeout(aline)
                return rc, errors     
            standby = int(results)
            if standby < 1000:
                msg = "Network: Relatively few network standby connections (%d)." % standby
                html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Network Standby Connections</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"            
            else:
                msg = "Network: High number of standby connections: %d.  This may indicate a lot of short-lived connections and the absence of a connection pooler." % standby
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Network Standby Connections</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            	        

            print msg            
            if self.html_format:
                self.appendreport(html)


        ########################################        
        ### end of linux only network checks ###
        ########################################

        # finish special table format
        self.appendreport("</table>")
        self.appendreport("<p><br></p>")                                        


        return maint_globals.SUCCESS, ""            


    ###########################################################
    def delay(self, freeze):

        while True:
            # see if we passed work window threshold, assume minimum work windows is always 1 hour.
            workmins = ((time.time() / 60) - self.timestartmins)
            if workmins >= self.workwindowmins:
                results = "Work window (%d mins.) expired (%d mins.) Program terminating before work completed." % (self.workwindowmins, workmins)
                print results
                return maint_globals.TOOLONG, results
             
            # see if load is too high
            rc, results = self.check_load()
            if rc == maint_globals.HIGHLOAD:
                # wait 10 minutes before trying again
                print "deferring action (%s) for 10 minutes due to high load.  %s" % (self.action, results)
                time.sleep(600)
                continue
            elif freeze:
                # make sure we aren't getting too far behind in WALs ready to be archived.
                rc, results = self.get_readycnt()
                if rc <> maint_globals.SUCCESS:
	            errors = "Unable to get archiving status: %d %s\n" % (rc, results)
                    aline = "%s" % (errors)         
                    self.writeout(aline)
                    return rc, errors     
                    
                readycnt = int(results)
                if readycnt > self.max_ready_files:
                    print "deferring action (%s) for 10 minutes due to high load.  %s" % (self.action, results)
                    time.sleep(600)
                    continue
                else:
                    break
            else:
                break
   
        return maint_globals.SUCCESS, ""                                
        
    ###########################################################
    def do_vac_and_analyze(self):

        if self.action not in ('ANALYZE','VACUUM_ANALYZE', 'VACUUM_FREEZE'):
            return maint_globals.NOTICE, "N/A"
            
        # find tables whose row size is greater than threshold and thus will be deferred, i.e., work file created, but not executed.
        sql = "select '%s ' || n.nspname || '.' || c.relname || ';' as ddl from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname %s and n.nspname not in ('information_schema','pg_catalog') and c.reltuples > %d order by n.nspname, c.relname" \
        % (self.actstring, self.schemaclause, self.max_rows)
        if self.verbose:
            print sql
        if self.html_format:
            cmd = "psql %s --html -t -c \"%s\" > %s" % (self.connstring, sql, self.workfile_deferred)        
        else:
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
                # WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select s.setting, n.nspname as schema, c.relname as table, age(c.relfrozenxid) as xid_age, pg_size_pretty(pg_table_size(c.oid)) as table_size, round((age(c.relfrozenxid)::float / s.setting::float) * 100) as pct from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r'  and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 85 ORDER BY age(c.relfrozenxid) DESC LIMIT 20;
                # note to avoid tracking down the database with the table that has the highest xid age, just do it regardless of the size of the table being over a gigabyte
                # sql="WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select '%s ' || n.nspname || '.' || c.relname || ';' as ddl from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' %s and c.reltuples < %d and pg_table_size(c.oid) > 1073741824 and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 85  ORDER BY age(c.relfrozenxid) desc" \
                # % (self.actstring, self.schemaclause, self.max_rows) 
                sql="WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select '%s ' || n.nspname || '.' || c.relname || ';' as ddl from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' %s and c.reltuples < %d and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 85  ORDER BY age(c.relfrozenxid) desc" \
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
            return maint_globals.SUCCESS, ""            

        if self.schema == '':
             print "%s %s database-wide %s in progress..." % (adate, "Smart" if self.smart_mode else "Extensive", self.action)
        else:
             print "%s %s schema-wide %s in progress..." % (adate, "Smart" if self.smart_mode else "Extensive", self.action)    
        
        # Version 1.1: instead of executing as a batch, read in and process each table in a loop
        f = open(self.workfile, "r")    
        lineno = 0
        count  = 0
        
        for line in f:
            lineno = lineno + 1
            aline = line.strip()
            if len(aline) < 1:
                continue

            # sleep if load is too high and if work window has passed, return rc to indicate abort.
            rc, results = self.delay(True);
            if rc == maint_globals.TOOLONG:
                print results
                return rc, results
            elif rc <> maint_globals.SUCCESS:
                print "delay error: rc=%d %s" % (rc, results)
                return rc, results            

            adate = self.getnow()            
            
            if self.dry_run == 1:
                print "%s:  simulate executing --> %s" % (adate, aline)
                continue

            print "%s:  executing --> %s" % (adate, aline)            
            
            cmd = "psql %s -t -c \"%s\"" % (self.connstring, aline)
            rc, results = self.executecmd(cmd, False)
            if rc <> maint_globals.SUCCESS:
    	        errors = "rc=%d  Unable to execute command: %s\n" % (rc, results, aline)
                aline = "%s" % (errors)         
                self.writeout(aline)
                return rc, errors    
 
        f.close()
        
        adate = self.getnow()
        workmins = ((time.time() / 60) - self.timestartmins)        
        print "%s work file commands completed. Total time: %d minutes " % (adate, workmins)    
        
        return maint_globals.SUCCESS, ""
                
################################################################################################################


