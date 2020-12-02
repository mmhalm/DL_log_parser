# 2020 mmhalm@utu.fi
# Project work for vm.utu.fi download log parsing

import os
import pwd
import logging
import logging.handlers
import sqlite3
from datetime import datetime

# pylint: disable=undefined-variable

# Unprivileged os.nice() values: 0 ... 20 (= lowest priority)
NICE            = 20
EXECUTE_AS      = "utu"
LOGLEVEL        = logging.INFO  # logging.[DEBUG|INFO|WARNING|ERROR|CRITICAL]
CONFIG_FILE     = "site.conf" # All instance/site specific values

SCRIPTNAME = os.path.basename(__file__)


#
# Read config file
#
def read_config_file(cfgfile: str):
    """Reads (with ConfigParser()) '[Site]' and creates global variables. Argument 'cfgfile' has to be a filename only (not path + file) and the file must exist in the same directory as this script."""
    cfgfile = os.path.join(
        os.path.split(os.path.realpath(__file__))[0],
        cfgfile
    )
    if not os.path.exists(cfgfile):
        raise FileNotFoundError(f"Site configuration '{cfgfile}' not found!")
    import configparser
    cfg = configparser.ConfigParser()
    cfg.optionxform = lambda option: option # preserve case
    cfg.read(cfgfile)
    for k, v in cfg.items('Site'):
        globals()[k] = v

#
# Get filename, size and datetime from a (log) list
#
def getFilenameSizeDatetime(item: list):

    filename =  item[6].split("/")[-1]  
    size = int(item[9])                         
    myDatetime = datetime.strptime(item[3], "[%d/%b/%Y:%H:%M:%S")
 
    return (filename, myDatetime, size)


###############################################################################
#
# MAIN
#
###############################################################################
if __name__ == '__main__':

    #
    # Be nice, we're not in a hurry
    #
    os.nice(NICE)


    #
    # Set up logging
    #
    log = logging.getLogger(SCRIPTNAME)
    log.setLevel(LOGLEVEL)
    handler = logging.handlers.SysLogHandler(address = '/dev/log')
    handler.setFormatter(
        logging.Formatter('%(name)s: [%(levelname)s] %(message)s')
    )
    log.addHandler(handler)

    #
    # Resolve user and require to be executed as EXECUTE_AS
    #
    running_as = pwd.getpwuid(os.geteuid()).pw_name
    if running_as != EXECUTE_AS:
        log.error(
           f"This job must be executed as {EXECUTE_AS} (started by user '{running_as}')"
        )
        os._exit(-1)
    else:
        log.debug(f"Started! (executing as '{running_as}')")


    #
    # Read site specific configuration
    #
    log.debug(f"CWD: {os.getcwd()}")
    try:
        log.debug(f"Reading site configuration '{CONFIG_FILE}'")
        read_config_file(CONFIG_FILE)
    except:
        log.exception(f"Error reading site configuration '{CONFIG_FILE}'")
        os._exit(-1)

    #
    # Get last inserted datetime
    #
    select = '''select max(datetime) from download''' 
    try:
        with sqlite3.connect(DATABASE) as db:
            selcur = db.cursor()
            result = selcur.execute(select).fetchall()[0][0]
            lastDateTime = datetime.strptime(result, "%Y-%m-%d %H:%M:%S")
            print(lastDateTime)
    except:
        log.exception(f"Error getting last inserted datime from download file")

    

    #
    # Read log file
    #
    with open("vm.utu.fi.access.log", "r") as inpFile:

        with sqlite3.connect(DATABASE) as db:

            cursor = db.cursor()
            errcount = 0
            rowcount = 0
            savecount = 0

            sqlcommand = '''INSERT INTO dlevent (filename, datetime, size) VALUES (?,?,?)'''           

            for line in inpFile:
                item=line.split()
                rowcount += 1
                if item[6].startswith("/download"):
                    result = getFilenameSizeDatetime(item)

                    if lastDateTime < result[1]:
                    
                        #print(f"file: {result[0]}, size: {result[2]}, datetime: {result[1].isoformat()} ")

                        try:            
                            cursor.execute(sqlcommand, result)
                            savecount += 1
                        except Exception as e:
                            print(f"error: {str(e)} ")
                            errcount += 1
                            log.error(f"Error in {result[0]}, {result[1].isoformat()}")

    print(f"errors: {errcount}, rows: {rowcount}, saved items: {savecount}")
