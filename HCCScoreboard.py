import pymysql as SQL
import pymysql.cursors
import time
import datetime
import time
import dashing
import sys

### Import SQL Login
f = open(sys.argv[1], 'r')
lines = f.readlines()
SQLItems = {}
for i in lines:
    SQLItems[i.split(" ")[0]] = i.split(" ")[1][:-1]
    print(i.split(" ")[0],i.split(" ")[1][:-1])
f.close()


def getUNOTimeData(timeDelta, SQLItems,scoreBox,numEntries):
    xdmoddb = SQL.connect(host=SQLItems["xdmodmysql_host"],user=SQLItems["xdmodmysql_username"],password=SQLItems["xdmodmysql_pass"],db=SQLItems["xdmodmysql_db"],cursorclass=pymysql.cursors.DictCursor)
    with open('key.txt', 'r') as file:
        auth_key = file.read().strip()
    dash = dashing.DashingImport('viz.unl.edu', auth_token = auth_key)
    # Dashboard Data Structure
    # Array of dict with keys of label, value, dept, campus
    
    dataToDash = []
    with xdmoddb:
        cur = xdmoddb.cursor()
        stmt = "SELECT account_name, SUM(cpu_time), group_name,college,campus FROM jobfact as j INNER JOIN mod_hpcdb.hpcdb_accounts as h ON j.account_id=h.account_id INNER JOIN mod_shredder.ldapGroups as s ON j.group_name=s.GroupName WHERE campus not like 'UNL' and campus not like 'IANR' and start_time_ts > "+timeDelta+" GROUP BY account_name order by SUM(cpu_time) DESC LIMIT "+numEntries+";"
        print(stmt)
        cur.execute(stmt)
        result = cur.fetchall()
        print(result) 
        for i in result:
            #cpuHour = str(int(result[i]["SUM(cpu_time)"])/3600)
            print(i["SUM(cpu_time)"])
            cpuHour = str(round(int(i["SUM(cpu_time)"])/3600))

            dataToDash.append({"label":i["account_name"],"value":cpuHour,"dept":i["college"],"campus":i["campus"]})
        print(dataToDash) 
        ### dostuff
        dash.SendEvent(scoreBox, {'items': dataToDash})
        cur.close()


# SYNCOFFSET is to account for database delay regarding data pushed at midnight and is used for testing with snapshots      
SYNCOFFSET =86400 
DAY = 86400     
WEEK = DAY * 7      
MONTH = DAY * 30
currentTime = int(time.time())      
## Last Day
#getUNOTimeData(str(currentTime - (DAY+SYNCOFFSET)),SQLItems,"TopDay")
## Last Week
getUNOTimeData(str(currentTime - (WEEK+SYNCOFFSET)),SQLItems,"TopWeek","5")
## Last Month aka 30 days
getUNOTimeData(str(currentTime - (MONTH+SYNCOFFSET)),SQLItems,"TopMonth","15")
