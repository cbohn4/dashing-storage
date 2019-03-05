import pymysql as SQL
import pymysql.cursors
import time
import datetime
import time
import dashing
import sys
import urllib.request
import re
import dashing
import json

### Import SQL Login
f = open(sys.argv[1], 'r')
lines = f.readlines()
SQLItems = {}
for i in lines:
    SQLItems[i.split(" ")[0]] = i.split(" ")[1][:-1]
    print(i.split(" ")[0],i.split(" ")[1][:-1])
f.close()
with open('key.txt', 'r') as file:
    auth_key = file.read().strip()
dash = dashing.DashingImport('viz.unl.edu',port=5000, auth_token = auth_key)

### Cloud Service Pricing

#AWS -- 4 Core Instance with 8GB Memory for 1 hour
AWS_CPU = 0.1664/4
AWS_MEM = 0
AWS_STOR = 0.0000616438 # per GB Hour // 0.045 per GB/Month
AWS_NET = 0.01 # per GB
#GCP -- Retrieved from Google Cloud Platform
GCP_CPU = 0.031611 # per CPU Hour
GCP_MEM = 0.004237 # per GB Hour
GCP_STOR = 0.00005479452 # per GB Hour // 0.040 per GB/Month
GCP_NET = 0.08/1024 # per GB
#Azure -- 4 Core Instance with 8GB Memory for 1 hour
AZ_CPU = 0.06
AZ_MEM = 0
AZ_STOR = (1.54/32)/730 # per GB Hour // 0.048 per GB/Month
AZ_NET = 0.0865722 # per GB
#DO -- 4 Core Instance with 8GB Memory for 1 hour
DO_CPU =  0.0119
DO_MEM = 0
DO_STOR = 0.000136986 # per GB Hour // 0.10 per GB/Month
DO_NET = 0.01 # per GB
#IBM -- 4 Core Instance with 8GB Memory for 1 hour
IBM_CPU =  0.0173
IBM_MEM = 0
IBM_STOR = 0.000035616 # per GB Hour // 0.10 per GB/Month
IBM_NET = 0.09 # per GB

f = urllib.request.urlopen("http://anvil-beta.unl.edu:8123/")
rawData = json.load(f)
ANVIL_MEM = int(rawData["mem_count"])/1024
ANVIL_CPU = int(rawData["core_count"])
ANVIL_STOR = int(rawData["volume_gb"]) + int(rawData["disk_gb"])

((ANVIL_MEM * AWS_MEM + ANVIL_CPU * AWS_CPU + AWS_STOR * ANVIL_STOR)* 24)
SILO_STOR = 2*1024**2
CTR_STOR = 2*1024**2


### Do data magic to get pricing from data values for clusters
def getPricingData(timeDelta, SQLItems):

    ### Handle Authentication with database and dashing
    xdmoddb = SQL.connect(host=SQLItems["xdmodmysql_host"],user=SQLItems["xdmodmysql_username"],password=SQLItems["xdmodmysql_pass"],db=SQLItems["xdmodmysql_db"],cursorclass=pymysql.cursors.DictCursor)
    with open(sys.argv[2], 'r') as file:
        auth_key = file.read().strip()
    dash = dashing.DashingImport('viz.unl.edu', auth_token = auth_key)   
    with xdmoddb:
        cur = xdmoddb.cursor()
        stmt = "SELECT cpu_time,start_time_ts, end_time_ts, mem_req   FROM jobfact WHERE start_time_ts > "+timeDelta+";"
        print(stmt)
        cpuHour = 0.0
        memAmt = 0.0
        cur.execute(stmt)
        result = cur.fetchall()
        for i in result:
            # Translate Memory
            timeUsed = (float(i["end_time_ts"])-float(i["start_time_ts"]))/3600.0
            if "M" in i["mem_req"]:
                i["mem_req"] = str(float(i["mem_req"].split("M")[0])/1024.0)
            elif "G" in i["mem_req"]:
                i["mem_req"] = str(float(i["mem_req"].split("G")[0])/1.0)
            
            else:
                i["mem_req"] = "0"
            #cost += (float(i["cpu_time"])/3600) * cpuCost + float(i["mem_req"]) * memCost
            cpuHour += float(i["cpu_time"])/3600
            memAmt += float(i["mem_req"])
        stmt = "SELECT MIN(start_time_ts) FROM jobfact;"
        cur.execute(stmt)
        result = cur.fetchall()
        return(cpuHour, memAmt, result[0]["MIN(start_time_ts)"])
        cur.close()
        
# SYNCOFFSET is to account for database delay regarding data pushed at midnight and is used for testing with snapshots 
REDMONTHSTOR = (7.2*1024*1024) + CTR_STOR + SILO_STOR    
REDMONTHCOMP = 6309390
REDMONTHMEM = 29900800
NETMONTH = (730*60*60)*(0.063)/8 + (730*60*60)*(6.5)/8
SYNCOFFSET =86400 
DAY = 86400     
WEEK = DAY * 7      
MONTH = DAY * 30
YEAR = DAY * 365
ALL = DAY * 99999999
currentTime = int(time.time())      
dayCPU, dayMem, startTS = getPricingData(str(currentTime - (DAY+SYNCOFFSET)),SQLItems)
weekCPU, weekMem, startTS = getPricingData(str(currentTime - (WEEK+SYNCOFFSET)),SQLItems)
monthCPU, monthMem,startTS = getPricingData(str(currentTime - (MONTH+SYNCOFFSET)),SQLItems)
yearCPU, yearMem, startTS = getPricingData(str(currentTime - (YEAR+SYNCOFFSET)),SQLItems)
#allCPU, allMem, startTS = getPricingData(str(currentTime - (ALL+SYNCOFFSET)),SQLItems)
allMonth = (time.time() - startTS)/(3600 * 24 * 30)
awsDay = (float(dayCPU)) * AWS_CPU + float(dayMem) * AWS_MEM + REDMONTHCOMP/30 * AWS_CPU + REDMONTHSTOR/30 * AWS_STOR + REDMONTHMEM/30 * AWS_MEM + NETMONTH/30 * AWS_NET + ((ANVIL_MEM * AWS_MEM + ANVIL_CPU * AWS_CPU + AWS_STOR * ANVIL_STOR)* 24)
awsWeek = (float(weekCPU)) * AWS_CPU + float(weekMem) * AWS_MEM + REDMONTHCOMP/4 * AWS_CPU + REDMONTHSTOR/4 * AWS_STOR+REDMONTHMEM/4 * AWS_MEM + NETMONTH/4 * AWS_NET + ((ANVIL_MEM * AWS_MEM + ANVIL_CPU * AWS_CPU + AWS_STOR * ANVIL_STOR)* 24*7)
awsMonth = (float(monthCPU)) * AWS_CPU + float(monthMem) * AWS_MEM + REDMONTHCOMP * AWS_CPU + REDMONTHSTOR * AWS_STOR+ REDMONTHMEM * AWS_MEM + NETMONTH * AWS_NET + ((ANVIL_MEM * AWS_MEM + ANVIL_CPU * AWS_CPU + AWS_STOR * ANVIL_STOR)* 24*30)
awsYear = (float(yearCPU)) * AWS_CPU + float(yearMem) * AWS_MEM + REDMONTHCOMP * 12 * AWS_CPU + REDMONTHSTOR *12* AWS_STOR+REDMONTHMEM*12 * AWS_MEM + NETMONTH *12 * AWS_NET + ((ANVIL_MEM * AWS_MEM + ANVIL_CPU * AWS_CPU + AWS_STOR * ANVIL_STOR)* 24*365)
#awsAll = (float(allCPU)) * AWS_CPU + float(allMem) * AWS_MEM + REDMONTHCOMP * allMonth * AWS_CPU + REDMONTHSTOR * allMonth * AWS_STOR+REDMONTHMEM * allMonth * AWS_MEM + NETMONTH * allMonth * AWS_NET + ((ANVIL_MEM * AWS_MEM + ANVIL_CPU * AWS_CPU + AWS_STOR * ANVIL_STOR)* 24*30*allMonth)

print("AWS day cost: $" + str(awsDay))
print("AWS week cost: $" + str(awsWeek))
print("AWS month cost: $" + str(awsMonth))
print("AWS year cost: $" + str(awsYear))
#print("AWS all cost: $" + str(awsAll))

gcpDay = (float(dayCPU)) * GCP_CPU + float(dayMem) * GCP_MEM + REDMONTHCOMP/30 * GCP_CPU + REDMONTHSTOR/30 * GCP_STOR + REDMONTHMEM/30 * GCP_MEM + NETMONTH/30 * GCP_NET + ((ANVIL_MEM * GCP_MEM + ANVIL_CPU * GCP_CPU + GCP_STOR * ANVIL_STOR)* 24)
gcpWeek = (float(weekCPU)) * GCP_CPU + float(weekMem) * GCP_MEM + REDMONTHCOMP/4 * GCP_CPU + REDMONTHSTOR/4 * GCP_STOR+REDMONTHMEM/4 * GCP_MEM + NETMONTH/4 * GCP_NET + ((ANVIL_MEM * GCP_MEM + ANVIL_CPU * GCP_CPU + GCP_STOR * ANVIL_STOR)* 24*7)
gcpMonth = (float(monthCPU)) * GCP_CPU + float(monthMem) * GCP_MEM + REDMONTHCOMP * GCP_CPU + REDMONTHSTOR * GCP_STOR+ REDMONTHMEM * GCP_MEM + NETMONTH * GCP_NET + ((ANVIL_MEM * GCP_MEM + ANVIL_CPU * GCP_CPU + GCP_STOR * ANVIL_STOR)* 24*30)
gcpYear = (float(yearCPU)) * GCP_CPU + float(yearMem) * GCP_MEM + REDMONTHCOMP * 12 * GCP_CPU + REDMONTHSTOR *12* GCP_STOR+REDMONTHMEM*12 * GCP_MEM + NETMONTH *12 * GCP_NET + ((ANVIL_MEM * GCP_MEM + ANVIL_CPU * GCP_CPU + GCP_STOR * ANVIL_STOR)* 24*365)
#gcpAll = (float(allCPU)) * GCP_CPU + float(allMem) * GCP_MEM + REDMONTHCOMP * allMonth * GCP_CPU + REDMONTHSTOR * allMonth * GCP_STOR+REDMONTHMEM * allMonth * GCP_MEM + NETMONTH * allMonth * GCP_NET + ((ANVIL_MEM * GCP_MEM + ANVIL_CPU * GCP_CPU + GCP_STOR * ANVIL_STOR)* 24*30*allMonth)

print("GCP Red Day Cost: $" + str(REDMONTHCOMP/30 * GCP_CPU + REDMONTHSTOR/30 * GCP_STOR))
print("GCP day cost: $" + str(gcpDay))
print("GCP week cost: $" + str(gcpWeek))
print("GCP month cost: $" + str(gcpMonth))
print("GCP year cost: $" + str(gcpYear))
#print("GCP all cost: $" + str(gcpAll))

azDay = (float(dayCPU)) * AZ_CPU + float(dayMem) * AZ_MEM + REDMONTHCOMP/30 * AZ_CPU + REDMONTHSTOR/30 * AZ_STOR + REDMONTHMEM/30 * AZ_MEM + NETMONTH/30 * AZ_NET + ((ANVIL_MEM * AZ_MEM + ANVIL_CPU * AZ_CPU + AZ_STOR * ANVIL_STOR)* 24)
azWeek = (float(weekCPU)) * AZ_CPU + float(weekMem) * AZ_MEM + REDMONTHCOMP/4 * AZ_CPU + REDMONTHSTOR/4 * AZ_STOR+REDMONTHMEM/4 * AZ_MEM + NETMONTH/4 * AZ_NET + ((ANVIL_MEM * AZ_MEM + ANVIL_CPU * AZ_CPU + AZ_STOR * ANVIL_STOR)* 24*7)
azMonth = (float(monthCPU)) * AZ_CPU + float(monthMem) * AZ_MEM + REDMONTHCOMP * AZ_CPU + REDMONTHSTOR * AZ_STOR+ REDMONTHMEM * AZ_MEM + NETMONTH * AZ_NET + ((ANVIL_MEM * AZ_MEM + ANVIL_CPU * AZ_CPU + AZ_STOR * ANVIL_STOR)* 24*30)
azYear = (float(yearCPU)) * AZ_CPU + float(yearMem) * AZ_MEM + REDMONTHCOMP * 12 * AZ_CPU + REDMONTHSTOR *12* AZ_STOR+REDMONTHMEM*12 * AZ_MEM + NETMONTH *12 * AZ_NET + ((ANVIL_MEM * AZ_MEM + ANVIL_CPU * AZ_CPU + AZ_STOR * ANVIL_STOR)* 24*365)
#azAll = (float(allCPU)) * AZ_CPU + float(allMem) * AZ_MEM + REDMONTHCOMP * allMonth * AZ_CPU + REDMONTHSTOR * allMonth * AZ_STOR+REDMONTHMEM * allMonth * AZ_MEM + NETMONTH * allMonth * AZ_NET + ((ANVIL_MEM * AZ_MEM + ANVIL_CPU * AZ_CPU + AZ_STOR * ANVIL_STOR)* 24*30*allMonth)

print("AZ day cost: $" + str(azDay))
print("AZ week cost: $" + str(azWeek))
print("AZ month cost: $" + str(azMonth))
print("AZ year cost: $" + str(azYear))
#print("AZ all cost: $" + str(azAll))


doDay = (float(dayCPU)) * DO_CPU + float(dayMem) * DO_MEM + REDMONTHCOMP/30 * DO_CPU + REDMONTHSTOR/30 * DO_STOR + REDMONTHMEM/30 * DO_MEM + NETMONTH/30 * DO_NET + ((ANVIL_MEM * DO_MEM + ANVIL_CPU * DO_CPU + DO_STOR * ANVIL_STOR)* 24)
doWeek = (float(weekCPU)) * DO_CPU + float(weekMem) * DO_MEM + REDMONTHCOMP/4 * DO_CPU + REDMONTHSTOR/4 * DO_STOR+REDMONTHMEM/4 * DO_MEM + NETMONTH/4 * DO_NET + ((ANVIL_MEM * DO_MEM + ANVIL_CPU * DO_CPU + DO_STOR * ANVIL_STOR)* 24*7)
doMonth = (float(monthCPU)) * DO_CPU + float(monthMem) * DO_MEM + REDMONTHCOMP * DO_CPU + REDMONTHSTOR * DO_STOR+ REDMONTHMEM * DO_MEM + NETMONTH * DO_NET + ((ANVIL_MEM * DO_MEM + ANVIL_CPU * DO_CPU + DO_STOR * ANVIL_STOR)* 24*30)
doYear = (float(yearCPU)) * DO_CPU + float(yearMem) * DO_MEM + REDMONTHCOMP * 12 * DO_CPU + REDMONTHSTOR *12* DO_STOR+REDMONTHMEM*12 * DO_MEM + NETMONTH *12 * DO_NET + ((ANVIL_MEM * DO_MEM + ANVIL_CPU * DO_CPU + DO_STOR * ANVIL_STOR)* 24*365)
#doAll = (float(allCPU)) * DO_CPU + float(allMem) * DO_MEM + REDMONTHCOMP * allMonth * DO_CPU + REDMONTHSTOR * allMonth * DO_STOR+REDMONTHMEM * allMonth * DO_MEM + NETMONTH * allMonth * DO_NET + ((ANVIL_MEM * DO_MEM + ANVIL_CPU * DO_CPU + DO_STOR * ANVIL_STOR)* 24*30*allMonth)

print("DO day cost: $" + str(doDay))
print("DO week cost: $" + str(doWeek))
print("DO month cost: $" + str(doMonth))
print("DO year cost: $" + str(doYear))
#print("DO all cost: $" + str(doAll))

ibmDay = (float(dayCPU)) * IBM_CPU + float(dayMem) * IBM_MEM + REDMONTHCOMP/30 * IBM_CPU + REDMONTHSTOR/30 * IBM_STOR + REDMONTHMEM/30 * IBM_MEM + NETMONTH/30 * IBM_NET + ((ANVIL_MEM * IBM_MEM + ANVIL_CPU * IBM_CPU + IBM_STOR * ANVIL_STOR)* 24)
ibmWeek = (float(weekCPU)) * IBM_CPU + float(weekMem) * IBM_MEM + REDMONTHCOMP/4 * IBM_CPU + REDMONTHSTOR/4 * IBM_STOR+REDMONTHMEM/4 * IBM_MEM + NETMONTH/4 * IBM_NET + ((ANVIL_MEM * IBM_MEM + ANVIL_CPU * IBM_CPU + IBM_STOR * ANVIL_STOR)* 24*7)
ibmMonth = (float(monthCPU)) * IBM_CPU + float(monthMem) * IBM_MEM + REDMONTHCOMP * IBM_CPU + REDMONTHSTOR * IBM_STOR+ REDMONTHMEM * IBM_MEM + NETMONTH * IBM_NET + ((ANVIL_MEM * IBM_MEM + ANVIL_CPU * IBM_CPU + IBM_STOR * ANVIL_STOR)* 24*30)
ibmYear = (float(yearCPU)) * IBM_CPU + float(yearMem) * IBM_MEM + REDMONTHCOMP * 12 * IBM_CPU + REDMONTHSTOR *12* IBM_STOR+REDMONTHMEM*12 * IBM_MEM + NETMONTH *12 * IBM_NET + ((ANVIL_MEM * IBM_MEM + ANVIL_CPU * IBM_CPU + IBM_STOR * ANVIL_STOR)* 24*365)
#ibmAll = (float(allCPU)) * IBM_CPU + float(allMem) * IBM_MEM + REDMONTHCOMP * allMonth * IBM_CPU + REDMONTHSTOR * allMonth * IBM_STOR+REDMONTHMEM * allMonth * IBM_MEM + NETMONTH * allMonth * IBM_NET + ((ANVIL_MEM * IBM_MEM + ANVIL_CPU * IBM_CPU + IBM_STOR * ANVIL_STOR)* 24*30*allMonth)

print("IBM day cost: $" + str(ibmDay))
print("IBM week cost: $" + str(ibmWeek))
print("IBM month cost: $" + str(ibmMonth))
print("IBM year cost: $" + str(ibmYear))
#print("IBM all cost: $" + str(ibmAll))


dash.SendEvent('awsDay', {'current': awsDay})
time.sleep(1)
dash.SendEvent('awsMonth', {'current': awsMonth})
time.sleep(1)
dash.SendEvent('awsYear', {'current': awsYear})
time.sleep(1)
dash.SendEvent('gcpDay', {'current': gcpDay})
time.sleep(1)
dash.SendEvent('gcpMonth', {'current': gcpMonth})
time.sleep(1)
dash.SendEvent('gcpYear', {'current': gcpYear})
time.sleep(1)
dash.SendEvent('azDay', {'current': azDay})
time.sleep(1)
dash.SendEvent('azMonth', {'current': azMonth})
time.sleep(1)
dash.SendEvent('azYear', {'current': azYear})
time.sleep(1)
dash.SendEvent('ibmDay', {'current': ibmDay})
time.sleep(1)
dash.SendEvent('ibmMonth', {'current': ibmMonth})
time.sleep(1)
dash.SendEvent('ibmYear', {'current': ibmYear})
time.sleep(1)
dash.SendEvent('doDay', {'current': doDay})
time.sleep(1)
dash.SendEvent('doMonth', {'current': doMonth})
time.sleep(1)
dash.SendEvent('doYear', {'current': doYear})
time.sleep(1)

