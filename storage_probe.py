#!/usr/bin/python

import subprocess
import dashing
import os
import time
import sys
import socket
MOUNT = "/lustre"
CLUSTER = socket.gethostname().split(".")[1].capitalize()
from math import log
terabyte = 1073741824

unit_list = zip(['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'], [0, 0, 1, 1, 1, 1])
def sizeof_fmt(num):
    """Human friendly file size"""
    if num > 1:
        exponent = min(int(log(num, 1024)), len(unit_list) - 1)
        quotient = float(num) / 1024**exponent
        unit, num_decimals = unit_list[exponent + 1]
        format_string = '%%.%sf%s' % (num_decimals, unit)
        format_string = format_string % (quotient)
        return format_string
    if num == 0:
        return '0 bytes'
    if num == 1:
        return '1 byte'

def main():
    with open('/home/swanson/cathrine98/dashing-storage/key.txt', 'r') as file:
        auth_key = file.read().strip()

    p = subprocess.Popen(["df", "-P", MOUNT], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdoutdata, stderrdata) = p.communicate()
    for line in stdoutdata.split("\n"):
        split_line = line.split()
        if len(split_line) < 5:
            continue
        if split_line[5] == MOUNT:
            dash = dashing.DashingImport('viz.unl.edu', auth_token = auth_key)
            dashUNO = dashing.DashingImport('viz.unl.edu',port=4000, auth_token = auth_key)
            send_dict = { 'min': 0, 'max': float("%.1f" % (float(split_line[1]) / terabyte)) , 'value': float("%.1f" % (float(split_line[2]) / terabyte)), 'moreinfo': "Capacity: %s" % sizeof_fmt(int(split_line[1])) }
            dash.SendEvent(CLUSTER+'Storage', send_dict)
            dash.SendEvent('HCCAmazonPrice', {CLUSTER.lower()+'Storage': send_dict['value']})
            
            dashUNO.SendEvent(CLUSTER+'Storage', send_dict)
            dashUNO.SendEvent('HCCAmazonPrice', {CLUSTER.lower()+'Storage': send_dict['value']})


    # Send the number of jobs running
    command = "squeue -t R -O numcpus,account -h".split(" ")
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    sum_running_cores = 0
    per_user_cores = {}
    for line in stdout.split("\n"):
        if line == "":
            break
        try:
            (cores, username) = line.split()
            sum_running_cores += int(cores)
            username.strip()
            if username not in per_user_cores:
                per_user_cores[username] = 0
            per_user_cores[username] += int(cores)
        except:
            print "Error parsing line: %s" % line
            pass
        
    if os.path.isfile('dashing.txt') and os.access('dashing.txt', os.R_OK):
        date = os.path.getmtime('dashing.txt')
        with open('dashing.txt', 'r') as file:
            last_running_cores = int(file.read())
    else:
        print "Error reading from dashing.txt"
        date = time.time() + 3600
        last_running_cores = sum_running_cores
    with open('dashing.txt', 'w') as file:
        file.write(str(sum_running_cores))
    date = time.strftime('%m-%d-%Y %H:%M:%S', time.localtime(date))
    dash.SendEvent(CLUSTER+'Running', {'current': sum_running_cores, 'last': last_running_cores, 'last_period': date})
    dash.SendEvent('HCCAmazonPrice', {CLUSTER+'Cores': sum_running_cores})
    
    dashUNO.SendEvent(CLUSTER+'Running', {'current': sum_running_cores, 'last': last_running_cores, 'last_period': date})
    dashUNO.SendEvent('HCCAmazonPrice', {CLUSTER+'Cores': sum_running_cores})
    
    # send number of completed jobs
    current_time = time.strftime('%m/%d/%y-%H:%M:%S', time.localtime(time.time()))
    start_time = time.strftime('%m/%d/%y', time.localtime(time.time()))
    command = "sacct -a -E " + current_time + " -S " + start_time + " -s CD -o JobID -X"
    command = command.split(" ")
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    jobs_completed = len(stdout.split())
    path = '/common/swanson/cathrine98/.dashing/'
    files = ['crane_jobs.txt', 'tusker_jobs.txt', 'sandhills_jobs.txt']
    filename = path + files[0]
    with open(filename, 'w') as file:
        file.write(str(jobs_completed))
    total_jobs = 0
    for filename in files:
        filename = path + filename
        if os.path.isfile(filename) and os.access(filename, os.R_OK):
            with open(filename, 'r') as file:
                total_jobs += int(file.read())
        else:
            print "Error reading from %s" % filename
    
    dash.SendEvent('JobsCompleted', {'current': total_jobs})
    
    dashUNO.SendEvent('JobsCompleted', {'current': total_jobs})


    #Send number of CPU Hours for Today
    command = "sacct -a -o CPUTimeRaw -n -T"
    command = command.split(" ")
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    stdout = map(int, stdout.split())
    hours_completed = sum(stdout)/3600
    files = ['crane_hours.txt', 'tusker_hours.txt', 'sandhills_hours.txt']
    filename = path + files[0]
    with open(filename, 'w') as file:
        file.write(str(hours_completed))
    total_hours = 0
    for filename in files:
        filename = path + filename
        if os.path.isfile(filename) and os.access(filename, os.R_OK):
            with open(filename, 'r') as file:
                total_hours += int(file.read())
        else:
            print "Error reading from %s" % filename


    dash.SendEvent('HoursToday', {'current': total_hours})
    
    dashUNO.SendEvent('HoursToday', {'current': total_hours})



if __name__ == "__main__":
    main()

