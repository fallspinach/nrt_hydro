from datetime import datetime
import os, pytz, yaml
from glob import glob
import socket

fconfig = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/config.yaml'
cluster = socket.getfqdn().split('.')[-3].split('-')[0]
with open(fconfig, 'r') as f:
    config_all = yaml.safe_load(f)
    config     = config_all[cluster]

## find the last file
def find_last_time(glob_pattern, file_pattern):

    last_time = datetime(1000, 1, 1, 0, 0, 0, 0, pytz.utc)

    for dfile in glob(glob_pattern):
        ftime = datetime.strptime(os.path.basename(dfile), file_pattern)
        ftime = ftime.replace(tzinfo=pytz.utc)
        if ftime>last_time:
            last_time = ftime

    return last_time

## find the first file
def find_first_time(glob_pattern, file_pattern):

    first_time = datetime(3000, 1, 1, 0, 0, 0, 0, pytz.utc)

    for dfile in glob(glob_pattern):
        ftime = datetime.strptime(os.path.basename(dfile), file_pattern)
        ftime = ftime.replace(tzinfo=pytz.utc)
        if ftime<first_time:
            first_time = ftime

    return first_time

## find the last dir
def find_last_time_dir(glob_pattern, file_pattern):

    last_time = datetime(1000, 1, 1, 0, 0, 0, 0, pytz.utc)

    for dfile in glob(glob_pattern):
        ftime = datetime.strptime(dfile, file_pattern)
        ftime = ftime.replace(tzinfo=pytz.utc)
        if ftime>last_time:
            last_time = ftime

    return last_time


