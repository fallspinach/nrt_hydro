''' Some utility functions

Usage:
    imported by other scripts only
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

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

## find the last file
def find_last_time2(glob_pattern, file_pattern0, file_pattern1, sep):

    last_time0 = datetime(1000, 1, 1, 0, 0, 0, 0, pytz.utc)
    last_time1 = datetime(1000, 1, 1, 0, 0, 0, 0, pytz.utc)

    for dfile in glob(glob_pattern):
        ftime0 = datetime.strptime(os.path.basename(dfile).split(sep)[0], file_pattern0)
        ftime1 = datetime.strptime(os.path.basename(dfile).split(sep)[1], file_pattern1)
        ftime0 = ftime0.replace(tzinfo=pytz.utc)
        ftime1 = ftime1.replace(tzinfo=pytz.utc)
        if ftime0>last_time0:
            last_time0 = ftime0
            last_time1 = ftime1
        elif ftime0==last_time0 and ftime1>last_time1:
            last_time1 = ftime1

    return last_time0, last_time1

## Written by ChatGPT, with slight modifications
def replace_brackets(file_name, replacements, bracket=True):
    """
    Replaces strings (bracketed in <>) in a file based on the keys and values in the replacements dictionary.

    :param file_name: The name of the file to modify.
    :param replacements: A dictionary where each key is a string (without <>) to be replaced, 
                         and the corresponding value is the string to replace it with.
    """
    # Read the file content
    with open(file_name, 'r') as file:
        content = file.read()

    # Replace the strings in the content
    for key, value in replacements.items():
        if bracket:
            content = content.replace(f'<{key}>', value)
        else:
            content = content.replace(key, value)

    # Write the modified content back to the file
    with open(file_name, 'w') as file:
        file.write(content)