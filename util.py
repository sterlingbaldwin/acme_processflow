from math import floor

import sys
import traceback
import re
import os

def print_debug(e):
    print '1', e.__doc__
    print '2', sys.exc_info()
    print '3', sys.exc_info()[0]
    print '4', sys.exc_info()[1]
    print '5', traceback.tb_lineno(sys.exc_info()[2])
    ex_type, ex, tb = sys.exc_info()
    print '6', traceback.print_tb(tb)


class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_message(message, status='error'):
    if status == 'error':
        print colors.FAIL + '[-] ' + colors.ENDC + colors.BOLD + str(message) + colors.ENDC
    elif status == 'ok':
        print colors.OKGREEN + '[+] ' + colors.ENDC + str(message)

def filename_to_file_list_key(filename, pattern):
    """
    Takes a filename and returns the key for the file_list
    """
    # these offsets need to change if the output_pattern changes. This is unavoidable given the escape characters
    start_offset = 8
    end_offset = 12
    month_start_offset = end_offset + 1
    month_end_offset = month_start_offset + 2
    index = re.search(pattern, filename).start()
    year = int(filename[index + start_offset: index + end_offset])
    month = int(filename[index + month_start_offset: index + month_end_offset])
    key = "{year}-{month}".format(year=year, month=month)
    return key

def filename_to_year_set(filename, pattern, freq):
    """
    Takes a filename and returns the year_set that the file belongs to
    """
    # these offsets need to change if the output_pattern changes. This is unavoidable given the escape characters
    pattern_format = 'YYYY-MM'
    file_format = '.nc'
    if not filename.endswith(file_format):
        print_message('unable to find year set, unexpected file format')
        return 0
    file_date = filename[ -(len(pattern_format) + len(file_format)): - len(file_format)]
    year = int(file_date[:4])
    if year % freq == 0:
        return int(year / freq)
    else:
        return int(year / freq) + 1

def create_symlink_dir(src_dir, src_list, dst):
    """
    Create a directory, and fill it with symlinks to all the items in the source directory
    """
    if not os.path.exists(dst):
        os.makedirs(dst)
    for f in src_list:
        source = os.path.join(src_dir, f)
        destination = os.path.join(dst, f)
        os.symlink(source, destination)
