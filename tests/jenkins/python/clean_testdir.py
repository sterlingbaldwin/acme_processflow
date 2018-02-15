import sys
import os
import time
import argparse
from Util import *

parser = argparse.ArgumentParser(description="install conda",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument("-d", "--testdir",
                    help="parent testdir where old dated test subdirectories are to be cleaned up")

parser.add_argument("-n", "--ndays",
                    action="store", type=int,
                    help="number of days, test directories older than specified <ndays> will be removed")

args = parser.parse_args()

testdir = args.testdir
ndays = args.ndays

if os.path.isdir(testdir) == False:
    print('ERROR, test dir ' + testdir + ' does not exist')
    sys.exit(FAILURE)

seconds = ndays * 3600

now = time.time()
for a_file in os.listdir(testdir):
    the_file = os.path.join(testdir, a_file)
    if os.stat(the_file).st_mtime < (now - seconds):
        print("FOUND...directory: " + the_file)
        #os.system("rm -rf " + the_file)

    
