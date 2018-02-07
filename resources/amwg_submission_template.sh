#!/bin/bash
%%ACCOUNT%%
#SBATCH -N 1
#SBATCH -o %%CONSOLE_OUT%%
#SBATCH -n 16
#SBATCH -t 0-05:00
#SBATCH --oversubscribe

python << END
import os
import sys
import re
src_dir = '%%SRC_DIR%%'
src_list = %%SRC_LIST%%
dst = '%%DST%%'

if not src_list:
    sys.exit(1)
if not os.path.exists(dst):
    os.makedirs(dst)
for src_file in src_list:
    if not src_file:
        continue
    source = os.path.join(src_dir, src_file)
    destination = os.path.join(dst, src_file)
    if os.path.lexists(destination):
        continue
    try:
        os.symlink(source, destination)
    except Exception as e:
        msg = format_debug(e)
        logging.error(msg)

# Rename the files to the format amwg expects
for item in os.listdir(dst):
    search = re.search(r'_\d{6}_', item)
    if not search:
        continue
    index = search.start()
    os.rename(
        os.path.join(dst, item),
        os.path.join(dst, item[:index] + '_climo.nc'))
END

export NCARG_ROOT='/usr/local/src/NCL-6.3.0'
csh %%RUN_AMWG_PATH%%