#!/bin/bash
%%ACCOUNT%%
#SBATCH -N 1
#SBATCH --workdir %%WORKDIR%%
#SBATCH -n 32
#SBATCH -t 0-05:00
#SBATCH -o %%CONSOLE_OUTPUT%%

python << END
import os
import sys
input_files = %%FILES%%
test_archive_path = os.path.join(
    '%%INPUT_PATH%%',
    '%%EXPERIMENT%%',
    'run')
if not os.path.exists(test_archive_path):
    os.makedirs(test_archive_path)

for file in input_files:
    if not os.path.exists(file):
        sys.exit(1)
    head, tail = os.path.split(file)
    dst = os.path.join(test_archive_path, tail)
    if not os.path.exists(dst):
        os.symlink(file, dst)
END

export OMP_NUM_THREADS=2
sh %%SCRIPT_PATH%%

