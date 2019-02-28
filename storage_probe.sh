#!/bin/bash
source /util/opt/lmod/lmod/init/profile
module load anaconda
source activate dashing
python /util/admin/dashing-storage/storage_probe.py
source deactivate dashing
module unload anaconda
