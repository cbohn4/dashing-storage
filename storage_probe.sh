#!/bin/bash
module load anaconda
source activate dashing
/usr/bin/python /util/admin/dashing-storage/storage_probe.py
source deactivate dashing
module unload anaconda
