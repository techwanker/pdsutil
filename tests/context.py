
import os
import sys

def get_file_path(file_name):
    fdir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(fdir, file_name)
