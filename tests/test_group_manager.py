#---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
#---------------------------------------------------------------------
import os, sys

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from data_management.dataman import DataManager
from data_management.group_manager import GroupManager

sys.path.insert(1, os.path.join(sys.path[0], '..'))

def test1():

    print("called")
    # dataman = DataManager('opi2031-006', 'iocmondb_devel', 'epicsgw-51-2', None)
    dataman = DataManager('opi2031-006', 'iocmondb_devel', 'epicsgw-51-2', 'svp-pvmon02-051', None)
    groupman = GroupManager(dataman)

    print("calling load")
    groupman.load()

if __name__ == "__main__":

    test1()
