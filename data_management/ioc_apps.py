# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import json
import logging
import threading

from utilities.file_saver import FileSaver

log = logging.getLogger("PVMLogger")


class IocApps(object):

    def __init__(self, my_hostname):
        self._pv_dict = {}
        self._id_to_path_dict = {}
        self._lock = threading.Lock()
        self._my_hostname = my_hostname

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()

    def get_data(self, pv_name):

        result = []
        try:
            self.lock()
            path_ids = self._pv_dict.get(pv_name, [])
            path_ids = list(set(path_ids))

            for path_id in path_ids:
                cmd_file = self._id_to_path_dict.get(path_id)
                result.append(cmd_file)

            result.sort()

        finally:
            self.unlock()

        return result

    def load(self):

        saver = FileSaver(prefix="find_pv")

        file_name = saver.get_newest()

        if not file_name:
            log.error("No datafile detected")
            return

        opened = False
        try:
            f = open(file_name)
            opened = True
            data = json.load(f)
            log.debug("Loaded File: '%s'", file_name)

        except Exception as err:
            log.debug(
                "Exception loading IOCAPPS data: %s",
                str(err),
            )
            data = None
        finally:
            if opened:
                f.close()

        if data:
            self._pv_dict = data.get("PVS")
            log.debug(
                "IOCAPPS loaded %d PVs",
                len(self._pv_dict),
            )

            temp = data.get("DATA")
            for k, v in temp.items():
                key = int(k)
                self._id_to_path_dict[key] = v

            log.debug(
                "IOCAPPS loaded %d paths",
                len(self._id_to_path_dict),
            )
