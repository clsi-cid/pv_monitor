# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import datetime
import logging
import os
import socket

log = logging.getLogger("PVMLogger")

SAVE_FILE_PATTERN = "_####_##_##-##_##_##."


class FileSaver(object):

    def __init__(self, path=None, prefix="FileSaver", keep_last=5):

        self.path = os.path.join(
            os.curdir, "data", self.get_base_hostname(), "json"
        )

        os.makedirs(self.path, exist_ok=True)
        
        self.prefix = prefix
        self.keep_last = keep_last

        log.debug("FILE_SAVER: PATH:   '%s'", self.path)
        log.debug("FILE_SAVER: PREFIX: '%s'", self.prefix)

    def get_base_hostname(self) -> str:
        """
        Return the first part of the current hostname to be used as part of a
        file name.
        Ideally: IOC-00001.company.com -> IOC-00001
        """
        hostname = socket.gethostname().lower()
        return hostname.split(".")[0]

    def save(self, json_string):

        t = datetime.datetime.now()

        filename = "%s_%4d_%02d_%02d-%02d_%02d_%02d.json" % (
            self.prefix,
            t.year,
            t.month,
            t.day,
            t.hour,
            t.minute,
            t.second,
        )

        path = os.path.join(self.path, filename)

        log.debug("path: %s", path)

        f = open(path, "w")
        f.write(json_string)
        f.close()
        self.purge()

    def get_newest(self):

        files = self.get_matching_files()

        if len(files) == 0:
            return

        return os.path.join(self.path, files[0])

    def purge(self):
        files = self.get_matching_files()

        purge = files[self.keep_last :]
        for file in purge:
            log.debug("purge: %s", file)
            os.remove(os.path.join(self.path, file))

    def get_matching_files(self):

        log.debug("path: %s prefix: %s", self.path, self.prefix)

        files = [f for f in os.listdir(self.path)]

        matching = []
        for name in files:
            file = os.path.join(self.path, name)
            if not os.path.isfile(file):
                continue

            if not name.startswith("%s_" % self.prefix):
                continue

            if not name.endswith(".json"):
                continue

            matched = False
            p = len(self.prefix)

            log.debug("Consider file: %r", name)

            for i, c in enumerate(name[p:]):

                try:
                    expect = SAVE_FILE_PATTERN[i]

                except Exception as err:
                    log.error(
                        "exception parsing file name %s: %r",
                        name,
                        err,
                    )
                    break

                if expect == "#" and c.isdigit():
                    continue

                elif expect == "." and c == expect:
                    matched = True
                    break

                elif c == expect:
                    continue

            if not matched:
                continue

            matching.append(name)

        matching = sorted(matching)
        return [i for i in reversed(matching)]
