# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import copy
import hashlib
import logging
import threading
import time

from definitions import APP_STATE, GROUP, IOC_STATE, KEY

log = logging.getLogger("PVMLogger")

SQL_INSERT_GROUP = (
    "insert into groups (id, name, create_time, update_time, status) "
    + "values (%s, %s, %s, %s, %s)"
)

SQL_INSERT_GROUP_MEMBERSHIP = (
    "insert into group_memberships (group_id, ioc_id, create_time, update_time,"
    " status) values (%s, %s, %s, %s, %s)"
)


class GroupManager(object):

    def __init__(self, dataman):

        self._iocman = None
        self._dataman = dataman

        # The member list is a list of all IOCs that are in a group
        self._member_list = []
        self._memberships_by_group = {}
        self._memberships_by_ioc = {}
        self._group_dict = {}

        self._lock = threading.Lock()
        self.locked = False

    def set_iocman(self, iocman):
        self._iocman = iocman

    def membership_add(self, group_id, ioc_id):

        # Sanity check
        if not isinstance(group_id, int):
            raise ValueError("group ID not an int")

        # Sanity check
        if not isinstance(ioc_id, int):
            raise ValueError("IOC ID not an int")

        self._lock.acquire()
        try:
            data = self._group_dict.get(group_id, {})
            ioc_list = data.get(KEY.IOC_LIST, [])
            ioc_list.append(ioc_id)

            ioc_list = list(set(ioc_list))
            data[KEY.IOC_LIST] = ioc_list

        finally:
            self._lock.release()

        # Must add the memvership in the SQL database
        cur_time = int(time.time())
        values = (group_id, ioc_id, cur_time, cur_time, 0)
        self._dataman._sql.execute(SQL_INSERT_GROUP_MEMBERSHIP, values)

    def membership_remove(self, group_id, ioc_id):

        # Sanity check
        if not isinstance(group_id, int):
            raise ValueError("group key not an int")

        # Sanity check
        if not isinstance(ioc_id, int):
            raise ValueError("group key not an int")

        self._lock.acquire()
        try:
            data = self._group_dict.get(group_id, {})
            ioc_list = data.get(KEY.IOC_LIST, [])

            try:
                ioc_list.remove(ioc_id)
            except:
                pass

            ioc_list = list(set(ioc_list))
            data[KEY.IOC_LIST] = ioc_list
        finally:
            self._lock.release()

        cmd = (
            "delete from group_memberships where group_id=%d and ioc_id=%d"
            % (
                group_id,
                ioc_id,
            )
        )
        self._dataman._sql.execute(cmd, None)

    def get_group_dict(self, ioc_dict=None):
        """
        Return sorted list of groups, with "permanent" groups first
        """
        try:
            self._lock.acquire()
            result = copy.deepcopy(self._group_dict)
            return result
        finally:
            self._lock.release()

    def get_group_list(self, counts=False):

        log.debug("called; counts: %r", counts)

        group_list_perm = []
        group_list_temp = []

        try:
            self._lock.acquire()

            for group_id, group_data in self._group_dict.items():
                group_name = group_data.get(KEY.NAME)
                group_name_sort = group_name.lower()
                if counts:
                    ioc_list = self.get_memberships_by_group(
                        group_id, have_lock=True
                    )
                    count = len(ioc_list)

                else:
                    count = 0

                if group_id < 1000:
                    group_list_perm.append(
                        (group_name_sort, (group_id, group_name, count))
                    )
                else:
                    group_list_temp.append(
                        (group_name_sort, (group_id, group_name, count))
                    )

        finally:
            self._lock.release()

        group_list_perm.sort()
        group_list_temp.sort()
        group_list = group_list_perm + group_list_temp
        sorted_list = [item[1] for item in group_list]
        return sorted_list

    def get_memberships_by_ioc(self, ioc_id, sort=True, not_member=False):
        """
        This returns a list ouf (name, group_id)
        """

        self._lock.acquire()
        result = []
        try:
            for group_id, group_data in self._group_dict.items():

                if group_id < 1000:
                    continue

                members = group_data.get(KEY.IOC_LIST)
                group_name = group_data.get(KEY.NAME)

                if not_member:
                    if ioc_id not in members:
                        result.append((group_name, group_id))

                else:

                    if ioc_id in members:
                        result.append((group_name, group_id))

            if sort:
                temp = [
                    (item[0].lower(), (item[0], item[1])) for item in result
                ]
                temp.sort()
                result = [item[1] for item in temp]

            return copy.deepcopy(result)

        finally:
            self._lock.release()

    def get_memberships_by_group(self, group_id, have_lock=False):

        log.debug(
            "called; %s (%d)",
            GROUP.name(group_id),
            group_id,
        )

        ioc_list = []
        if group_id == GROUP.ACTIVE:
            # This is a special case.  Show every IOC with:
            # 1) an expected beacon
            # 2) expected app
            # 3) An active beacon

            # 1) All apps with an expected beacon port
            cmd = "select ioc_id from expected_beacon_ports"
            sql_result = self._dataman._sql.select(cmd)
            if sql_result is not None:
                ioc_list = [item[0] for item in sql_result]

            # 2) All IOCs with an expected app
            cmd = "select ioc_id from apps where state in (%d, %d)" % (
                APP_STATE.CRITICAL,
                APP_STATE.NOT_CRITICAL,
            )
            sql_result = self._dataman._sql.select(cmd)
            if sql_result is not None:
                temp = [item[0] for item in sql_result]
                ioc_list.extend(temp)

            # 3) All IOCs with an active beacon
            beacons = self._dataman.get_beacons(skip_lost=True)
            ioc_ids = [item[0] for item in beacons]
            ioc_list.extend(ioc_ids)
            ioc_list = list(set(ioc_list))

        elif group_id == GROUP.ALL:
            ioc_list = self._dataman.get_ioc_id_list()

        elif group_id == GROUP.UNNASSIGNED:
            ioc_list = []

            all_iocs = self._dataman.get_ioc_id_list()
            for ioc_id in all_iocs:
                if ioc_id in self._member_list:
                    continue
                ioc_list.append(ioc_id)
        else:
            try:
                if not have_lock:
                    self._lock.acquire()
                group_data = self._group_dict.get(group_id)
                if group_data is None:
                    log.error(
                        "No group data for group: %d",
                        group_id,
                    )
                    ioc_list = []
                else:
                    ioc_list = copy.deepcopy(group_data.get(KEY.IOC_LIST, []))

            finally:
                if not have_lock:
                    self._lock.release()

        return ioc_list

    def _get_ioc_list_unassigned(self, ioc_dict, locked=False):

        unassigned_list = []
        for key in ioc_dict.keys():
            groups = self.get_memberships_by_group(key, locked=locked)
            if len(groups) == 0:
                unassigned_list.append(key)

        return unassigned_list

    def _get_ioc_list_state(self, ioc_dict, state, locked=False):

        result = []
        for ioc_key, data in ioc_dict.items():
            # Note that IOCs are in "ACTIVE" mode by default.
            if data.get(KEY.STATE, IOC_STATE.ACTIVE) == state:
                result.append(ioc_key)

        return result

    def _get_ioc_list_permanent_group(self, group_key, locked=False):

        # Sanity check
        if not isinstance(group_key, int):
            raise ValueError("group key not an int")

        if group_key == 0:
            return self._iocman.get_id_list()

        ioc_dict = self._iocman.get_state_dict()

        if group_key == 1:
            return self._get_ioc_list_unassigned(ioc_dict, locked=locked)
        elif group_key == 2:
            return self._get_ioc_list_state(
                ioc_dict, IOC_STATE.ACTIVE, locked=locked
            )
        elif group_key == 3:
            return self._get_ioc_list_state(
                ioc_dict, IOC_STATE.MAINTENANCE, locked=locked
            )
        elif group_key == 4:
            return self._get_ioc_list_state(
                ioc_dict, IOC_STATE.TEST, locked=locked
            )
        elif group_key == 5:
            return self._get_ioc_list_state(
                ioc_dict, IOC_STATE.DECOMISSIONED, locked=locked
            )

        raise ValueError("Don't know how to handle group_key: %d" % group_key)

    def get_group_name(self, group_id):

        # Sanity check
        if not isinstance(group_id, int):
            raise ValueError("group key not an int")

        try:
            self._lock.acquire()
            group_data = self._group_dict.get(group_id, {})
            group_name = group_data.get(
                KEY.NAME, "Unknown group (ID: %r)" % group_id
            )
            return group_name

        finally:
            self._lock.release()

    def delete(self, group_id):

        log.debug("called; group_id: %d", group_id)

        # Sanity check
        if not isinstance(group_id, int):
            raise ValueError("group id not an int")

        # Must delete the entry from the database
        cmd = "delete from groups where id=%d" % group_id
        self._dataman._sql.execute(cmd, None)

        # Delete the memberships from the database
        cmd = "delete from group_memberships where group_id=%d" % group_id
        self._dataman._sql.execute(cmd, None)

        self._lock.acquire()
        try:
            del self._group_dict[group_id]

        finally:
            self._lock.release()

    def create(self, group_name):

        log.debug("called; group_name: %s", group_name)

        group_name = group_name.strip()

        if not group_name:
            return

        group_name = str(group_name)
        hash_name = group_name.encode("utf-8")

        # Why did I do this?
        group_id = int(hashlib.md5(hash_name).hexdigest()[:9], base=16)

        # See if this group already exists
        cmd = "select name from groups where id=%d" % group_id
        sql_result = self._dataman._sql.select(cmd)

        if sql_result:
            item = sql_result[0]
            if item[0] == group_name:
                raise ValueError("Group %s already exists" % group_name)

        cur_time = int(time.time())
        values = (group_id, group_name, cur_time, cur_time, 0)
        self._dataman._sql.execute(SQL_INSERT_GROUP, values)

        # Add group to in-memory cache
        try:
            self._lock.acquire()
            self._group_dict[group_id] = {
                KEY.NAME: group_name,
                KEY.IOC_LIST: [],
            }

        finally:
            self._lock.release()

        return group_id

    def load(self):
        log.debug("called")

        self.load_groups()
        self.load_memberships()

        # Due to legacy data there can be items in the expected beacon ports and
        # active apps, but there is no corresponding IOC in the database, These
        # can be added if missing.
        active_iocs = self.get_memberships_by_group(GROUP.ACTIVE)

        # Also there can be group members that are not in the database
        all_iocs = active_iocs + self._member_list
        all_iocs = list(set(all_iocs))

        # There is a problem in which there are memberships but the
        # ioc does not exist in the ioc database.
        # Or there are expected apps/ports
        # This is a result of importing legacy data I think.  Just do an update
        # on all expected IOCs, this should add them to the IOC database

        orig_len = len(all_iocs)
        unique = set(all_iocs)
        unique_len = len(unique)

        log.info(
            "\n%s\nPROCESSING ALL IOCS!!!!!!!!!!!!!!!!!!!!!!!!!! %d %d",
            "*" * 80,
            orig_len,
            unique_len,
        )

        for ioc_id in unique:
            self._iocman.update_ioc(ioc_id)

    def load_memberships(self):

        cmd = "select group_id, ioc_id from group_memberships"
        sql_result = self._dataman._sql.select(cmd)

        try:
            self._lock.acquire()

            self._memberships_by_group = {}
            self._member_list = []

            for item in sql_result:
                group_id = int(item[0])
                ioc_id = int(item[1])

                group_data = self._group_dict.get(group_id)
                if group_data is None:
                    log.warning("Group %d not found", group_id)
                    continue

                member_list = group_data.get(KEY.IOC_LIST, [])
                member_list.append(ioc_id)
                group_data[KEY.IOC_LIST] = list(set(member_list))
                self._group_dict[group_id] = group_data

                groups = self._memberships_by_ioc.get(ioc_id, [])
                groups.append(group_id)
                groups = list(set(groups))
                self._memberships_by_ioc[ioc_id] = groups
                self._member_list.append(ioc_id)

            self._member_list = list(set(self._member_list))

        finally:
            self._lock.release()

    def load_groups(self):

        cmd = "select id,name,status from groups"
        sql_result = self._dataman._sql.select(cmd)

        try:
            self._lock.acquire()
            self._group_dict = {}
            for item in sql_result:
                group_id = int(item[0])
                name = item[1]
                status = item[2]

                data = {KEY.NAME: name, KEY.STATUS: status, KEY.IOC_LIST: []}

                self._group_dict[group_id] = data
                log.debug(
                    "Loading group %s (%d)",
                    name,
                    group_id,
                )

        finally:
            self._lock.release()
