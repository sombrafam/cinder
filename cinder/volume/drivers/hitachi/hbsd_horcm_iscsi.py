# Copyright (C) 2015, Hitachi, Ltd.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
"""iSCSI Cinder volume driver for HORCM interface."""

import re

from oslo_log import log as logging

from cinder import exception

from cinder.volume.drivers.hitachi import hbsd_horcm as horcm
from cinder.volume.drivers.hitachi import hbsd_utils as utils

_ISCSI_LINUX_MODE_OPTS = ['-host_mode', 'LINUX']
_ISCSI_HOST_MODE_OPT = '-host_mode_opt'
_ISCSI_HMO_REPORT_FULL_PORTAL = 83
_ISCSI_HMO_DISABLE_IO = 91
_ISCSI_TARGETS_PATTERN = re.compile(
    (r"^CL\w-\w+ +(?P<gid>\d+) +%s(?!pair00 )\S* +(?P<iqn>\S+) +"
     r"\w+ +\w +\d+ ") % utils.TARGET_PREFIX, re.M)
_ISCSI_PORT_PATTERN = re.compile(
    r"^(CL\w-\w)\w* +ISCSI +TAR +\w+ +\w+ +\w +\w+ +Y ", re.M)
_ISCSI_IPV4_ADDR_PATTERN = re.compile(
    r"^IPV4_ADDR +: +(?P<ipv4_addr>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$", re.M)
_ISCSI_TCP_PORT_PATTERN = re.compile(
    r'^TCP_PORT\ +:\ +(?P<tcp_port>\d+)$', re.M)

LOG = logging.getLogger(__name__)


class HBSDHORCMISCSI(horcm.HBSDHORCM):
    """iSCSI Class for HORCM interface."""

    def connect_storage(self):
        target_ports = self.conf.hitachi_target_ports
        compute_target_ports = self.conf.hitachi_compute_target_ports
        pair_target_ports = self.conf.hitachi_horcm_pair_target_ports

        super(HBSDHORCMISCSI, self).connect_storage()
        result = self.run_raidcom('get', 'port')
        for port in _ISCSI_PORT_PATTERN.findall(result[1]):
            if (target_ports and port in target_ports and
                    self._set_target_portal(port)):
                self.storage_info['ports'].append(port)
            if (compute_target_ports and port in compute_target_ports and
                    (port in self.storage_info['portals'] or
                     self._set_target_portal(port))):
                self.storage_info['compute_ports'].append(port)
            if pair_target_ports and port in pair_target_ports:
                self.storage_info['pair_ports'].append(port)

        self.check_ports_info()
        if pair_target_ports and not self.storage_info['pair_ports']:
            msg = utils.output_log(650, resource="Pair target ports")
            raise exception.HBSDError(data=msg)
        LOG.debug(
            'Setting pair_target_ports: %s',
            self.storage_info['pair_ports'])
        LOG.debug(
            'Setting portals: %s', self.storage_info['portals'])

    def _set_target_portal(self, port):
        ipv4_addr = None
        tcp_port = None
        result = self.run_raidcom(
            'get', 'port', '-port', port, '-key', 'opt')
        match = _ISCSI_IPV4_ADDR_PATTERN.search(result[1])
        if match:
            ipv4_addr = match.group('ipv4_addr')
        match = _ISCSI_TCP_PORT_PATTERN.search(result[1])
        if match:
            tcp_port = match.group('tcp_port')
        if not ipv4_addr or not tcp_port:
            return False
        self.storage_info['portals'][port] = ':'.join(
            [ipv4_addr, tcp_port])
        return True

    def create_target_to_storage(self, port, target_name, hba_ids):
        args = [
            'add', 'host_grp', '-port', port, '-host_grp_name', target_name]
        if hba_ids:
            args.extend(['-iscsi_name', hba_ids + utils.TARGET_IQN_SUFFIX])

        result = self.run_raidcom(*args)

        return horcm.find_value(result[1], 'gid')

    def set_hba_ids(self, port, gid, hba_ids):
        self.run_raidcom(
            'add', 'hba_iscsi', '-port', '-'.join([port, gid]),
            '-hba_iscsi_name', hba_ids)

    def set_target_mode(self, port, gid):
        hostmode_setting = _ISCSI_LINUX_MODE_OPTS
        hostmode_setting.append(_ISCSI_HOST_MODE_OPT)
        hostmode_setting.append(_ISCSI_HMO_REPORT_FULL_PORTAL)
        if self.conf.hitachi_horcm_disable_io_wait:
            hostmode_setting.append(_ISCSI_HMO_DISABLE_IO)
        self.run_raidcom(
            'modify', 'host_grp', '-port',
            '-'.join([port, gid]), *hostmode_setting)

    def find_targets_from_storage(self, targets, connector, target_ports):
        nr_not_found = 0
        target_name = '-'.join([utils.DRIVER_PREFIX, connector['ip']])
        success_code = horcm.HORCM_EXIT_CODE.union([horcm.EX_ENOOBJ])
        iqn = self.get_hba_ids_from_connector(connector)
        iqn_pattern = re.compile(
            r'^CL\w-\w+ +\d+ +\S+ +%s ' % iqn, re.M)
        iscsi_target_pattern = re.compile(
            r"^CL\w-\w+ +\d+ +%s +(?P<iqn>\S+) +\w+ +\w +\d+ " % target_name,
            re.M)

        for port in target_ports:
            targets['info'][port] = False

            result = self.run_raidcom(
                'get', 'hba_iscsi', '-port', port, target_name,
                success_code=success_code)
            if iqn_pattern.search(result[1]):
                gid = result[1].splitlines()[1].split()[1]
                targets['info'][port] = True
                targets['list'].append((port, gid))

                if (port, gid) not in self.storage_info['iqns']:
                    result = self.run_raidcom('get', 'host_grp', '-port', port)
                    match = iscsi_target_pattern.search(result[1])
                    if match:
                        self.storage_info['iqns'][(port, gid)] = match.group(
                            'iqn')
                continue
            if self.conf.hitachi_horcm_name_only_discovery:
                nr_not_found += 1
                continue

            result = self.run_raidcom(
                'get', 'host_grp', '-port', port)
            for gid, iqn in _ISCSI_TARGETS_PATTERN.findall(result[1]):
                result = self.run_raidcom(
                    'get', 'hba_iscsi', '-port', '-'.join([port, gid]))
                if iqn_pattern.search(result[1]):
                    targets['info'][port] = True
                    targets['list'].append((port, gid))
                    self.storage_info['iqns'][(port, gid)] = iqn
                    break
            else:
                nr_not_found += 1

        return nr_not_found

    def get_properties_iscsi(self, targets, multipath):
        if not multipath:
            target_list = targets['list'][:1]
        else:
            target_list = targets['list'][:]

        for target in target_list:
            if target not in self.storage_info['iqns']:
                port, gid = target
                result = self.run_raidcom('get', 'host_grp', '-port', port)
                match = re.search(
                    r"^CL\w-\w+ +%s +\S+ +(?P<iqn>\S+) +\w+ +\w +\d+ " % gid,
                    result[1], re.M)
                if not match:
                    msg = utils.output_log(650, resource='Target IQN')
                    raise exception.HBSDError(data=msg)
                self.storage_info['iqns'][target] = match.group('iqn')
                LOG.debug(
                    'Found target iqn of host group. (port: %(port)s, '
                    'gid: %(gid)s, target iqn: %(iqn)s)',
                    {'port': port, 'gid': gid, 'iqn': match.group('iqn')})
        return super(HBSDHORCMISCSI, self).get_properties_iscsi(
            targets, multipath)
