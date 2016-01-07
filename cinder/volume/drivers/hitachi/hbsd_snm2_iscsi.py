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
"""iSCSI Cinder volume driver for SNM2 interface."""

import re

from oslo_log import log as logging

from cinder import exception

from cinder.volume.drivers.hitachi import hbsd_snm2 as snm2
from cinder.volume.drivers.hitachi import hbsd_utils as utils

LOG = logging.getLogger(__name__)


class HBSDSNM2ISCSI(snm2.HBSDSNM2):
    """iSCSI Class for SNM2 interface."""

    def connect_storage(self):
        target_ports = self.conf.hitachi_target_ports
        compute_target_ports = self.conf.hitachi_compute_target_ports

        super(HBSDSNM2ISCSI, self).connect_storage()
        stdout = self.run_snm2_refer('autargetini')
        for port, ipv4_addr, tcp_port in self._get_iscsi_info():
            if not re.search(
                    r'^Port +%s +Target +Security +ON$' % port,
                    stdout, re.M):
                continue

            if (target_ports and port in target_ports and
                    self._set_target_portal(port, ipv4_addr, tcp_port)):
                self.storage_info['ports'].append(port)
            if (compute_target_ports and port in compute_target_ports and
                    (port in self.storage_info['portals'] or
                     self._set_target_portal(port, ipv4_addr, tcp_port))):
                self.storage_info['compute_ports'].append(port)

        self.check_ports_info()
        LOG.debug(
            'Setting portals: %s', self.storage_info['portals'])

    def _get_iscsi_info(self):
        port = None
        ipv4_addr = None
        tcp_port = None
        stdout = self.run_snm2_refer('auiscsi')
        for line in stdout.splitlines():
            if line.startswith("Port"):
                port = line.split()[1]
            elif 'Port Number' in line:
                tcp_port = line.split()[3]
            elif 'IPv4 Address' in line:
                ipv4_addr = line.split()[3]
            elif not line:
                yield (port, ipv4_addr, tcp_port)

    def _set_target_portal(self, port, ipv4_addr, tcp_port):
        if not ipv4_addr or not tcp_port:
            return False
        self.storage_info['portals'][port] = ':'.join(
            [ipv4_addr, tcp_port])
        return True

    def find_targets_from_storage(self, targets, connector, target_ports):
        iqn = self.get_hba_ids_from_connector(connector)
        nr_not_found = 0
        found = False
        port = None

        stdout = self.run_snm2_refer('autargetini')
        for match in re.finditer(
                r'^Port +(?P<port>\d\w) +Target +Security |'
                r'^ *(?P<gid>\d{3}):%(target_prefix)s\S* +%(iqn)s$' % {
                    'target_prefix': utils.TARGET_PREFIX,
                    'iqn': iqn,
                }, stdout, re.M):
            if match.group('port'):
                if match.group('port') in target_ports:
                    port = match.group('port')
                    found = False
                else:
                    port = None
            elif match.group('gid') and port and not found:
                gid = int(match.group('gid'))
                targets['info'][port] = True
                targets['list'].append((port, gid))
                found = True

        for port in target_ports:
            if port not in targets['info']:
                targets['info'][port] = False
                nr_not_found += 1

        return nr_not_found

    def create_target_to_storage(self, port, target_name, hba_ids):
        self.run_snm2(
            'autargetdef', '-add', port[0], port[1], '-talias',
            target_name, '-iname', hba_ids + utils.TARGET_IQN_SUFFIX,
            '-authmethod', 'CHAP', 'None', '-mutual', 'disable')
        return self._get_gid_by_target_name(port, target_name)

    def _get_gid_by_target_name(self, port, target_name):
        is_target_port = False
        stdout = self.run_snm2_refer('autargetdef')
        for line in stdout.splitlines():
            if not line:
                continue
            line = line.split()
            if line[0] == 'Port':
                is_target_port = line[1] == port
                continue
            if is_target_port and line[0][4:] == target_name:
                return int(line[0][:3])
        return None

    def set_target_mode(self, port, gid):
        self.run_snm2(
            'autargetopt', '-set', port[0], port[1],
            '-tno', gid, '-ReportFullPortalList', 'enable')

    def set_hba_ids(self, port, gid, hba_ids):
        self.run_snm2(
            'autargetini', '-add', port[0], port[1],
            '-tno', gid, '-iname', hba_ids)

    def delete_target_from_storage(self, port, gid):
        result = self.run_snm2(
            'autargetdef', '-rm', port[0], port[1],
            '-tno', gid, do_raise=False)
        if result[0]:
            utils.output_log(307, port=port, id=gid)

    def run_map_cmd_refer(self):
        return self.run_snm2_refer('autargetmap')

    def run_map_cmd(self, opr, ldev, port, gid, lun, **kwargs):
        return self.run_snm2(
            'autargetmap', opr, port[0], port[1], gid, lun, ldev, **kwargs)

    def get_properties_iscsi(self, targets, multipath):
        if not multipath:
            target_list = targets['list'][:1]
        else:
            target_list = targets['list'][:]
        for target in target_list:
            if target not in self.storage_info['iqns']:
                self._set_target_iqn(target)
        return super(HBSDSNM2ISCSI, self).get_properties_iscsi(
            targets, multipath)

    def _set_target_iqn(self, target):
        iqn = self._get_target_iqn(*target)
        if not iqn:
            msg = utils.output_log(650, resource='Target IQN')
            raise exception.HBSDError(data=msg)
        self.storage_info['iqns'][target] = iqn

    def _get_target_iqn(self, port, gid):
        is_target_port = False
        is_target_host = None
        target_string = '%03d:' % gid
        stdout = self.run_snm2_refer('autargetdef')
        for line in stdout.splitlines():
            if not line:
                continue
            line = line.split()
            if line[0] == 'Port':
                is_target_port = line[1] == port
                continue
            if is_target_port and line[0].startswith(target_string):
                is_target_host = True
                continue
            if is_target_host and line[0] == 'iSCSI':
                return line[3]
        return None
