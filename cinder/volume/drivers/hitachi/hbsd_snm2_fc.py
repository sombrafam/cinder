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
"""Fibre channel Cinder volume driver for SNM2 interface."""

import re

from oslo_log import log as logging

from cinder import exception

from cinder.volume.drivers.hitachi import hbsd_snm2 as snm2
from cinder.volume.drivers.hitachi import hbsd_utils as utils
from cinder.zonemanager import utils as fczm_utils

_HBA_ERROR = ["DMEC002043"]

LOG = logging.getLogger(__name__)


class HBSDSNM2FC(snm2.HBSDSNM2):
    """Fibre channel Class for SNM2 interface."""

    def __init__(self, conf, storage_protocol, **kwargs):
        super(HBSDSNM2FC, self).__init__(
            conf, storage_protocol, **kwargs)
        self._lookup_service = fczm_utils.create_lookup_service()

    def connect_storage(self):
        target_ports = self.conf.hitachi_target_ports
        compute_target_ports = self.conf.hitachi_compute_target_ports

        super(HBSDSNM2FC, self).connect_storage()
        stdout = self.run_snm2_refer('auhgwwn')
        fibre_stdout = self.run_snm2_refer('aufibre1')
        for line in fibre_stdout.splitlines()[3:]:
            if re.match('Transfer', line):
                break
            line = line.split()
            if len(line) < 4:
                continue
            port = ''.join([line[0], line[1]])
            wwn = line[3]
            if not re.search(
                    r'^Port +%s +Host +Group +Security +ON$' % port,
                    stdout, re.M):
                continue

            if target_ports and port in target_ports:
                self.storage_info['ports'].append(port)
                self.storage_info['wwns'][port] = wwn
            if compute_target_ports and port in compute_target_ports:
                self.storage_info['compute_ports'].append(port)
                self.storage_info['wwns'][port] = wwn

        self.check_ports_info()
        LOG.debug(
            'Setting target wwns: %s', self.storage_info['wwns'])

    def find_targets_from_storage(self, targets, connector, target_ports):
        hba_ids = self.get_hba_ids_from_connector(connector)
        nr_not_found = 0
        found = False
        port = None

        stdout = self.run_snm2_refer('auhgwwn')
        for match in re.finditer(
                r'^Port +(?P<port>\d\w) +Host +Group +Security |'
                r'^.+ (?P<wwpns>%(wwpns)s) '
                r'+(?P<gid>\d{3}):%(target_prefix)s' % {
                    'wwpns': '|'.join(hba_ids),
                    'target_prefix': utils.TARGET_PREFIX,
                }, stdout, re.M | re.I):
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
                LOG.debug(
                    'Found wwpns in host group. (port: %(port)s, '
                    'gid: %(gid)s, wwpns: %(wwpns)s)',
                    {'port': port, 'gid': gid, 'wwpns': match.group('wwpns')})
                found = True

        for port in target_ports:
            if port not in targets['info']:
                targets['info'][port] = False
                nr_not_found += 1

        return nr_not_found

    def create_target_to_storage(self, port, target_name, dummy_hba_ids):
        self.run_snm2(
            'auhgdef', '-add', port[0], port[1], '-gname', target_name)
        return self._get_gid_by_target_name(port, target_name)

    def _get_gid_by_target_name(self, port, target_name):
        is_target_port = False
        stdout = self.run_snm2_refer('auhgdef')
        for line in stdout.splitlines():
            if not line:
                continue
            line = line.split()
            if line[0] == 'Port':
                is_target_port = line[1] == port
                continue
            if is_target_port and line[1] == target_name:
                return int(line[0])
        return None

    def set_target_mode(self, port, gid):
        self.run_snm2(
            'auhgopt', '-set', port[0], port[1], '-gno', gid,
            '-platform', 'Linux', '-middleware', 'NotSpecified')

    def set_hba_ids(self, port, gid, hba_ids):
        registered_wwns = []
        for wwn in hba_ids:
            try:
                result = self.run_snm2(
                    'auhgwwn', '-set', '-permhg', port[0], port[1], wwn,
                    '-gno', gid, ignore_error=_HBA_ERROR)
                if result[0]:
                    self.run_snm2(
                        'auhgwwn', '-assign', '-permhg', port[0], port[1],
                        wwn, '-gno', gid)
                registered_wwns.append(wwn)
            except exception.HBSDError:
                utils.output_log(317, port=port, gid=gid, wwn=wwn)
        if not registered_wwns:
            msg = utils.output_log(614, port=port, gid=gid)
            raise exception.HBSDError(msg)

    def delete_target_from_storage(self, port, gid):
        result = self.run_snm2(
            'auhgdef', '-rm', port[0], port[1], '-gno', gid, do_raise=False)
        if result[0]:
            utils.output_log(306, port=port, id=gid)

    def run_map_cmd_refer(self):
        return self.run_snm2_refer('auhgmap')

    def run_map_cmd(self, opr, ldev, port, gid, lun, **kwargs):
        return self.run_snm2(
            'auhgmap', opr, port[0], port[1], gid, lun, ldev, **kwargs)

    @fczm_utils.AddFCZone
    def initialize_connection(self, volume, connector):
        conn_info = super(HBSDSNM2FC, self).initialize_connection(
            volume, connector)
        if self.conf.hitachi_zoning_request:
            init_targ_map = utils.build_initiator_target_map(
                connector, conn_info['data']['target_wwn'],
                self._lookup_service)
            if init_targ_map:
                conn_info['data']['initiator_target_map'] = init_targ_map
        return conn_info
