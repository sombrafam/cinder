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
"""Fibre channel Cinder volume driver for HORCM interface."""

import re

from oslo_log import log as logging

from cinder import exception

from cinder.volume.drivers.hitachi import hbsd_horcm as horcm
from cinder.volume.drivers.hitachi import hbsd_utils as utils
from cinder.zonemanager import utils as fczm_utils

_FC_LINUX_MODE_OPTS = ['-host_mode', 'LINUX']
_FC_HOST_MODE_OPT = '-host_mode_opt'
_FC_HMO_DISABLE_IO = 91
_HOST_GROUPS_PATTERN = re.compile(
    r"^CL\w-\w+ +(?P<gid>\d+) +%s(?!pair00 )\S* +\d+ " % utils.TARGET_PREFIX,
    re.M)
_FC_PORT_PATTERN = re.compile(
    (r"^(CL\w-\w)\w* +(?:FIBRE|FCoE) +TAR +\w+ +\w+ +\w +\w+ +Y +"
     r"\d+ +\d+ +(\w{16})"), re.M)

LOG = logging.getLogger(__name__)


class HBSDHORCMFC(horcm.HBSDHORCM):
    """Fibre channel Class for HORCM interface."""

    def __init__(self, conf, storage_protocol, **kwargs):
        super(HBSDHORCMFC, self).__init__(
            conf, storage_protocol, **kwargs)
        self._lookup_service = fczm_utils.create_lookup_service()

    def connect_storage(self):
        target_ports = self.conf.hitachi_target_ports
        compute_target_ports = self.conf.hitachi_compute_target_ports
        pair_target_ports = self.conf.hitachi_horcm_pair_target_ports

        super(HBSDHORCMFC, self).connect_storage()
        result = self.run_raidcom('get', 'port')
        for port, wwn in _FC_PORT_PATTERN.findall(result[1]):
            if target_ports and port in target_ports:
                self.storage_info['ports'].append(port)
                self.storage_info['wwns'][port] = wwn
            if compute_target_ports and port in compute_target_ports:
                self.storage_info['compute_ports'].append(port)
                self.storage_info['wwns'][port] = wwn
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
            'Setting target wwns: %s', self.storage_info['wwns'])

    def create_target_to_storage(self, port, target_name, dummy_hba_ids):
        result = self.run_raidcom(
            'add', 'host_grp', '-port', port, '-host_grp_name', target_name)
        return horcm.find_value(result[1], 'gid')

    def set_hba_ids(self, port, gid, hba_ids):
        registered_wwns = []
        for wwn in hba_ids:
            try:
                self.run_raidcom(
                    'add', 'hba_wwn', '-port',
                    '-'.join([port, gid]), '-hba_wwn', wwn)
                registered_wwns.append(wwn)
            except exception.HBSDError:
                utils.output_log(317, port=port, gid=gid, wwn=wwn)
        if not registered_wwns:
            msg = utils.output_log(614, port=port, gid=gid)
            raise exception.HBSDError(msg)

    def set_target_mode(self, port, gid):
        hostmode_setting = _FC_LINUX_MODE_OPTS
        if self.conf.hitachi_horcm_disable_io_wait:
            hostmode_setting.append(_FC_HOST_MODE_OPT)
            hostmode_setting.append(_FC_HMO_DISABLE_IO)
        self.run_raidcom(
            'modify', 'host_grp', '-port',
            '-'.join([port, gid]), *hostmode_setting,
            success_code=horcm.ALL_EXIT_CODE)

    def find_targets_from_storage(self, targets, connector, target_ports):
        nr_not_found = 0
        target_name = '-'.join([utils.DRIVER_PREFIX, connector['ip']])
        success_code = horcm.HORCM_EXIT_CODE.union([horcm.EX_ENOOBJ])
        wwpns = self.get_hba_ids_from_connector(connector)
        wwpns_pattern = re.compile(
            r'^CL\w-\w+ +\d+ +\S+ +(%s) ' % '|'.join(wwpns), re.M)

        for port in target_ports:
            targets['info'][port] = False

            result = self.run_raidcom(
                'get', 'hba_wwn', '-port', port, target_name,
                success_code=success_code)
            wwpns = wwpns_pattern.findall(result[1])
            if wwpns:
                gid = result[1].splitlines()[1].split()[1]
                targets['info'][port] = True
                targets['list'].append((port, gid))
                LOG.debug(
                    'Found wwpns in host group. '
                    '(port: %(port)s, gid: %(gid)s, wwpns: %(wwpns)s)',
                    {'port': port, 'gid': gid, 'wwpns': wwpns})
                continue
            if self.conf.hitachi_horcm_name_only_discovery:
                nr_not_found += 1
                continue

            result = self.run_raidcom(
                'get', 'host_grp', '-port', port)
            for gid in _HOST_GROUPS_PATTERN.findall(result[1]):
                result = self.run_raidcom(
                    'get', 'hba_wwn', '-port', '-'.join([port, gid]))
                wwpns = wwpns_pattern.findall(result[1])
                if wwpns:
                    targets['info'][port] = True
                    targets['list'].append((port, gid))
                    LOG.debug(
                        'Found wwpns in host group. (port: %(port)s, '
                        'gid: %(gid)s, wwpns: %(wwpns)s)',
                        {'port': port, 'gid': gid, 'wwpns': wwpns})
                    break
            else:
                nr_not_found += 1

        return nr_not_found

    @fczm_utils.AddFCZone
    def initialize_connection(self, volume, connector):
        conn_info = super(HBSDHORCMFC, self).initialize_connection(
            volume, connector)
        if self.conf.hitachi_zoning_request:
            init_targ_map = utils.build_initiator_target_map(
                connector, conn_info['data']['target_wwn'],
                self._lookup_service)
            if init_targ_map:
                conn_info['data']['initiator_target_map'] = init_targ_map
        return conn_info
