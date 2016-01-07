# Copyright (C) 2014, 2015, Hitachi, Ltd.
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
"""Library of SNM2 interfaces for Hitachi storage."""

import math
import re

from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import excutils
from oslo_utils import timeutils

from cinder import exception

from cinder.volume.drivers.hitachi import hbsd_common as common
from cinder.volume.drivers.hitachi import hbsd_utils as utils

from six.moves import range

_SNM2_ENV = [
    'env', 'LANG=C', 'STONAVM_HOME=/usr/stonavm',
    'LD_LIBRARY_PATH=/usr/stonavm/lib', 'STONAVM_RSP_PASS=on', 'STONAVM_ACT=on'
]

VVOL_TYPE = "N/A(V-VOL)"

_DEFAULT_LDEV_RANGE = [0, 65535]

_MAX_LUN = 2047
_EXEC_TIMEOUT = 20
_EXEC_INTERVAL = 1

_CREATE_ERRORS = [
    'DMEC002047',
    'DMED09000A',
]
_DELETE_ERRORS = [
    'DMEC002048',
    'DMED090026',
]
_PAIR_ERRORS = [
    'DMER0300B8',
    'DMER0800CF',
    'DMER0800D[0-6D]',
    'DMER03006A',
    'DMER030080',
]
_DISPLAY_ERROR = ['DMEC002015']

_SNM2_NO_RETRY_ERRORS = [
    'DMEC002033',
    'DMEC002037',
    'DMEC002104',
    'DMEC002105',
    'DMEC002122',
]

LOG = logging.getLogger(__name__)


def _add_used_lun(used_list, stdout, ldev, port, gid):
    for line in stdout.splitlines()[2:]:
        if not line:
            continue
        line = line.split()
        if line[0] == port and int(line[1][0:3]) == gid:
            if int(line[2]) not in used_list:
                used_list.append(int(line[2]))
            if int(line[3]) == ldev:
                lun = int(line[2])
                LOG.debug(
                    'An logical unit path has already defined in the '
                    'specified logical device. (LDEV: %(ldev)s, '
                    'port: %(port)s, gid: %(gid)s, lun: %(lun)s)',
                    {'ldev': ldev, 'port': port, 'gid': gid, 'lun': lun})
                return lun
    return None


def _get_target_lun(stdout, ldev, port, gid):
    used_list = []
    lun = _add_used_lun(
        used_list, stdout, ldev, port, gid)
    if lun is not None:
        return lun, True
    if not used_list:
        return 0, False

    used_luns = set(used_list)
    for i in range(_MAX_LUN + 1):
        if i not in used_luns:
            return i, False
    msg = utils.output_log(650, resource="HLUN")
    raise exception.HBSDError(data=msg)


def _check_ldev(ldev_info, ldev, existing_ref):
    if not ldev_info:
        msg = utils.output_log(707)
        raise exception.ManageExistingInvalidReference(
            existing_ref=existing_ref, reason=msg)
    if (ldev_info['vol_type'] != utils.NORMAL_LDEV_TYPE or
            ldev_info['dppool'] == 'N/A'):
        msg = utils.output_log(702, ldev=ldev, ldevtype=utils.NVOL_LDEV_TYPE)
        raise exception.ManageExistingInvalidReference(
            existing_ref=existing_ref, reason=msg)
    # Hitachi storage calculates volume sizes in a block unit, 512 bytes.
    if ldev_info['vol_size'] % utils.GIGABYTE_PER_BLOCK_SIZE:
        msg = utils.output_log(703, ldev=ldev)
        raise exception.ManageExistingInvalidReference(
            existing_ref=existing_ref, reason=msg)
    if ldev_info['num_port']:
        msg = utils.output_log(704, ldev=ldev)
        raise exception.ManageExistingInvalidReference(
            existing_ref=existing_ref, reason=msg)


class HBSDSNM2(common.HBSDCommon):
    """SNM2 interface Class for hbsd drivers."""

    def run_storage_cli(self, *cmd, **kwargs):
        interval = kwargs.pop('interval', _EXEC_INTERVAL)

        def _wait_for_snm2_execution(start_time, *cmd, **kwargs):
            ignore_error = kwargs.pop('ignore_error', [])
            no_retry_error = ignore_error + _SNM2_NO_RETRY_ERRORS
            timeout = kwargs.pop('timeout', _EXEC_TIMEOUT)
            noretry = kwargs.pop('noretry', False)

            result = self._run_snm2(*cmd, **kwargs)
            if (not result[0] or noretry or
                    utils.check_ignore_error(no_retry_error, result[2]) or
                    utils.check_timeout(start_time, timeout)):
                raise loopingcall.LoopingCallDone(result)

        loop = loopingcall.FixedIntervalLoopingCall(
            _wait_for_snm2_execution, timeutils.utcnow(), *cmd, **kwargs)

        return loop.start(interval=interval).wait()

    @utils.synchronized('snm2')
    def _run_snm2(self, *cmd, **kwargs):
        return utils.execute(*cmd, **kwargs)

    def run_snm2(self, command, *args, **kwargs):
        cmd = _SNM2_ENV + [
            command, '-unit', self.conf.hitachi_storage_id] + list(args)
        return self.run_and_verify_storage_cli(*cmd, **kwargs)

    def run_snm2_refer(self, command, *args, **kwargs):
        result = self.run_snm2(command, '-refer', *args, **kwargs)
        return result[1]

    def get_storage_cli_info(self):
        version = 'N/A'
        result = self.run_snm2('auman', '-help', do_raise=False)
        match = re.search(r'^Version +(?P<version>\S+)', result[1], re.M)
        if match:
            version = match.group('version')
        return ('SNM2 CLI', version)

    def _run_auluref(self, *args, **kwargs):
        result = self.run_snm2(
            'auluref', *args, ignore_error=_DISPLAY_ERROR, **kwargs)
        return result[1]

    def get_unused_ldev(self):
        start, end = self.storage_info['ldev_range'][:2]
        stdout = self._run_auluref()
        if not stdout:
            return start
        free_ldev = start
        found = False
        for line in stdout.splitlines()[2:]:
            if not line:
                continue
            line = line.split()
            ldev_num = int(line[0])
            if free_ldev > ldev_num:
                continue
            if free_ldev == ldev_num:
                free_ldev += 1
            else:
                found = True
                break
            if free_ldev > end:
                break
        else:
            found = True
        if not found:
            msg = utils.output_log(648, resource='LDEV')
            raise exception.HBSDError(data=msg)
        return free_ldev

    def run_map_cmd(self, opr, ldev, port, gid, lun, **kwargs):
        raise NotImplementedError()

    def run_map_cmd_refer(self):
        raise NotImplementedError()

    @utils.synchronized('map_ldev')
    def unmap_ldev(self, targets, ldev):
        for port, gid, lun in targets['list']:
            self.run_map_cmd('-rm', ldev, port, gid, lun)
            LOG.debug(
                'Deleted logical unit path of the specified logical '
                'device. (LDEV: %(ldev)s, port: %(port)s, gid: %(gid)s, '
                'lun: %(lun)s)',
                {'ldev': ldev, 'port': port, 'gid': gid, 'lun': lun})

    @utils.synchronized('create_ldev')
    def create_ldev(self, size, is_vvol=False):
        max_try_num = len(utils.DEFAULT_TRY_RANGE)
        for i in utils.DEFAULT_TRY_RANGE:
            LOG.debug(
                'Try number: %(num)s / %(max)s',
                {'num': i + 1, 'max': max_try_num})
            try:
                return super(HBSDSNM2, self).create_ldev(size, is_vvol=is_vvol)
            except exception.HBSDNotFound:
                utils.output_log(312, resource='LDEV')
            except Exception:
                with excutils.save_and_reraise_exception():
                    utils.output_log(636)
        msg = utils.output_log(636)
        raise exception.HBSDError(data=msg)

    def create_ldev_on_storage(self, ldev, size, is_vvol):
        if is_vvol:
            command = 'aureplicationvvol'
            args = ['-add', '-lu', ldev, '-size', '%sg' % size]
        else:
            command = 'auluadd'
            args = ['-lu', ldev, '-dppoolno',
                    self.conf.hitachi_pool, '-size', '%sg' % size]
        result = self.run_snm2(command, *args, ignore_error=_CREATE_ERRORS)
        if result[0]:
            raise exception.HBSDNotFound

    @utils.synchronized('map_ldev')
    def map_ldev(self, targets, ldev):
        port, gid = targets['list'][0]
        stdout = self.run_map_cmd_refer()
        lun, is_mapped = _get_target_lun(stdout, ldev, port, gid)
        if not is_mapped:
            self.run_map_cmd('-add', ldev, port, gid, lun)
            LOG.debug(
                'Created logical unit path to the specified logical '
                'device. (LDEV: %(ldev)s, port: %(port)s, gid: %(gid)s, '
                'lun: %(lun)s)',
                {'ldev': ldev, 'port': port, 'gid': gid, 'lun': lun})
        for port, gid in targets['list'][1:]:
            try:
                self.run_map_cmd('-add', ldev, port, gid, lun)
                LOG.debug(
                    'Created logical unit path to the specified logical '
                    'device. (LDEV: %(ldev)s, port: %(port)s, '
                    'gid: %(gid)s, lun: %(lun)s)',
                    {'ldev': ldev, 'port': port, 'gid': gid, 'lun': lun})
            except exception.HBSDError:
                utils.output_log(314, ldev=ldev, port=port, id=gid, lun=lun)
        return lun

    def delete_ldev_from_storage(self, ldev):
        ldev_info = self._get_ldev_info(ldev)
        if not ldev_info:
            utils.output_log(319, ldev=ldev)
            return
        if ldev_info['vol_type'] == VVOL_TYPE:
            command = 'aureplicationvvol'
            args = ['-rm', '-lu', ldev]
        else:
            command = 'auludel'
            args = ['-lu', ldev, '-f']
        self.run_snm2(
            command, *args, timeout=30, interval=3,
            ignore_error=_DELETE_ERRORS)

    def extend_ldev(self, ldev, dummy_old_size, new_size):
        self.run_snm2('auluchgsize', '-lu', ldev, '-size', '%sg' % new_size)

    def get_pool_info(self):
        t_cap, cop_per, lt_per = None, None, None

        stdout = self.run_snm2_refer(
            'audppool', '-detail', '-g', '-dppoolno',
            self.storage_info['pool_id'])
        for line in stdout.splitlines():
            if not line:
                continue
            elif 'Total Capacity' in line:
                t_cap = line.split()[3]
                continue
            elif 'Current Over Provisioning Percent' in line:
                cop_per = line.split()[5].rstrip('%')
                continue
            elif 'Limit Threshold' in line:
                lt_per = line.split()[3].rstrip('%')
                break

        if not t_cap or not cop_per or not lt_per:
            msg = utils.output_log(640, pool=self.storage_info['pool_id'])
            raise exception.HBSDError(data=msg)

        total_gb = int(math.floor(float(t_cap) * (float(lt_per) / 100)))
        free_gb = total_gb - int(
            math.ceil(float(t_cap) * (float(cop_per) / 100)))
        return total_gb, free_gb

    def copy_on_storage(self, pvol, size, metadata):
        ldev_info = self._get_ldev_info(pvol)
        if not ldev_info:
            msg = utils.output_log(612, ldev=pvol)
            raise exception.HBSDError(data=msg)

        if ldev_info['vol_type'] == VVOL_TYPE:
            raise exception.HBSDNotSupported()
        self.delete_pair(pvol, all_split=False)
        return super(HBSDSNM2, self).copy_on_storage(pvol, size, metadata)

    def check_vvol(self, ldev):
        return self._get_ldev_info(ldev).get('vol_type') == VVOL_TYPE

    def _get_ldev_info(self, ldev, do_raise=False):
        data = {}
        stdout = self._run_auluref(
            '-lu', ldev, do_raise=do_raise, noretry=True)
        if stdout:
            line = stdout.splitlines()[2].split()
            data['vol_size'] = int(line[1])
            data['dppool'] = line[5]
            data['num_port'] = int(line[-2])
            data['vol_type'] = line[-1]
        return data

    def delete_pair_from_storage(self, pvol, svol, is_thin):
        self.run_aureplicationlocal('-simplex', pvol, svol, is_thin)

    def run_aureplicationlocal(self, opr, pvol, svol,
                               is_thin, *args, **kwargs):
        method = '-ss' if is_thin else '-si'
        return self.run_snm2(
            'aureplicationlocal', opr, method, '-pvol',
            pvol, '-svol', svol, *args, **kwargs)

    def create_pair_on_storage(self, pvol, svol, is_thin):
        args = ['-pace', self.storage_info['pace'], '-compsplit']
        if is_thin:
            pool = self.conf.hitachi_thin_pool
            args.extend(['-localrepdppoolno', pool, '-localmngdppoolno', pool])
            method = utils.THIN
        else:
            method = utils.FULL
        result = self.run_aureplicationlocal(
            '-create', pvol, svol, is_thin, *args, ignore_error=_PAIR_ERRORS)
        if result[0]:
            msg = utils.output_log(615, copy_method=method, pvol=pvol)
            raise exception.HBSDBusy(message=msg)

    def run_aureplicationlocal_refer(self, *args):
        return self.run_snm2_refer(
            'aureplicationlocal', *args, ignore_error=_DISPLAY_ERROR)

    def get_pair_info(self, ldev):
        stdout = self.run_aureplicationlocal_refer('-pvol', ldev)
        if stdout:
            pair_info = {'pvol': ldev, 'svol_info': []}
            for line in stdout.splitlines()[1:]:
                if not line:
                    continue
                if 'SnapShot' in line[100:]:
                    is_thin = True
                else:
                    is_thin = False
                pair_info['svol_info'].append({
                    'ldev': int(line.split()[2]),
                    'is_thin': is_thin,
                    'is_psus': re.search(r'Split\((.*)%\)', line),
                })
            return pair_info

        stdout = self.run_aureplicationlocal_refer('-svol', ldev)
        if not stdout:
            return None
        line = stdout.splitlines()[1]
        if 'SnapShot' in line[100:]:
            is_thin = True
        else:
            is_thin = False
        LOG.debug('Pair status: %s', line)
        return {
            'pvol': int(line.split()[1]),
            'svol_info': [{
                'ldev': ldev,
                'is_thin': is_thin,
                'is_psus': re.search(r'Split\((.*)%\)', line),
            }],
        }

    def get_ldev_size_in_gigabyte(self, ldev, existing_ref):
        ldev_info = self._get_ldev_info(ldev)
        _check_ldev(ldev_info, ldev, existing_ref)
        # Hitachi storage calculates volume sizes in a block unit, 512 bytes.
        return ldev_info['vol_size'] / utils.GIGABYTE_PER_BLOCK_SIZE

    def find_mapped_targets_from_storage(self, targets, ldev, target_ports):
        ldev_pt = re.compile(
            r'^ *(?P<port>%(ports)s) +(?P<gid>\d{3}):%(target_prefix)s\S* +'
            r'(?P<lun>\d+) +%(ldev)s$' % {
                'ports': '|'.join(target_ports),
                'target_prefix': utils.TARGET_PREFIX,
                'ldev': ldev,
            }, re.M)
        stdout = self.run_map_cmd_refer()
        for port, gid, lun in ldev_pt.findall(stdout):
            targets['list'].append((port, int(gid), int(lun)))

    def get_unmap_targets_list(self, target_list, mapped_list):
        unmap_list = []
        for mapping_info in mapped_list:
            if mapping_info[:2] in target_list:
                unmap_list.append(mapping_info)
        return unmap_list

    @utils.synchronized('create_target')
    def create_mapping_targets(self, targets, connector):
        return super(HBSDSNM2, self).create_mapping_targets(targets, connector)

    def check_param(self):
        super(HBSDSNM2, self).check_param()
        if not self.storage_info['ldev_range']:
            self.storage_info['ldev_range'] = _DEFAULT_LDEV_RANGE
        LOG.debug(
            'Setting ldev_range: %s', self.storage_info['ldev_range'])

    def config_lock(self):
        for key in ['snm2', 'create_ldev', 'create_target', 'map_ldev']:
            self.lock[key] = '_'.join([key, self.conf.hitachi_storage_id])

    def connect_storage(self):
        if self.conf.hitachi_copy_speed <= 2:
            pace = 'slow'
        elif self.conf.hitachi_copy_speed == 3:
            pace = 'normal'
        else:
            pace = 'prior'
        self.storage_info['pace'] = pace
        super(HBSDSNM2, self).connect_storage()

    def discard_zero_page(self, volume):
        pass
