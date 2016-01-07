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
"""Common class for Hitachi storage drivers."""

import re

from oslo_config import cfg
from oslo_config import types
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import units
import six

from cinder import exception

from cinder import utils as cinder_utils
from cinder.volume.drivers.hitachi import hbsd_utils as utils
from cinder.volume import utils as volume_utils

"""
Version history:
    1.0.0 - Initial driver
    1.1.0 - Add manage_existing/manage_existing_get_size/unmanage methods
    1.2.0 - Refactor the drivers to use the storage interfaces in common.
"""

VERSION = '1.2.0'

_COPY_METHOD = set(['FULL', 'THIN'])

_INHERITED_VOLUME_OPTS = [
    'volume_backend_name',
    'volume_driver',
    'reserved_percentage',
    'use_multipath_for_image_xfer',
    'enforce_multipath_for_image_xfer',
    'num_volume_device_scan_tries',
]

common_opts = [
    cfg.StrOpt(
        'hitachi_storage_cli',
        choices=['SNM2', 'HORCM'],
        help='Type of storage command line interface'),
    cfg.StrOpt(
        'hitachi_storage_id',
        help='ID of storage system'),
    cfg.StrOpt(
        'hitachi_pool',
        help='Pool of storage system'),
    cfg.StrOpt(
        'hitachi_thin_pool',
        help='Thin pool of storage system'),
    cfg.StrOpt(
        'hitachi_ldev_range',
        help='Logical device range of storage system'),
    cfg.StrOpt(
        'hitachi_default_copy_method',
        default='FULL',
        choices=['FULL', 'THIN'],
        help='Default copy method of storage system'),
    cfg.Opt(
        'hitachi_copy_speed',
        type=types.Integer(min=1, max=15),
        default=3,
        help='Copy speed of storage system'),
    cfg.Opt(
        'hitachi_copy_check_interval',
        type=types.Integer(min=1, max=600),
        default=3,
        help='Interval to check copy'),
    cfg.Opt(
        'hitachi_async_copy_check_interval',
        type=types.Integer(min=1, max=600),
        default=10,
        help='Interval to check copy asynchronously'),
    cfg.ListOpt(
        'hitachi_target_ports',
        help='Target port names for host group or iSCSI target'),
    cfg.ListOpt(
        'hitachi_compute_target_ports',
        help=(
            'Target port names of compute node '
            'for host group or iSCSI target')),
    cfg.BoolOpt(
        'hitachi_group_request',
        default=False,
        help='Request for creating host group or iSCSI target'),
    cfg.BoolOpt(
        'hitachi_driver_cert_mode',
        default=False,
        secret=True,
        help='Driver cert mode'),
]

CONF = cfg.CONF
CONF.register_opts(common_opts)

LOG = logging.getLogger(__name__)


def _str2int(num):
    if not num:
        return None
    if num.isdigit():
        return int(num)
    if not re.match(r'\w\w:\w\w:\w\w', num):
        return None
    try:
        return int(num.replace(':', ''), 16)
    except ValueError:
        return None


class HBSDCommon(object):
    """Common Class for the hbsd drivers."""

    def __init__(self, conf, driverinfo, **kwargs):
        self.conf = conf
        self.db = kwargs.get('db')
        self.ctxt = None
        self.lock = {
            'do_setup': 'do_setup',
        }
        self.driver_info = driverinfo
        self.storage_info = {
            'protocol': driverinfo['proto'],
            'pool_id': None,
            'ldev_range': [],
            'ports': [],
            'compute_ports': [],
            'pair_ports': [],
            'wwns': {},
            'portals': {},
            'iqns': {},
            'output_first': True
        }

        self._stats = {}

    def run_and_verify_storage_cli(self, *cmd, **kwargs):
        do_raise = kwargs.pop('do_raise', True)
        ignore_error = kwargs.get('ignore_error')
        success_code = kwargs.get('success_code', set([0]))
        (ret, stdout, stderr) = self.run_storage_cli(*cmd, **kwargs)
        if (ret not in success_code and
                not utils.check_ignore_error(ignore_error, stderr)):
            msg = utils.output_log(
                600, cmd=' '.join([six.text_type(c) for c in cmd]),
                ret=ret, out=' '.join(stdout.splitlines()),
                err=' '.join(stderr.splitlines()))
            if do_raise:
                raise exception.HBSDError(data=msg)
        return ret, stdout, stderr

    def run_storage_cli(self, *cmd, **kwargs):
        raise NotImplementedError()

    def get_copy_method(self, metadata):
        method = metadata.get(
            'copy_method', self.conf.hitachi_default_copy_method)
        if method not in _COPY_METHOD:
            msg = utils.output_log(602, meta='copy_method')
            raise exception.HBSDError(data=msg)
        if method == 'THIN' and not self.conf.hitachi_thin_pool:
            msg = utils.output_log(601, param='hitachi_thin_pool')
            raise exception.HBSDError(data=msg)
        return method

    def create_volume(self, volume):
        try:
            ldev = self.create_ldev(volume['size'])
        except Exception:
            with excutils.save_and_reraise_exception():
                utils.output_log(636)
        metadata = volume.get('metadata', {})
        return {
            'provider_location': six.text_type(ldev),
            'metadata': dict(
                metadata, ldev=ldev, type=utils.NORMAL_LDEV_TYPE),
        }

    def create_ldev(self, size, is_vvol=False):
        ldev = self.get_unused_ldev()
        self.create_ldev_on_storage(ldev, size, is_vvol)
        LOG.debug('Created logical device. (LDEV: %s)', ldev)
        return ldev

    def create_ldev_on_storage(self, ldev, size, is_vvol):
        raise NotImplementedError()

    def get_unused_ldev(self):
        raise NotImplementedError()

    def create_volume_from_snapshot(self, volume, snapshot):
        ldev = utils.get_ldev(snapshot)
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is None'.
        if ldev is None:
            msg = utils.output_log(
                624, type='snapshot', id=snapshot['id'])
            raise exception.HBSDError(data=msg)
        size = volume['size']
        if size != snapshot['volume_size']:
            msg = utils.output_log(
                617, type='snapshot', volume_id=volume['id'])
            raise exception.HBSDError(data=msg)
        metadata = volume.get('metadata', {})
        new_ldev, ldev_type = self._copy_ldev(ldev, size, metadata)
        return {
            'provider_location': six.text_type(new_ldev),
            'metadata': dict(
                metadata, ldev=new_ldev, type=ldev_type,
                snapshot=snapshot['id']),
        }

    def _copy_ldev(self, ldev, size, metadata):
        try:
            return self.copy_on_storage(ldev, size, metadata)
        except exception.HBSDNotSupported:
            return self._copy_on_host(ldev, size)

    def _copy_on_host(self, src_ldev, size):
        dest_ldev = self.create_ldev(size)
        try:
            self._copy_with_dd(src_ldev, dest_ldev, size)
        except Exception:
            with excutils.save_and_reraise_exception():
                try:
                    self._delete_ldev(dest_ldev)
                except exception.HBSDError:
                    utils.output_log(313, ldev=dest_ldev)
        return dest_ldev, utils.NORMAL_LDEV_TYPE

    def _copy_with_dd(self, src_ldev, dest_ldev, size):
        src_info = None
        dest_info = None
        properties = cinder_utils.brick_get_connector_properties(
            multipath=self.conf.use_multipath_for_image_xfer,
            enforce_multipath=self.conf.enforce_multipath_for_image_xfer)
        try:
            dest_info = self._attach_ldev(dest_ldev, properties)
            src_info = self._attach_ldev(src_ldev, properties)
            volume_utils.copy_volume(
                src_info['device']['path'], dest_info['device']['path'],
                size * units.Ki, self.conf.volume_dd_blocksize)
        finally:
            if src_info:
                self._detach_ldev(src_info, src_ldev, properties)
            if dest_info:
                self._detach_ldev(dest_info, dest_ldev, properties)
        self.discard_zero_page({'provider_location': six.text_type(dest_ldev)})

    def _attach_ldev(self, ldev, properties):
        volume = {
            'provider_location': six.text_type(ldev),
        }
        conn = self.initialize_connection(volume, properties)
        try:
            connector = cinder_utils.brick_get_connector(
                conn['driver_volume_type'],
                use_multipath=self.conf.use_multipath_for_image_xfer,
                device_scan_attempts=self.conf.num_volume_device_scan_tries,
                conn=conn)
            device = connector.connect_volume(conn['data'])
        except Exception as ex:
            with excutils.save_and_reraise_exception():
                utils.output_log(634, ldev=ldev, reason=six.text_type(ex))
                self._terminate_connection(volume, properties)
        return {
            'conn': conn,
            'device': device,
            'connector': connector,
        }

    def _detach_ldev(self, attach_info, ldev, properties):
        volume = {
            'provider_location': six.text_type(ldev),
        }
        connector = attach_info['connector']
        try:
            connector.disconnect_volume(
                attach_info['conn']['data'], attach_info['device'])
        except Exception as ex:
            utils.output_log(329, ldev=ldev, reason=six.text_type(ex))
        self._terminate_connection(volume, properties)

    def _terminate_connection(self, volume, connector):
        try:
            self.terminate_connection(volume, connector)
        except exception.HBSDError:
            utils.output_log(310, ldev=utils.get_ldev(volume))

    def copy_on_storage(self, pvol, size, metadata):
        is_thin = self.get_copy_method(metadata) == "THIN"
        ldev_type = utils.VVOL_LDEV_TYPE if is_thin else utils.NORMAL_LDEV_TYPE
        svol = self.create_ldev(size, is_vvol=is_thin)
        try:
            self.create_pair_on_storage(pvol, svol, is_thin)
        except Exception:
            with excutils.save_and_reraise_exception():
                try:
                    self._delete_ldev(svol)
                except exception.HBSDError:
                    utils.output_log(313, ldev=svol)
        return svol, ldev_type

    def create_pair_on_storage(self, pvol, svol, is_thin):
        raise NotImplementedError()

    def _delete_ldev(self, ldev):
        self.delete_pair(ldev)
        self.delete_ldev_from_storage(ldev)

    def delete_pair(self, ldev, all_split=True):
        pair_info = self.get_pair_info(ldev)
        if not pair_info:
            return
        if pair_info['pvol'] == ldev:
            self.delete_pair_based_on_pvol(pair_info, all_split)
        else:
            self.delete_pair_based_on_svol(
                pair_info['pvol'], pair_info['svol_info'][0])

    def get_pair_info(self, ldev):
        raise NotImplementedError()

    def delete_pair_based_on_pvol(self, pair_info, all_split):
        svols = []

        for svol_info in pair_info['svol_info']:
            if svol_info['is_thin'] or not svol_info['is_psus']:
                svols.append(six.text_type(svol_info['ldev']))
                continue
            self.delete_pair_from_storage(
                pair_info['pvol'], svol_info['ldev'], False)
        if all_split and svols:
            msg = utils.output_log(
                616, pvol=pair_info['pvol'], svol=', '.join(svols))
            raise exception.HBSDBusy(message=msg)

    def delete_pair_from_storage(self, pvol, svol, is_thin):
        raise NotImplementedError()

    def delete_pair_based_on_svol(self, pvol, svol_info):
        if not svol_info['is_psus']:
            msg = utils.output_log(616, pvol=pvol, svol=svol_info['ldev'])
            raise exception.HBSDBusy(message=msg)
        self.delete_pair_from_storage(
            pvol, svol_info['ldev'], svol_info['is_thin'])

    def delete_ldev_from_storage(self, ldev):
        raise NotImplementedError()

    def create_cloned_volume(self, volume, src_vref):
        ldev = utils.get_ldev(src_vref)
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is not None'.
        if ldev is None:
            msg = utils.output_log(624, type='volume', id=src_vref['id'])
            raise exception.HBSDError(data=msg)
        size = volume['size']
        if size != src_vref['size']:
            msg = utils.output_log(617, type='volume', volume_id=volume['id'])
            raise exception.HBSDError(data=msg)
        metadata = volume.get('metadata', {})
        new_ldev, ldev_type = self._copy_ldev(ldev, size, metadata)
        return {
            'provider_location': six.text_type(new_ldev),
            'metadata': dict(
                metadata, ldev=new_ldev,
                type=ldev_type, volume=src_vref['id']),
        }

    def delete_volume(self, volume):
        ldev = utils.get_ldev(volume)
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is not None'.
        if ldev is None:
            utils.output_log(304, method='delete_volume', id=volume['id'])
            return
        try:
            self._delete_ldev(ldev)
        except exception.HBSDBusy:
            raise exception.HBSDVolumeIsBusy(volume_name=volume['name'])

    def create_snapshot(self, snapshot):
        src_vref = self.db.volume_get(self.ctxt, snapshot['volume_id'])
        ldev = utils.get_ldev(src_vref)
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is None'.
        if ldev is None:
            msg = utils.output_log(624, type='volume', id=src_vref['id'])
            raise exception.HBSDError(data=msg)
        size = snapshot['volume_size']
        metadata = utils.get_volume_metadata(src_vref)
        new_ldev, ldev_type = self._copy_ldev(ldev, size, metadata)
        if not self.conf.hitachi_driver_cert_mode:
            self.db.snapshot_metadata_update(
                self.ctxt, snapshot['id'],
                dict(ldev=new_ldev, type=ldev_type), False)
        return {
            'provider_location': six.text_type(new_ldev),
        }

    def delete_snapshot(self, snapshot):
        ldev = utils.get_ldev(snapshot)
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is None'.
        if ldev is None:
            utils.output_log(
                304, method='delete_snapshot', id=snapshot['id'])
            return
        try:
            self._delete_ldev(ldev)
        except exception.HBSDBusy:
            raise exception.HBSDSnapshotIsBusy(snapshot_name=snapshot['name'])

    def get_volume_stats(self, refresh=False):
        if refresh:
            if self.storage_info['output_first']:
                self.storage_info['output_first'] = False
                utils.output_log(3, config_group=self.conf.config_group)
            self._update_volume_stats()
        return self._stats

    def _update_volume_stats(self):
        data = {}
        backend_name = self.conf.safe_get('volume_backend_name')
        data['volume_backend_name'] = (
            backend_name or self.driver_info['volume_backend_name'])
        data['vendor_name'] = 'Hitachi'
        data['driver_version'] = VERSION
        data['storage_protocol'] = self.storage_info['protocol']
        try:
            total_gb, free_gb = self.get_pool_info()
        except exception.HBSDError:
            utils.output_log(620, pool=self.conf.hitachi_pool)
            return
        data['total_capacity_gb'] = total_gb
        data['free_capacity_gb'] = free_gb
        data['allocated_capacity_gb'] = 0 if total_gb == 'infinite' else (
            total_gb - free_gb)
        data['reserved_percentage'] = self.conf.safe_get('reserved_percentage')
        data['QoS_support'] = False
        LOG.debug("Updating volume status. (%s)", data)
        self._stats = data

    def get_pool_info(self):
        raise NotImplementedError()

    def copy_dest_vol_meta_to_src_vol(self, src_vol, dest_vol):
        metadata = src_vol.get('metadata', {})
        try:
            self.db.volume_metadata_update(
                self.ctxt, src_vol['id'], metadata, True)
        except Exception as ex:
            utils.output_log(
                318, src_vol_id=src_vol['id'], dest_vol_id=dest_vol['id'],
                reason=six.text_type(ex))

    def discard_zero_page(self, volume):
        raise NotImplementedError()

    def extend_volume(self, volume, new_size):
        ldev = utils.get_ldev(volume)
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is None'.
        if ldev is None:
            msg = utils.output_log(613, volume_id=volume['id'])
            raise exception.HBSDError(data=msg)
        if self.check_vvol(ldev):
            msg = utils.output_log(618, volume_id=volume['id'])
            raise exception.HBSDError(data=msg)
        self.delete_pair(ldev)
        self.extend_ldev(ldev, volume['size'], new_size)

    def check_vvol(self, ldev):
        raise NotImplementedError()

    def extend_ldev(self, ldev, old_size, new_size):
        raise NotImplementedError()

    def manage_existing(self, volume, existing_ref):
        ldev = _str2int(existing_ref.get('source-id'))
        metadata = volume.get('metadata', {})
        return {
            'provider_location': six.text_type(ldev),
            'metadata': dict(
                metadata, ldev=ldev, type=utils.NORMAL_LDEV_TYPE),
        }

    def manage_existing_get_size(self, dummy_volume, existing_ref):
        ldev = _str2int(existing_ref.get('source-id'))
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is None'.
        if ldev is None:
            msg = utils.output_log(707)
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)
        return self.get_ldev_size_in_gigabyte(ldev, existing_ref)

    def get_ldev_size_in_gigabyte(self, ldev, existing_ref):
        raise NotImplementedError()

    def unmanage(self, volume):
        ldev = utils.get_ldev(volume)
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is None'.
        if ldev is None:
            utils.output_log(304, method='unmanage', id=volume['id'])
            return
        if self.check_vvol(ldev):
            utils.output_log(
                706, volume_id=volume['id'],
                volume_type=utils.NORMAL_LDEV_TYPE)
            raise exception.HBSDVolumeIsBusy(volume_name=volume['name'])
        try:
            self.delete_pair(ldev)
        except exception.HBSDBusy:
            raise exception.HBSDVolumeIsBusy(volume_name=volume['name'])

    def do_setup(self, context):
        self.ctxt = context

        self.check_param()
        self.config_lock()
        self.connect_storage()
        self.init_cinder_hosts()
        self.output_param_to_log()

    def check_param(self):
        utils.check_opt_value(self.conf, _INHERITED_VOLUME_OPTS)
        utils.check_opts(self.conf, common_opts)
        utils.check_opts(self.conf, self.driver_info['volume_opts'])
        if (self.conf.hitachi_default_copy_method == 'THIN' and
                not self.conf.hitachi_thin_pool):
            msg = utils.output_log(601, param='hitachi_thin_pool')
            raise exception.HBSDError(data=msg)
        if self.conf.hitachi_ldev_range:
            self.storage_info['ldev_range'] = self._range2list(
                'hitachi_ldev_range')
        if (not self.conf.hitachi_target_ports and
                not self.conf.hitachi_compute_target_ports):
            msg = utils.output_log(601, param='hitachi_target_ports')
            raise exception.HBSDError(data=msg)
        if self.storage_info['protocol'] == 'iSCSI':
            self.check_param_iscsi()

    def check_param_iscsi(self):
        if self.conf.hitachi_use_chap_auth:
            if not self.conf.hitachi_auth_user:
                msg = utils.output_log(601, param='hitachi_auth_user')
                raise exception.HBSDError(data=msg)
            if not self.conf.hitachi_auth_password:
                msg = utils.output_log(601, param='hitachi_auth_password')
                raise exception.HBSDError(data=msg)

    def _range2list(self, param):
        values = [_str2int(x) for x in self.conf.safe_get(param).split('-')]
        if (len(values) != 2 or
                values[0] is None or values[1] is None or
                values[0] > values[1]):
            msg = utils.output_log(601, param=param)
            raise exception.HBSDError(data=msg)
        return values

    def config_lock(self):
        raise NotImplementedError()

    def connect_storage(self):
        self.storage_info['pool_id'] = self.get_pool_id()
        # When 'pool_id' is 0, it should be true.
        # Therefore, it cannot remove 'is None'.
        if self.storage_info['pool_id'] is None:
            msg = utils.output_log(640, pool=self.conf.hitachi_pool)
            raise exception.HBSDError(data=msg)
        LOG.debug('Setting pool id: %s', self.storage_info['pool_id'])

    def check_ports_info(self):
        if (self.conf.hitachi_target_ports and
                not self.storage_info['ports']):
            msg = utils.output_log(650, resource="Target ports")
            raise exception.HBSDError(data=msg)
        if (self.conf.hitachi_compute_target_ports and
                not self.storage_info['compute_ports']):
            msg = utils.output_log(650, resource="Compute target ports")
            raise exception.HBSDError(data=msg)
        LOG.debug(
            'Setting target_ports: %s', self.storage_info['ports'])
        LOG.debug(
            'Setting compute_target_ports: %s',
            self.storage_info['compute_ports'])

    def get_pool_id(self):
        pool = self.conf.hitachi_pool
        if pool.isdigit():
            return int(pool)
        return None

    def init_cinder_hosts(self, **kwargs):
        targets = kwargs.pop('targets', {'info': {}, 'list': []})
        connector = cinder_utils.brick_get_connector_properties(
            multipath=self.conf.use_multipath_for_image_xfer,
            enforce_multipath=self.conf.enforce_multipath_for_image_xfer)
        target_ports = self.storage_info['ports']

        if target_ports:
            if (self.find_targets_from_storage(
                    targets, connector, target_ports) and
                    self.conf.hitachi_group_request):
                self.create_mapping_targets(targets, connector)

            utils.require_target_existed(targets)

    def find_targets_from_storage(self, targets, connector, target_ports):
        raise NotImplementedError()

    def create_mapping_targets(self, targets, connector):
        hba_ids = self.get_hba_ids_from_connector(connector)
        for port in targets['info'].keys():
            if targets['info'][port]:
                continue

            try:
                self._create_target(targets, port, connector['ip'], hba_ids)
            except exception.HBSDError:
                utils.output_log(
                    self.driver_info['msg_id']['target'], port=port)

        if not targets['list']:
            self.find_targets_from_storage(
                targets, connector, targets['info'].keys())

    def get_hba_ids_from_connector(self, connector):
        if self.driver_info['hba_id'] in connector:
            return connector[self.driver_info['hba_id']]
        msg = utils.output_log(650, resource=self.driver_info['hba_id_type'])
        raise exception.HBSDError(data=msg)

    def _create_target(self, targets, port, ip, hba_ids):
        target_name = '-'.join([utils.DRIVER_PREFIX, ip])
        gid = self.create_target_to_storage(port, target_name, hba_ids)
        LOG.debug(
            'Created target. (port: %(port)s, gid: %(gid)s, '
            'target_name: %(target)s)',
            {'port': port, 'gid': gid, 'target': target_name})
        try:
            self.set_target_mode(port, gid)
            self.set_hba_ids(port, gid, hba_ids)
        except Exception:
            with excutils.save_and_reraise_exception():
                self.delete_target_from_storage(port, gid)
        targets['info'][port] = True
        targets['list'].append((port, gid))

    def create_target_to_storage(self, port, target_name, hba_ids):
        raise NotImplementedError()

    def set_target_mode(self, port, gid):
        raise NotImplementedError()

    def set_hba_ids(self, port, gid, hba_ids):
        raise NotImplementedError()

    def delete_target_from_storage(self, port, gid):
        raise NotImplementedError()

    def output_param_to_log(self):
        utils.output_log(1, config_group=self.conf.config_group)
        name, version = self.get_storage_cli_info()
        utils.output_storage_cli_info(name, version)
        utils.output_opt_info(self.conf, _INHERITED_VOLUME_OPTS)
        utils.output_opts(self.conf, common_opts)
        utils.output_opts(self.conf, self.driver_info['volume_opts'])

    def get_storage_cli_info(self):
        raise NotImplementedError()

    def initialize_connection(self, volume, connector):
        targets = {
            'info': {},
            'list': [],
        }
        ldev = utils.get_ldev(volume)
        # When 'ldev' is 0, it should be true.
        # Therefore, it cannot remove 'is None'.
        if ldev is None:
            msg = utils.output_log(619, volume_id=volume['id'])
            raise exception.HBSDError(data=msg)

        target_ports = self.get_target_ports(connector)
        if (self.find_targets_from_storage(
                targets, connector, target_ports) and
                self.conf.hitachi_group_request):
            self.create_mapping_targets(targets, connector)

        utils.require_target_existed(targets)

        targets['list'].sort()
        self.modify_target_mode(volume, targets)
        target_lun = self.map_ldev(targets, ldev)

        return {
            'driver_volume_type': self.driver_info['volume_type'],
            'data': self.get_properties(volume, targets, target_lun,
                                        connector),
        }

    def get_target_ports(self, connector):
        if connector['ip'] == CONF.my_ip:
            return self.storage_info['ports']
        return (self.storage_info['compute_ports'] or
                self.storage_info['ports'])

    def modify_target_mode(self, volume, targets):
        pass

    def map_ldev(self, targets, ldev):
        raise NotImplementedError()

    def get_properties(self, volume, targets, target_lun, connector):
        multipath = connector.get('multipath', False)
        if self.storage_info['protocol'] == 'FC':
            data = self.get_properties_fc(targets)
        elif self.storage_info['protocol'] == 'iSCSI':
            data = self.get_properties_iscsi(targets, multipath)
        data['target_discovered'] = False
        data['access_mode'] = self._get_access_mode(volume)
        if not multipath or self.storage_info['protocol'] == 'FC':
            data['target_lun'] = target_lun
        else:
            data['target_luns'] = [target_lun] * len(targets['list'])
        return data

    def get_properties_fc(self, targets):
        data = {}
        data['target_wwn'] = [
            self.storage_info['wwns'][x] for x in targets['info'].keys()
            if targets['info'][x]]
        return data

    def get_properties_iscsi(self, targets, multipath):
        data = {}
        primary_target = targets['list'][0]
        if not multipath:
            data['target_portal'] = self.storage_info[
                'portals'][primary_target[0]]
            data['target_iqn'] = self.storage_info['iqns'][primary_target]
        else:
            data['target_portals'] = [
                self.storage_info['portals'][x[0]] for x in targets['list']]
            data['target_iqns'] = [
                self.storage_info['iqns'][x] for x in targets['list']]
        if self.conf.hitachi_use_chap_auth:
            data['auth_method'] = 'CHAP'
            data['auth_username'] = self.conf.hitachi_auth_user
            data['auth_password'] = self.conf.hitachi_auth_password
        return data

    def _get_access_mode(self, volume):
        if 'id' not in volume:
            return 'rw'
        rv = self.db.volume_admin_metadata_get(self.ctxt, volume['id'])
        admin_metadata = dict(six.iteritems(rv))
        access_mode = admin_metadata.get('attached_mode')
        if not access_mode:
            access_mode = (
                'ro' if admin_metadata.get('readonly') == 'True' else 'rw')
        return access_mode

    def terminate_connection(self, volume, connector, **dummy_kwargs):
        targets = {
            'info': {},
            'list': [],
        }
        mapped_targets = {
            'list': [],
        }
        unmap_targets = {}

        ldev = utils.get_ldev(volume)
        if ldev is None:
            utils.output_log(302, volume_id=volume['id'])
            return
        target_ports = self.get_target_ports(connector)
        self.find_targets_from_storage(targets, connector, target_ports)
        utils.require_target_existed(targets)
        self.find_mapped_targets_from_storage(
            mapped_targets, ldev, target_ports)

        unmap_targets['list'] = self.get_unmap_targets_list(
            targets['list'], mapped_targets['list'])
        unmap_targets['list'].sort(reverse=True)
        self.unmap_ldev(unmap_targets, ldev)

    def find_mapped_targets_from_storage(self, targets, ldev, target_ports):
        raise NotImplementedError()

    def get_unmap_targets_list(self, target_list, mapped_list):
        raise NotImplementedError()

    def unmap_ldev(self, targets, ldev):
        raise NotImplementedError()
