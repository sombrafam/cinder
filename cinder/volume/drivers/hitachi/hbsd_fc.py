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
"""Fibre channel Cinder volume driver for Hitachi storage."""

from oslo_config import cfg

from cinder.volume import driver
from cinder.volume.drivers.hitachi import hbsd_common as common
from cinder.volume.drivers.hitachi import hbsd_utils as utils

fc_opts = [
    cfg.BoolOpt(
        'hitachi_zoning_request',
        default=False,
        help='Request for FC Zone creating host group'),
]

_DRIVER_INFO = {
    'proto': 'FC',
    'hba_id': 'wwpns',
    'hba_id_type': 'World Wide Name',
    'msg_id': {
        'target': 308,
    },
    'volume_backend_name': utils.DRIVER_PREFIX + 'FC',
    'volume_opts': fc_opts,
    'volume_type': 'fibre_channel',
}

CONF = cfg.CONF
CONF.register_opts(fc_opts)


class HBSDFCDriver(driver.FibreChannelDriver):
    """Fibre channel Class for hbsd drivers.

    Version history:
        1.0.0 - Initial driver
        1.1.0 - Add manage_existing/manage_existing_get_size/unmanage methods
        1.2.0 - Refactor the drivers to use the storage interfaces in common
    """

    VERSION = common.VERSION

    def __init__(self, *args, **kwargs):
        super(HBSDFCDriver, self).__init__(*args, **kwargs)

        self.configuration.append_config_values(common.common_opts)
        self.configuration.append_config_values(fc_opts)
        self.common = utils.import_object(
            self.configuration, _DRIVER_INFO, **kwargs)

    def check_for_setup_error(self):
        pass

    def create_volume(self, volume):
        return self.common.create_volume(volume)

    def create_volume_from_snapshot(self, volume, snapshot):
        return self.common.create_volume_from_snapshot(volume, snapshot)

    def create_cloned_volume(self, volume, src_vref):
        return self.common.create_cloned_volume(volume, src_vref)

    def delete_volume(self, volume):
        self.common.delete_volume(volume)

    def create_snapshot(self, snapshot):
        return self.common.create_snapshot(snapshot)

    def delete_snapshot(self, snapshot):
        self.common.delete_snapshot(snapshot)

    def local_path(self, volume):
        pass

    def get_volume_stats(self, refresh=False):
        return self.common.get_volume_stats(refresh)

    def copy_volume_data(self, context, src_vol, dest_vol, remote=None):
        super(HBSDFCDriver, self).copy_volume_data(
            context, src_vol, dest_vol, remote)
        self.common.copy_dest_vol_meta_to_src_vol(src_vol, dest_vol)
        self.common.discard_zero_page(dest_vol)

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        super(HBSDFCDriver, self).copy_image_to_volume(
            context, volume, image_service, image_id)
        self.common.discard_zero_page(volume)

    def restore_backup(self, context, backup, volume, backup_service):
        super(HBSDFCDriver, self).restore_backup(
            context, backup, volume, backup_service)
        self.common.discard_zero_page(volume)

    def extend_volume(self, volume, new_size):
        self.common.extend_volume(volume, new_size)

    def manage_existing(self, volume, existing_ref):
        """Manage an existing the storage volume.

        existing_ref is a dictionary of the form:

        {'source-id': <logical device number on storage>}
        """
        return self.common.manage_existing(volume, existing_ref)

    def manage_existing_get_size(self, volume, existing_ref):
        return self.common.manage_existing_get_size(volume, existing_ref)

    def unmanage(self, volume):
        self.common.unmanage(volume)

    def do_setup(self, context):
        self.common.do_setup(context)

    def ensure_export(self, context, volume):
        pass

    def create_export(self, context, volume, connector):
        pass

    def remove_export(self, context, volume):
        pass

    def initialize_connection(self, volume, connector):
        return self.common.initialize_connection(volume, connector)

    def terminate_connection(self, volume, connector, **kwargs):
        self.common.terminate_connection(volume, connector, **kwargs)
