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
"""Unit tests for Hitachi Block Storage Driver."""

import mock
from os_brick.initiator import connector as brick_connector

from cinder import context
from cinder import db
from cinder import exception
from cinder import test
from cinder.tests.unit import fake_snapshot
from cinder.tests.unit import fake_volume
from cinder import utils
from cinder.volume import configuration as conf
from cinder.volume import driver
from cinder.volume.drivers.hitachi import hbsd_iscsi
from cinder.volume.drivers.hitachi import hbsd_snm2
from cinder.volume.drivers.hitachi import hbsd_utils
from cinder.volume import utils as volume_utils

START_TIME = 0

SUCCEED = 0
FAILED = 1
STDOUT = ""
STDERR = ""
CMD_SUCCEED = (SUCCEED, STDOUT, STDERR)

CONFIG_MAP = {
    'storage_id': 'HUS110_1234',
    'my_ip': '127.0.0.1',
}

COPY_METHOD_THIN = {'key': 'copy_method', 'value': 'THIN'}

# cmd: autargetini -unit HUS110_1234 -refer
AUTARGETINI_RESULT = """
Port 0A  Target Security  ON
  Target Name        iSCSI Name
  003:HBSD-%(my_ip)s iqn-initiator
Port 0B  Target Security  ON
Port 1A  Target Security  ON
  Target Name        iSCSI Name
  003:HBSD-%(my_ip)s iqn-initiator
Port 1B  Target Security  ON
""" % CONFIG_MAP

# cmd: auiscsi -unit HUS110_1234 -refer
AUISCSI_RESULT = """
Port 0A
  Port Number            : 3260
    IPv4 Address         : 172.16.254.200

Port 0B
  Port Number            : 3260
    IPv4 Address         : 192.168.0.205

Port 1A
  Port Number            : 3260
    IPv4 Address         : 192.168.0.212

Port 1B
  Port Number            : 3260
    IPv4 Address         : 192.168.0.213

"""

# cmd: auman -unit HUS110_1234 -help
AUMAN_RESULT = """
Hitachi Storage Navigator Modular 2
Version 28.00
Copyright (C) 2005, 2014, Hitachi, Ltd.

Usage:
  9500V, AMS, WMS, SMS, AMS2000, HUS100
    auman [ -en | -jp ] command_name
"""

# cmd: audppool -unit HUS110_1234 -refer -detail -g -dppoolno 1
AUDPPOOL_NO1_RESULT = """
DP Pool : 1
  Tier Mode                                 : Disable
  RAID Level                                : 1(1D+1D)
  Page Size                                 : 32MB
  Stripe Size                               : 256KB
  Type                                      : SAS
  Rotational Speed                          : 15000rpm
  Encryption                                : N/A
  Status                                    : Normal
  Reconstruction Progress                   : N/A
  Capacity
    Total Capacity                          : 264.0 GB
    Replication Available Capacity          : 264.0 GB
    Consumed Capacity
      Total                                 : 20.0 GB
      User Data                             : 20.0 GB
      Replication Data                      : 0.0 GB
      Management Area                       : 0.0 GB
    Needing Preparation Capacity            : 0.0 GB
  DP Pool Consumed Capacity
    Current Utilization Percent             : 7%
    Early Alert Threshold                   : 40%
    Depletion Alert Threshold               : 50%
    Notifications Active                    : Enable
  Over Provisioning
    Current Over Provisioning Percent       : 7%
    Warning Threshold                       : 100%
    Limit Threshold                         : 130%
    Notifications Active                    : Disable
    Limit Enforcement                       : Disable
  Replication
    Current Replication Utilization Percent : 7%
    Replication Depletion Alert Threshold   : 40%
    Replication Data Released Threshold     : 95%
  Defined LU Count                          : 5
"""

# cmd: audppool -unit HUS110_1234 -refer -detail -g -dppoolno 2
AUDPPOOL_NO2_RESULT = """
DP Pool : 2
"""

# cmd: autargetdef -unit HUS110_1234 -refer
AUTARGETDEF_REFER_RESULT = """
Port 0A
                             Authentication                  Mutual
  Target                     Method         CHAP Algorithm   Authentication
  003:HBSD-%(my_ip)s         CHAP,None      MD5              Disable
    User Name  : ---
    iSCSI Name : iqn-initiator.target
Port 0B
                             Authentication                  Mutual
  Target                     Method         CHAP Algorithm   Authentication
  003:HBSD-%(my_ip)s         CHAP,None      MD5              Disable
    User Name  : ---
    iSCSI Name : iqn-initiator.target
Port 1A
                             Authentication                  Mutual
  Target                     Method         CHAP Algorithm   Authentication
  003:HBSD-%(my_ip)s         CHAP,None      MD5              Disable
    User Name  : ---
    iSCSI Name : iqn-initiator.target
Port 1B

""" % CONFIG_MAP

# cmd: auluref -unit HUS110_1234
AULUREF_REFER_RESULT = """
 LU Capacity Size Group Pool Mode Level Type Speed Encryption of Paths Status
 0 104857600 blocks 256KB N/A 2 Disable 1( 1D+1P) SAS7K 7200rpm N/A 0 Normal
"""

# method: _run_auluref for copy
RUN_AULUREF_RESULT = """
 LU Capacity Size Group Pool Mode Level Type Speed Encryption of Paths Status
 0 4194304 blocks 256KB N/A 30 N/A 5( 2D+1P) SAS7K 7200rpm N/A 1 Normal
 1 204800 blocks 256KB 0 N/A N/A 5( 4D+1P) SAS 10000rpm N/A 0 Normal
 2 4194304 blocks N/A N/A N/A N/A N/A N/A N/A N/A 0 N/A(V-VOL)
 3 409600 blocks 64KB 0 N/A N/A 5( 4D+1P) SAS 10000rpm N/A 0 Normal
 4 4194304 blocks 256KB N/A 30 N/A 5( 2D+1P) SAS7K 7200rpm N/A 1 Normal
 5 4194304 blocks N/A N/A N/A N/A N/A N/A N/A N/A 0 N/A(V-VOL)
 6 6291456 blocks 256KB 3 N/A N/A 1( 1D+1D) SAS7K 7200rpm N/A 0 Normal
 7 20971520 blocks 256KB N/A 50 N/A 5( 3D+1P) SAS 10000rpm N/A 1 Normal
"""

# cmd: auluref -unit HUS110_1234 -lu 0
AULUREF_LU0_RESULT = """
 LU Capacity Size Group Pool Mode Level Type Speed Encryption of Paths Status
 0 104857600 blocks 256KB N/A 2 Disable 1( 1D+1P) SAS7K 7200rpm N/A 0 Normal
"""

# cmd: auluref -unit HUS110_1234 -lu 3
AULUREF_LU3_RESULT = ''

# cmd: auluref -unit HUS110_1234 -lu 4
AULUREF_LU4_RESULT = """
 LU Capacity Size Group Pool Mode Level Type Speed Encryption of Paths Status
 4 104857600 blocks 256KB N/A 2 Disable 1( 1D+1P) SAS7K 7200rpm N/A 0 Normal
"""

# cmd: auluref -unit HUS110_1234 -lu 5
AULUREF_LU5_RESULT = """
 LU Capacity Size Group Pool Mode Level Type Speed Encryption of Paths Status
 5 104857600 blks 256KB N/A 2 Disable 1( 1D+1P) SAS7K 7200rpm N/A 0 N/A(V-VOL)
"""

# cmd: auluref -unit HUS110_1234 -lu 10
AULUREF_LU10_RESULT = """
 LU Capacity Size Group Pool Mode Level Type Speed Encryption of Paths Status
 10 104857600 blocks 256KB N/A 2 Disable 1( 1D+1P) SAS7K 7200rpm N/A 0 Normal
"""

# cmd: auluref -unit HUS110_1234 -lu 11
AULUREF_LU11_RESULT = """
 LU Capacity Size Group Pool Mode Level Type Speed Encryption of Paths Status
 11 104857600 blks 256KB N/A 2 Disable 1( 1D+1P) SAS7K 7200rpm N/A 0 N/A(V-VOL)
"""

# cmd: aureplicationlocal -unit HUS110_1234 -refer -pvol 0
# ''

# cmd: aureplicationlocal -unit HUS110_1234 -refer -pvol 4
AUREPLICATIONLOCAL_PVOL4 = """
SS_LU0004_LU0009                     4         9  Split(100%)                \
                         SnapShot      ---:Ungrouped                N/A      \
           1032
"""

# cmd: aureplicationlocal -unit HUS110_1234 -refer -svol 10
AUREPLICATIONLOCAL_SVOL10 = """
SS_LU0007_LU0010                     7        10  Split(100%)                \
                         SnapShot      ---:Ungrouped                N/A      \
           1032
"""

# cmd: autargetmap -unit HUS110_1234 -refer
AUTARGETMAP_REFER_RESULT = """
Port  Group                                 H-LUN    LUN
  0A  003:HBSD-127.0.0.1                        2      6
  1A  003:HBSD-127.0.0.1                        2      6
"""

EXECUTE_TABLE = {
    ('autargetini', '-unit', CONFIG_MAP['storage_id'], '-refer'): (
        SUCCEED, AUTARGETINI_RESULT, STDERR),
    ('auiscsi', '-unit', CONFIG_MAP['storage_id'], '-refer'): (
        SUCCEED, AUISCSI_RESULT, STDERR),
    ('auman', '-unit', CONFIG_MAP['storage_id'], '-help'): (
        SUCCEED, AUMAN_RESULT, STDERR),
    ('audppool', '-unit', CONFIG_MAP['storage_id'], '-refer', '-detail', '-g',
     '-dppoolno', 1): (
        SUCCEED, AUDPPOOL_NO1_RESULT, STDERR),
    ('audppool', '-unit', CONFIG_MAP['storage_id'], '-refer', '-detail', '-g',
     '-dppoolno', 2): (
        SUCCEED, AUDPPOOL_NO2_RESULT, STDERR),
    ('autargetdef', '-unit', CONFIG_MAP['storage_id'], '-add', '0', 'B',
     '-gname', CONFIG_MAP['my_ip']): (SUCCEED, STDOUT, STDERR),
    ('autargetdef', '-unit', CONFIG_MAP['storage_id'], '-add', '1', 'B',
     '-gname', CONFIG_MAP['my_ip']): (FAILED, STDOUT, STDERR),
    ('autargetdef', '-unit', CONFIG_MAP['storage_id'], '-refer'): (
        SUCCEED, AUTARGETDEF_REFER_RESULT, STDERR),
    ('autargetopt', '-unit', CONFIG_MAP['storage_id'], '-set', '0', 'B',
     '-gno', None, '-ReportFullPortalList', 'enable'): (
        SUCCEED, STDOUT, STDERR),
    ('autargetopt', '-unit', CONFIG_MAP['storage_id'], '-set', '1', 'B',
     '-tno', None, '-ReportFullPortalList', 'enable'): (
        FAILED, STDOUT, STDERR),
    ('auluref', '-unit', CONFIG_MAP['storage_id']): (
        SUCCEED, AULUREF_REFER_RESULT, STDERR),
    ('auluref', '-unit', CONFIG_MAP['storage_id'], '-lu', 0): (
        SUCCEED, AULUREF_LU0_RESULT, STDERR),
    ('auluref', '-unit', CONFIG_MAP['storage_id'], '-lu', 3): (
        SUCCEED, AULUREF_LU3_RESULT, STDERR),
    ('auluref', '-unit', CONFIG_MAP['storage_id'], '-lu', 4): (
        SUCCEED, AULUREF_LU4_RESULT, STDERR),
    ('auluref', '-unit', CONFIG_MAP['storage_id'], '-lu', 5): (
        SUCCEED, AULUREF_LU5_RESULT, STDERR),
    ('auluref', '-unit', CONFIG_MAP['storage_id'], '-lu', 10): (
        SUCCEED, AULUREF_LU10_RESULT, STDERR),
    ('auluref', '-unit', CONFIG_MAP['storage_id'], '-lu', 11): (
        SUCCEED, AULUREF_LU11_RESULT, STDERR),
    ('autargetmap', '-unit', CONFIG_MAP['storage_id'], '-refer'): (
        SUCCEED, AUTARGETMAP_REFER_RESULT, STDERR),
    ('auluadd', '-unit', CONFIG_MAP['storage_id'], '-lu', 1, '-dppoolno', '1',
     '-size', '1g'): (
        SUCCEED, STDOUT, STDERR),
    ('aureplicationlocal', '-unit', CONFIG_MAP['storage_id'], '-refer',
     '-pvol', 4): (
        SUCCEED, AUREPLICATIONLOCAL_PVOL4, STDERR),
    ('aureplicationlocal', '-unit', CONFIG_MAP['storage_id'], '-refer',
     '-svol', 10): (
        SUCCEED, AUREPLICATIONLOCAL_SVOL10, STDERR),
}

DEFAULT_CONNECTOR = {
    'ip': CONFIG_MAP['my_ip'],
    'wwpns': ['0123456789abcdef'],
    'initiator': 'iqn-initiator',
    'multipath': False,
}

TEST_VOLUME0 = {
    'id': 'test-volume0',
    'name': 'test-volume0',
    'provider_location': '0',
    'size': 128,
    'status': 'available',
}

TEST_VOLUME1 = {
    'id': 'test-volume1',
    'name': 'test-volume1',
    'provider_location': '1',
    'size': 256,
    'status': 'available',
}

TEST_VOLUME2 = {
    'id': 'test-volume2',
    'name': 'test-volume2',
    'provider_location': None,
    'size': 128,
    'status': 'creating',
}

TEST_VOLUME3 = {
    'id': 'test-volume3',
    'name': 'test-volume3',
    'provider_location': '3',
    'size': 128,
    'status': 'available',
}

TEST_VOLUME4 = {
    'id': 'test-volume4',
    'name': 'test-volume4',
    'provider_location': '4',
    'size': 128,
    'status': 'available',
}

TEST_VOLUME5 = {
    'id': 'test-volume5',
    'name': 'test-volume5',
    'provider_location': '5',
    'size': 128,
    'status': 'in-use',
}

TEST_VOLUME6 = {
    'id': 'test-volume6',
    'name': 'test-volume6',
    'provider_location': '6',
    'size': 128,
    'status': 'available',
}

TEST_VOLUME7 = {
    'id': 'test-volume7',
    'name': 'test-volume7',
    'provider_location': '7',
    'size': 128,
    'status': 'available',
}

CTXT = context.get_admin_context()

TEST_VOLUME_OBJ = {
    'test-volume0': fake_volume.fake_volume_obj(CTXT, **TEST_VOLUME0),
    'test-volume1': fake_volume.fake_volume_obj(CTXT, **TEST_VOLUME1),
    'test-volume2': fake_volume.fake_volume_obj(CTXT, **TEST_VOLUME2),
    'test-volume3': fake_volume.fake_volume_obj(CTXT, **TEST_VOLUME3),
    'test-volume4': fake_volume.fake_volume_obj(CTXT, **TEST_VOLUME4),
    'test-volume5': fake_volume.fake_volume_obj(CTXT, **TEST_VOLUME5),
    'test-volume6': fake_volume.fake_volume_obj(CTXT, **TEST_VOLUME6),
    'test-volume7': fake_volume.fake_volume_obj(CTXT, **TEST_VOLUME7),
}

TEST_VOLUME_TABLE = {
    'test-volume0': fake_volume.fake_db_volume(**TEST_VOLUME0),
    'test-volume1': fake_volume.fake_db_volume(**TEST_VOLUME1),
    'test-volume2': fake_volume.fake_db_volume(**TEST_VOLUME2),
    'test-volume3': fake_volume.fake_db_volume(**TEST_VOLUME3),
    'test-volume4': fake_volume.fake_db_volume(**TEST_VOLUME4),
    'test-volume5': fake_volume.fake_db_volume(**TEST_VOLUME5),
    'test-volume6': fake_volume.fake_db_volume(**TEST_VOLUME6),
    'test-volume7': fake_volume.fake_db_volume(**TEST_VOLUME7),
}


def _execute(*args, **kargs):
    cmd = args[6:] if args[2] == 'STONAVM_HOME=/usr/stonavm' else args
    result = EXECUTE_TABLE.get(cmd, CMD_SUCCEED)
    return result


def _brick_get_connector_properties(*args, **kwargs):
    return DEFAULT_CONNECTOR


def _connect_volume(*args, **kwargs):
    return {'path': u'/dev/disk/by-path/xxxx', 'type': 'block'}


def _disconnect_volume(*args, **kwargs):
    pass


def _volume_get(context, volume_id):
    return TEST_VOLUME_TABLE.get(volume_id)


def _volume_admin_metadata_get(context, volume_id):
    return {'fake_key': 'fake_value'}


def _volume_metadata_update(context, volume_id, metadata, delete):
    pass


def _snapshot_metadata_update(context, snapshot_id, metadata, delete):
    pass


def _attach_volume(self, context, volume, properties, remote=False):
    return {'device': {'path': 'fake_path'}}


def _detach_volume(self, context, attach_info, volume, properties,
                   force=False, remote=False):
    pass


def _copy_volume(srcstr, deststr, size_in_m, blocksize, sync=False,
                 execute=utils.execute, ionice=None, throttle=None):
    pass


def _fake_run_auluref(*args, **kwargs):
    return RUN_AULUREF_RESULT


class HBSDSNM2ISCSIDriverTest(test.TestCase):
    """Test HBSDSNM2ISCSIDriver."""

    test_snapshot0 = {
        'id': 'test-snapshot0',
        'name': 'test-snapshot0',
        'provider_location': '0',
        'size': 128,
        'status': 'available',
        'volume': TEST_VOLUME_TABLE.get("test-volume0"),
        'volume_id': 'test-volume0',
        'volume_name': 'test-volume0',
        'volume_size': 128,
    }

    test_snapshot1 = {
        'id': 'test-snapshot1',
        'name': 'test-snapshot1',
        'provider_location': '1',
        'size': 256,
        'status': 'available',
        'volume': TEST_VOLUME_TABLE.get("test-volume1"),
        'volume_id': 'test-volume1',
        'volume_name': 'test-volume1',
        'volume_size': 256,
    }

    test_snapshot2 = {
        'id': 'test-snapshot2',
        'name': 'test-snapshot2',
        'provider_location': None,
        'size': 128,
        'status': 'available',
        'volume': TEST_VOLUME_TABLE.get("test-volume2"),
        'volume_id': 'test-volume2',
        'volume_name': 'test-volume2',
        'volume_size': 128,
    }

    test_snapshot3 = {
        'id': 'test-snapshot3',
        'name': 'test-snapshot3',
        'provider_location': '3',
        'size': 128,
        'status': 'available',
        'volume': TEST_VOLUME_TABLE.get("test-volume3"),
        'volume_id': 'test-volume3',
        'volume_name': 'test-volume3',
        'volume_size': 128,
    }

    test_snapshot4 = {
        'id': 'test-snapshot4',
        'name': 'test-snapshot4',
        'provider_location': '4',
        'size': 128,
        'status': 'available',
        'volume': TEST_VOLUME_TABLE.get("test-volume4"),
        'volume_id': 'test-volume4',
        'volume_name': 'test-volume4',
        'volume_size': 128,
    }

    test_snapshot5 = {
        'id': 'test-snapshot5',
        'name': 'test-snapshot5',
        'provider_location': '10',
        'size': 128,
        'status': 'available',
        'volume': TEST_VOLUME_TABLE.get("test-volume7"),
        'volume_id': 'test-volume7',
        'volume_name': 'test-volume7',
        'volume_size': 128,
    }

    test_snapshot6 = {
        'id': 'test-snapshot6',
        'name': 'test-snapshot6',
        'provider_location': '11',
        'size': 128,
        'status': 'available',
        'volume': TEST_VOLUME_TABLE.get("test-volume7"),
        'volume_id': 'test-volume7',
        'volume_name': 'test-volume7',
        'volume_size': 128,
    }

    test_snapshot7 = {
        'id': 'test-snapshot7',
        'name': 'test-snapshot7',
        'provider_location': '12',
        'size': 128,
        'status': 'available',
        'volume': TEST_VOLUME_TABLE.get("test-volume7"),
        'volume_id': 'test-volume7',
        'volume_name': 'test-volume7',
        'volume_size': 128,
    }

    TEST_SNAPSHOT_TABLE = {
        'test-snapshot0': fake_snapshot.fake_db_snapshot(**test_snapshot0),
        'test-snapshot1': fake_snapshot.fake_db_snapshot(**test_snapshot1),
        'test-snapshot2': fake_snapshot.fake_db_snapshot(**test_snapshot2),
        'test-snapshot3': fake_snapshot.fake_db_snapshot(**test_snapshot3),
        'test-snapshot4': fake_snapshot.fake_db_snapshot(**test_snapshot4),
        'test-snapshot5': fake_snapshot.fake_db_snapshot(**test_snapshot5),
        'test-snapshot6': fake_snapshot.fake_db_snapshot(**test_snapshot6),
        'test-snapshot7': fake_snapshot.fake_db_snapshot(**test_snapshot7),
    }

    test_existing_ref = {'source-id': '0'}
    test_existing_none_ldev_ref = {'source-id': '2'}
    test_existing_invalid_ldev_ref = {'source-id': 'AAA'}
    test_existing_value_error_ref = {'source-id': 'XX:XX:XX'}
    test_existing_no_ldev_ref = {}

    def setUp(self, *args):
        super(HBSDSNM2ISCSIDriverTest, self).setUp()

        self.configuration = mock.Mock(conf.Configuration)
        self.ctxt = context.get_admin_context()
        self._setup_config()
        self._setup_driver()

    def _setup_config(self, *args):
        self.configuration.config_group = "SNM2"

        self.configuration.volume_backend_name = "SNM2ISCSI"
        self.configuration.volume_driver = (
            "cinder.volume.drivers.hitachi.hbsd_iscsi.HBSDISCSIDriver")
        self.configuration.reserved_percentage = "0"
        self.configuration.use_multipath_for_image_xfer = False
        self.configuration.enforce_multipath_for_image_xfer = False
        self.configuration.num_volume_device_scan_tries = 3
        self.configuration.volume_dd_blocksize = "1000"

        self.configuration.hitachi_storage_cli = "SNM2"
        self.configuration.hitachi_storage_id = CONFIG_MAP['storage_id']
        self.configuration.hitachi_pool = "1"
        self.configuration.hitachi_thin_pool = None
        self.configuration.hitachi_ldev_range = "0-10"
        self.configuration.hitachi_default_copy_method = 'FULL'
        self.configuration.hitachi_copy_speed = 15
        self.configuration.hitachi_copy_check_interval = 1
        self.configuration.hitachi_async_copy_check_interval = 1
        self.configuration.hitachi_target_ports = "1A"
        self.configuration.hitachi_compute_target_ports = "1A"
        self.configuration.hitachi_group_request = True
        self.configuration.hitachi_driver_cert_mode = False

        self.configuration.hitachi_use_chap_auth = False
        self.configuration.hitachi_auth_user = "HBSD-CHAP-user"
        self.configuration.hitachi_auth_password = "HBSD-CHAP-password"

        self.configuration.safe_get = self._fake_safe_get

    def _fake_safe_get(self, value):
        try:
            val = getattr(self.configuration, value)
        except AttributeError:
            val = None
        return val

    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def _setup_driver(self, *args):
        self.driver = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self.driver.do_setup(None)
        self.driver.check_for_setup_error()
        self.driver.create_export(None, None, None)
        self.driver.ensure_export(None, None)
        self.driver.remove_export(None, None)

    # API test cases
    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        drv.do_setup(None)

    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_create_hostgrp(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        self.configuration.hitachi_target_ports = "0B"
        self.configuration.hitachi_compute_target_ports = "0B"

        drv.do_setup(None)

    @mock.patch.object(hbsd_snm2, '_EXEC_TIMEOUT', 1)
    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_create_hostgrp_error(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        self.configuration.hitachi_target_ports = "1B"
        self.configuration.hitachi_compute_target_ports = "1B"

        self.assertRaises(exception.HBSDError, drv.do_setup, None)

    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_ldev_range_is_none(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        self.configuration.hitachi_ldev_range = None

        drv.do_setup(None)
        self.assertEqual(
            [0, 65535],
            drv.common.storage_info['ldev_range'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_create_volume(self, *args):
        ret = self.driver.create_volume(fake_volume.fake_volume_obj(self.ctxt))
        self.assertEqual('1', ret['provider_location'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_create_volume_free_ldev_not_found(self, *args):
        self.driver.common.storage_info['ldev_range'] = [0, 0]
        self.assertRaises(
            exception.HBSDError, self.driver.create_volume,
            TEST_VOLUME_OBJ.get("test-volume0"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_extend_volume(self, *args):
        self.driver.extend_volume(
            TEST_VOLUME_OBJ.get("test-volume0"), 256)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_extend_volume_ldev_is_none(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.extend_volume,
            TEST_VOLUME_OBJ.get("test-volume2"), 256)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_extend_volume_ldev_is_vvol(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.extend_volume,
            TEST_VOLUME_OBJ.get("test-volume5"), 256)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_get_volume_stats(self, *args):
        stats = self.driver.get_volume_stats(True)
        self.assertEqual('Hitachi', stats['vendor_name'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_get_volume_stats_no_refresh(self, *args):
        stats = self.driver.get_volume_stats()
        self.assertEqual({}, stats)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_get_volume_stats_failed_to_get_dp_pool(self, *args):
        self.driver.common.storage_info['pool_id'] = 2
        stats = self.driver.get_volume_stats(True)
        self.assertEqual({}, stats)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_volume(self, *args):
        self.driver.delete_volume(TEST_VOLUME_OBJ.get("test-volume0"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_volume_ldev_is_none(self, *args):
        self.driver.delete_volume(TEST_VOLUME_OBJ.get("test-volume2"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_volume_ldev_not_found(self, *args):
        self.driver.delete_volume(TEST_VOLUME_OBJ.get("test-volume3"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_volume_volume_is_busy(self, *args):
        self.assertRaises(
            exception.HBSDVolumeIsBusy, self.driver.delete_volume,
            TEST_VOLUME_OBJ.get("test-volume4"))

    @mock.patch.object(
        db, 'snapshot_metadata_update', side_effect=_snapshot_metadata_update)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        hbsd_snm2.HBSDSNM2, '_run_auluref', _fake_run_auluref)
    @mock.patch.object(db, 'volume_get', side_effect=_volume_get)
    def test_create_snapshot_full(self, *args):
        ret = self.driver.create_snapshot(
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot7"))
        self.assertEqual('8', ret['provider_location'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        hbsd_snm2.HBSDSNM2, '_run_auluref', _fake_run_auluref)
    @mock.patch.object(
        db, 'snapshot_metadata_update', side_effect=_snapshot_metadata_update)
    @mock.patch.object(db, 'volume_get', side_effect=_volume_get)
    def test_create_snapshot_thin(self, *args):
        self.configuration.hitachi_thin_pool = 1
        self.configuration.hitachi_default_copy_method = "THIN"

        ret = self.driver.create_snapshot(
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot7"))
        self.assertEqual('8', ret['provider_location'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(db, 'volume_get', side_effect=_volume_get)
    def test_create_snapshot_ldev_is_none(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.create_snapshot,
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot2"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(db, 'volume_get', side_effect=_volume_get)
    def test_create_snapshot_ldev_not_found(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.create_snapshot,
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot3"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_snapshot_full(self, *args):
        self.driver.delete_snapshot(
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot5"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_snapshot_thin(self, *args):
        self.driver.delete_snapshot(
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot6"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_snapshot_ldev_is_none(self, *args):
        self.driver.delete_snapshot(
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot2"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_snapshot_ldev_not_found(self, *args):
        self.driver.delete_snapshot(
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot3"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_snapshot_snapshot_is_busy(self, *args):
        self.assertRaises(
            exception.HBSDSnapshotIsBusy, self.driver.delete_snapshot,
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot4"))

    @mock.patch.object(volume_utils, 'copy_volume', side_effect=_copy_volume)
    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        brick_connector.ISCSIConnector,
        'connect_volume', _connect_volume)
    @mock.patch.object(
        brick_connector.ISCSIConnector,
        'disconnect_volume', _disconnect_volume)
    def test_create_cloned_volume_with_dd(self, *args):
        test_vol_obj = TEST_VOLUME_OBJ.get("test-volume0")
        test_vol_obj.metadata = COPY_METHOD_THIN

        vol = self.driver.create_cloned_volume(
            test_vol_obj, TEST_VOLUME_OBJ.get("test-volume5"))
        self.assertEqual('1', vol['provider_location'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_create_cloned_volume_ldev_is_none(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.create_cloned_volume,
            TEST_VOLUME_OBJ.get("test-volume0"),
            TEST_VOLUME_OBJ.get("test-volume2"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_create_cloned_volume_invalid_size(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.create_cloned_volume,
            TEST_VOLUME_OBJ.get("test-volume0"),
            TEST_VOLUME_OBJ.get("test-volume1"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_create_volume_from_snapshot(self, *args):
        self.configuration.volume_dd_blocksize = 1024

        vol = self.driver.create_volume_from_snapshot(
            TEST_VOLUME_OBJ.get("test-volume0"),
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot0"))

        self.assertEqual('1', vol['provider_location'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_create_volume_from_snapshot_invalid_size(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.create_volume_from_snapshot,
            TEST_VOLUME_OBJ.get("test-volume0"),
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot1"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_create_volume_from_snapshot_ldev_is_none(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.create_volume_from_snapshot,
            TEST_VOLUME_OBJ.get("test-volume0"),
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot2"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        db, 'volume_admin_metadata_get',
        side_effect=_volume_admin_metadata_get)
    def test_initialize_connection(self, *args):
        self.configuration.hitachi_target_ports = ["0A", "1A"]
        rc = self.driver.initialize_connection(
            TEST_VOLUME_OBJ.get("test-volume0"), DEFAULT_CONNECTOR)
        self.assertEqual('iscsi', rc['driver_volume_type'])
        self.assertEqual('iqn-initiator.target', rc['data']['target_iqn'])
        self.assertEqual(0, rc['data']['target_lun'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_initialize_connection_ldev_is_none(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.initialize_connection,
            TEST_VOLUME_OBJ.get("test-volume2"), DEFAULT_CONNECTOR)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        db, 'volume_admin_metadata_get',
        side_effect=_volume_admin_metadata_get)
    def test_initialize_connection_already_attached(self, *args):
        rc = self.driver.initialize_connection(
            TEST_VOLUME_OBJ.get("test-volume6"), DEFAULT_CONNECTOR)
        self.assertEqual('iscsi', rc['driver_volume_type'])
        self.assertEqual('iqn-initiator.target', rc['data']['target_iqn'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_terminate_connection(self, *args):
        self.driver.terminate_connection(
            TEST_VOLUME_OBJ.get("test-volume6"), DEFAULT_CONNECTOR)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_terminate_connection_ldev_is_none(self, *args):
        self.driver.terminate_connection(
            TEST_VOLUME_OBJ.get("test-volume2"), DEFAULT_CONNECTOR)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_terminate_connection_ldev_not_found(self, *args):
        self.configuration.hitachi_target_ports = ["0A", "1A"]

        self.driver.terminate_connection(
            TEST_VOLUME_OBJ.get("test-volume3"), DEFAULT_CONNECTOR)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_terminate_connection_initiator_iqn_not_found(self, *args):
        connector = dict(DEFAULT_CONNECTOR)
        del connector['initiator']

        self.assertRaises(
            exception.HBSDError, self.driver.terminate_connection,
            TEST_VOLUME_OBJ.get("test-volume0"), connector)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_copy_volume_to_image(self, *args):
        image_service = 'fake_image_service'
        image_meta = 'fake_image_meta'

        with mock.patch.object(
                driver.VolumeDriver,
                'copy_volume_to_image') as mock_copy_volume_to_image:
            self.driver.copy_volume_to_image(
                self.ctxt, TEST_VOLUME_OBJ.get("test-volume0"),
                image_service, image_meta)

        mock_copy_volume_to_image.assert_called_with(
            self.ctxt, TEST_VOLUME_OBJ.get("test-volume0"),
            image_service, image_meta)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_manage_existing(self, *args):
        rc = self.driver.manage_existing(
            TEST_VOLUME_OBJ.get("test-volume0"),
            self.test_existing_ref)
        self.assertEqual('0', rc['provider_location'])
        self.assertEqual(0, rc['metadata']['ldev'])
        self.assertEqual(hbsd_utils.NORMAL_LDEV_TYPE, rc['metadata']['type'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_manage_existing_get_size_none_ldev_ref(self, *args):
        self.assertRaises(
            exception.ManageExistingInvalidReference,
            self.driver.manage_existing_get_size,
            TEST_VOLUME_OBJ.get("test-volume0"),
            self.test_existing_none_ldev_ref)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_manage_existing_get_size_invalid_ldev_ref(self, *args):
        self.assertRaises(
            exception.ManageExistingInvalidReference,
            self.driver.manage_existing_get_size,
            TEST_VOLUME_OBJ.get("test-volume0"),
            self.test_existing_invalid_ldev_ref)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_manage_existing_get_size_value_error_ref(self, *args):
        self.assertRaises(
            exception.ManageExistingInvalidReference,
            self.driver.manage_existing_get_size,
            TEST_VOLUME_OBJ.get("test-volume0"),
            self.test_existing_value_error_ref)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_manage_existing_get_size_no_ldev_ref(self, *args):
        self.assertRaises(
            exception.ManageExistingInvalidReference,
            self.driver.manage_existing_get_size,
            TEST_VOLUME_OBJ.get("test-volume0"),
            self.test_existing_no_ldev_ref)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_unmanage(self, *args):
        self.driver.unmanage(TEST_VOLUME_OBJ.get("test-volume0"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_unmanage_ldev_is_none(self, *args):
        self.driver.unmanage(TEST_VOLUME_OBJ.get("test-volume2"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_unmanage_volume_is_busy(self, *args):
        self.assertRaises(
            exception.HBSDVolumeIsBusy,
            self.driver.unmanage, TEST_VOLUME_OBJ.get("test-volume4"))

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        db, 'volume_metadata_update', side_effect=_volume_metadata_update)
    def test_copy_volume_data(self, *args):
        remote = 'fake_remote'

        with mock.patch.object(
                driver.VolumeDriver, 'copy_volume_data') as mock_copy_volume:
            self.driver.copy_volume_data(
                self.ctxt, TEST_VOLUME_OBJ.get("test-volume0"),
                TEST_VOLUME_OBJ.get("test-volume0"), remote)

        mock_copy_volume.assert_called_with(
            self.ctxt, TEST_VOLUME_OBJ.get("test-volume0"),
            TEST_VOLUME_OBJ.get("test-volume0"), remote)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_copy_image_to_volume(self, *args):
        image_service = 'fake_image_service'
        image_id = 'fake_image_id'
        self.configuration.hitachi_horcm_numbers = (400, 401)

        with mock.patch.object(
                driver.VolumeDriver,
                'copy_image_to_volume') as mock_copy_image:
            self.driver.copy_image_to_volume(
                self.ctxt, TEST_VOLUME_OBJ.get("test-volume0"),
                image_service, image_id)

        mock_copy_image.assert_called_with(
            self.ctxt, TEST_VOLUME_OBJ.get("test-volume0"),
            image_service, image_id)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_restore_backup(self, *args):
        backup = 'fake_backup'
        backup_service = 'fake_backup_service'

        with mock.patch.object(
                driver.VolumeDriver,
                'restore_backup') as mock_restore_backup:
            self.driver.restore_backup(
                self.ctxt, backup, TEST_VOLUME_OBJ.get("test-volume0"),
                backup_service)

        mock_restore_backup.assert_called_with(
            self.ctxt, backup, TEST_VOLUME_OBJ.get("test-volume0"),
            backup_service)
