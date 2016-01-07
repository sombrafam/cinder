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
"""Unit tests for Hitachi Block Storage Driver."""

import mock
import os
import time

from os_brick.initiator import connector as brick_connector
from oslo_concurrency import processutils

from cinder import context
from cinder import db
from cinder import exception
from cinder import test
from cinder.tests.unit import fake_snapshot
from cinder.tests.unit import fake_volume
from cinder import utils
from cinder.volume import configuration as conf
from cinder.volume import driver
from cinder.volume.drivers.hitachi import hbsd_horcm
from cinder.volume.drivers.hitachi import hbsd_iscsi
from cinder.volume.drivers.hitachi import hbsd_utils
from cinder.volume import utils as volume_utils

from six.moves import range

START_TIME = 0

SUCCEED = 0
STDOUT = ""
STDERR = ""
CMD_SUCCEED = (SUCCEED, STDOUT, STDERR)

CONFIG_MAP = {
    'serial': '492015',
    'my_ip': '127.0.0.1',
}
INST_NUMS = (200, 201)
ERR_INST_NUMS = (202, 203)
CG_MAP = {'cg%s' % x: hbsd_horcm._COPY_GROUP % (
    CONFIG_MAP['my_ip'], CONFIG_MAP['serial'], INST_NUMS[1], x)
    for x in range(3)
}

COPY_METHOD_THIN = {'key': 'copy_method', 'value': 'THIN'}

# cmd: cat /etc/horcm500.conf
CAT_RESULT = r"""
\\.\CMD-%(serial)s:/dev/sd
""" % CONFIG_MAP

# cmd: raidcom get copy_grp
GET_COPY_GRP_RESULT = """
%(cg0)s %(cg0)sP - - None
%(cg1)s %(cg1)sP - - None
%(cg1)s %(cg1)sS - - None
""" % CG_MAP

# cmd: raidcom get device_grp -device_grp_name HBSD-127.0.0.14920150C91P
GET_DEVICE_GRP_MU1P_RESULT = """
%(cg1)sP HBSD-ldev-0-2 0 None
""" % CG_MAP

# cmd: raidcom get device_grp -device_grp_name HBSD-127.0.0.14920150C91S
GET_DEVICE_GRP_MU1S_RESULT = """
%(cg1)sS HBSD-ldev-0-2 2 None
""" % CG_MAP

# cmd: raidcom get hba_iscsi -port CL1-A HBSD-127.0.0.1
GET_HBA_ISCSI_CL1A_HOSTGRP_RESULT = """
CL1-A 0 HBSD-%(my_ip)s iqn-initiator %(serial)s NICK_NAME
""" % CONFIG_MAP

# cmd: raidcom get hba_iscsi -port CL1-A-0
GET_HBA_ISCSI_CL1A0_RESULT = """
CL1-A 0 HBSD-%(my_ip)s iqn-initiator %(serial)s NICK_NAME
""" % CONFIG_MAP

# cmd: raidcom get dp_pool
GET_DP_POOL_RESULT = """
030 POLN 0 6006 6006 75 80 1 14860 32 167477
"""

# cmd: raidcom get dp_pool
GET_DP_POOL_ERROR_RESULT = """
"""

# cmd: raidcom get pool -key opt
GET_POOL_KEYOPT_RESULT = """
030 POLN 30 HBSDPOOL %(serial)s 1 10000 80 - OPEN N
""" % CONFIG_MAP

# cmd: raidcom get hba_wwn -port CL1-A-0
GET_HBA_WWN_CL1A0_RESULT = """
CL1-A 0 HBSD-%(my_ip)s iqn-initiator %(serial)s -
""" % CONFIG_MAP

# cmd: raidcom get hba_wwn -port CL1-B-0
GET_HBA_WWN_CL1B0_RESULT = """
CL1-B 0 HBSD-%(my_ip)s iqn-initiator %(serial)s -
""" % CONFIG_MAP

# cmd: raidcom get host_grp -port CL1-A
GET_HOST_GRP_CL1A_RESULT = """
CL1-A 0 HBSD-%(my_ip)s iqn-initiator.target B S %(serial)s L 8
""" % CONFIG_MAP

# cmd: raidcom get host_grp -port CL1-A -key opt
GET_HOST_GRP_CL1A_KEY_RESULT = """
CL1-A 0 HBSD-%(my_ip)s iqn-initiator.target B S %(serial)s L 8
CL1-A 1 - - - - %(serial)s -
""" % CONFIG_MAP

# cmd: raidcom get host_grp -port CL3-A -key opt
GET_HOST_GRP_CL3A_KEY_RESULT = """
CL3-A 0 - - - - %(serial)s -
""" % CONFIG_MAP

# cmd: raidcom get host_grp -port CL3-B -key opt
GET_HOST_GRP_CL3A_KEY_RESULT = """
CL3-A 0 - - - - %(serial)s -
""" % CONFIG_MAP

# cmd: raidcom get host_grp -port CL1-B
GET_HOST_GRP_CL1B_RESULT = """
CL1-B 0 HBSD-%(my_ip)s iqn-initiator.target B S %(serial)s L 8
""" % CONFIG_MAP

# cmd: raidcom get host_grp -port CL1-B -key opt
GET_HOST_GRP_CL1B_KEY_RESULT = """
CL1-B 0 HBSD-%(my_ip)s iqn-initiator.target B S %(serial)s L 8
CL1-B 1 - - - - %(serial)s -
""" % CONFIG_MAP

# raidcom add host_grp -port CLx-y -host_grp_name HBSD-127.0.0.1
ADD_HOSTGRP_RESULT = """
raidcom: Host group ID 0(0x0) will be used for adding.
"""

# raidcom add host_grp -port CLx-y -host_grp_name HBSD-pair00
ADD_HOSTGRP_PAIR_RESULT = """
raidcom: Host group ID 2(0x2) will be used for adding.
"""

# raidcom add lun -port CL1-A-0 -ldev_id x
ADD_LUN_LUN0_RESULT = """
raidcom: LUN 0(0x0) will be used for adding.
"""

# cmd: raidcom get ldev -ldev_list undefined -cnt 1
GET_LDEV_LDEV_LIST_UNDEFINED = """
LDEV : 1
VOL_TYPE : NOT DEFINED
"""

# cmd: raidcom get ldev -ldev_id 0 -cnt 2 -key front_end (NoLDEV)
GET_LDEV_LDEV0_CNT2_FRONTEND_RESULT = """
 %(serial)s 0 - - NOT DEFINED - - - -
 %(serial)s 1 - - NOT DEFINED - - - -
""" % CONFIG_MAP

# cmd: raidcom get ldev -ldev_id 0 -cnt 2 -key front_end (LDEV)
GET_LDEV_LDEV0_CNT2_FRONTEND_RESULT2 = """
 %(serial)s 0 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 1 - - NOT DEFINED - - - -
""" % CONFIG_MAP

# cmd: raidcom get ldev -ldev_id 0 -cnt 10 -key front_end (LDEV)
GET_LDEV_LDEV0_CNT10_FRONTEND_RESULT = """
 %(serial)s 0 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 1 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 2 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 3 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 4 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 5 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 6 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 7 0 0 OPEN-V-CVS 2097152 - CVS 0
 %(serial)s 8 - - NOT DEFINED - - - -
 %(serial)s 9 - - NOT DEFINED - - - -
""" % CONFIG_MAP

# cmd: raidcom get ldev -ldev_id 0 -check_status NOT DEFINED
GET_LDEV_CHECKSTATUS_ERR = """
raidcom: testing condition has failed with exit(1).
"""

# cmd: raidcom get ldev -ldev_id 0
GET_LDEV_LDEV0_RESULT = """
LDEV : 0
VOL_TYPE : OPEN-V-CVS
VOL_ATTR : CVS : HDP
VOL_Capacity(BLK) : 2097152
NUM_PORT : 0
STS : NML
"""

# cmd: raidcom get ldev -ldev_id 2 -cnt 2
GET_LDEV_LDEV2_CNT2_RESULT = """
LDEV : 2
VOL_TYPE : NOT DEFINED
"""

# cmd: raidcom get ldev -ldev_id 3
GET_LDEV_LDEV3_RESULT = """
LDEV : 3
VOL_TYPE : OPEN-V-CVS
VOL_ATTR : CVS : HDP
VOL_Capacity(BLK) : 2097152
NUM_PORT : 0
STS :
"""

# cmd: raidcom get ldev -ldev_id 4
GET_LDEV_LDEV4_RESULT = """
LDEV : 4
VOL_TYPE : OPEN-V-CVS
VOL_ATTR : CVS : QS : HDP : HDT
VOL_Capacity(BLK) : 2097152
NUM_PORT : 0
STS : NML
"""

# cmd: raidcom get ldev -ldev_id 5
GET_LDEV_LDEV5_RESULT = """
LDEV : 5
VOL_TYPE : OPEN-V-CVS
VOL_ATTR : CVS : HDP : VVOL
VOL_Capacity(BLK) : 2097152
NUM_PORT : 0
STS : NML
"""

# cmd: raidcom get ldev -ldev_id 6
GET_LDEV_LDEV6_RESULT = """
LDEV : 6
VOL_TYPE : OPEN-V-CVS
PORTs : CL1-A-0 0 HBSD-172.0.0.1
VOL_ATTR : CVS : HDP
VOL_Capacity(BLK) : 2097152
NUM_PORT : 1
STS : NML
"""

# cmd: raidcom get ldev -ldev_id 7
GET_LDEV_LDEV7_RESULT = """
LDEV : 7
VOL_TYPE : OPEN-V-CVS
VOL_ATTR : CVS : QS : HDP : HDT
VOL_Capacity(BLK) : 2097152
NUM_PORT : 0
STS : NML
"""

# cmd: raidcom get ldev -ldev_id 10
GET_LDEV_LDEV10_RESULT = """
LDEV : 10
VOL_TYPE : OPEN-V-CVS
VOL_ATTR : CVS : MRCF : HDP : HDT
VOL_Capacity(BLK) : 2097152
NUM_PORT : 1
STS : NML
"""

# cmd: raidcom get ldev -ldev_id 11
GET_LDEV_LDEV11_RESULT = """
LDEV : 11
VOL_TYPE : OPEN-V-CVS
VOL_ATTR : CVS : QS : HDP : HDT
VOL_Capacity(BLK) : 2097152
NUM_PORT : 1
STS : NML
"""

# cmd: raidcom get ldev -ldev_id 12
GET_LDEV_LDEV12_RESULT = """
LDEV : 12
VOL_TYPE : OPEN-V-CVS
VOL_ATTR : CVS : MRCF : HDP : HDT
VOL_Capacity(BLK) : 2097152
NUM_PORT : 1
STS : NML
"""

# cmd: raidcom get lun -port CL1-A-0
GET_LUN_CL1A0_RESULT = """
CL1-A 0 L 4 1 4 - None
CL1-A 0 L 254 1 5 - None
"""

# cmd: raidcom get lun -port CL1-B-0
GET_LUN_CL1B0_RESULT = """
CL1-B 0 L 5 1 4 - None
CL1-B 0 L 254 1 5 - None
"""

# cmd: raidcom get port
GET_PORT_RESULT = """
CL1-A ISCSI TAR AUT 01 Y PtoP Y 0 None - -
CL1-B ISCSI TAR AUT 01 Y PtoP Y 0 None - -
CL3-A ISCSI TAR AUT 01 Y PtoP Y 0 None - -
CL3-B ISCSI TAR AUT 01 Y PtoP Y 0 None - -
"""

# cmd: raidcom get port -port CL1-A -opt key
GET_PORT_CL1A_KEY_RESULT = """
TCP_PORT : 3260
IPV4_ADDR : 192.168.1.1
"""

# cmd: raidcom get port -port CL1-B -opt key
GET_PORT_CL1B_KEY_RESULT = """
TCP_PORT : 3260
IPV4_ADDR : 192.168.6.1
"""

# cmd: raidcom get port -port CL3-A -opt key
GET_PORT_CL3A_KEY_RESULT = """
TCP_PORT : 3260
IPV4_ADDR : 192.168.3.1
"""

# cmd: raidcom get port -port CL3-B -opt key
GET_PORT_CL3B_KEY_RESULT = """
TCP_PORT : 3260
IPV4_ADDR : 192.168.3.2
"""

# cmd: raidcom get snapshot -ldev_id 4
GET_SNAPSHOT_LDEV4_RESULT = """
HBSD-sanp P-VOL PSUS None 4 3 8 18 100 G--- 53ee291f
HBSD-sanp P-VOL PSUS None 4 4 9 18 100 G--- 53ee291f
"""

# cmd: raidcom get snapshot -ldev_id 7
GET_SNAPSHOT_LDEV7_RESULT = """
HBSD-sanp P-VOL PSUS None 7 3 8 18 100 G--- 53ee291f
HBSD-sanp P-VOL PSUS None 7 4 9 18 100 G--- 53ee291f
"""

# cmd: raidcom get snapshot -ldev_id 8
GET_SNAPSHOT_LDEV8_RESULT = """
HBSD-sanp S-VOL SSUS None 8 3 7 18 100 G--- 53ee291f
"""

# cmd: raidcom get snapshot -ldev_id 9
GET_SNAPSHOT_LDEV9_RESULT = """
HBSD-sanp S-VOL SSUS None 9 4 7 18 100 G--- 53ee291f
"""

# cmd: raidcom get snapshot -ldev_id 11
GET_SNAPSHOT_LDEV11_RESULT = """
HBSD-sanp S-VOL SSUS None 11 3 7 18 100 G--- 53ee291f
"""

# cmd: pairdisplay -g HBSD-127.0.0.14920150C90 -d HBSD-ldev-6-10 -CLI -ISI201
PAIRDISPLAY_LDEV7_10_RESULT = """
%(cg0)s HBSD-ldev-7-10 L CL1-A-1 0 0 0 - 7 P-VOL PSUS - 10 -
%(cg0)s HBSD-ldev-7-10 R CL1-A-1 0 1 0 - 10 S-VOL SSUS - 7 -
""" % CG_MAP

# cmd: pairdisplay -g HBSD-127.0.0.14920150C90 -d HBSD-ldev-7-12 -CLI -ISI201
PAIRDISPLAY_LDEV7_12_RESULT = """
%(cg0)s HBSD-ldev-7-12 L CL1-A-1 0 0 0 - 7 P-VOL PSUS - 12 -
%(cg0)s HBSD-ldev-7-12 R CL1-A-1 0 1 0 - 12 S-VOL SSUS - 7 -
""" % CG_MAP

# cmd: raidqry -h
RAIDQRY_RESULT = """
ver&rev: 01-33-03/06
"""

EXECUTE_TABLE = {
    ('add', 'hba_iscsi', '-port', 'CL3-A-0', '-hba_iscsi_name',
     'iqn-initiator'): (253, STDOUT, STDERR),
    ('add', 'host_grp', '-port', 'CL1-A', '-host_grp_name',
     'HBSD-pair00'): (
        SUCCEED, ADD_HOSTGRP_PAIR_RESULT, STDERR),
    ('add', 'host_grp', '-port', 'CL1-B-1', '-host_grp_name',
     'HBSD-pair00'): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('add', 'host_grp', '-port', 'CL3-A', '-host_grp_name',
     'HBSD-' + CONFIG_MAP['my_ip'], '-iscsi_name',
     'iqn-initiator.hbsd-target'): (
        SUCCEED, ADD_HOSTGRP_RESULT, STDERR),
    ('add', 'host_grp', '-port', 'CL3-B', '-host_grp_name',
     'HBSD-' + CONFIG_MAP['my_ip'], '-iscsi_name',
     'iqn-initiator.hbsd-target'): (
        SUCCEED, ADD_HOSTGRP_RESULT, STDERR),
    ('add', 'host_grp', '-port', 'CL3-B', '-host_grp_name',
     'HBSD-pair00'): (
        SUCCEED, ADD_HOSTGRP_PAIR_RESULT, STDERR),
    ('add', 'ldev', '-pool', 30, '-ldev_id',
     '1', '-capacity', '256G', '-emulation', 'OPEN-V'): (
        hbsd_horcm.EX_CMDRJE, STDOUT, 'SSB=0x2E22,0x0001'),
    ('add', 'ldev', '-pool', 30, '-ldev_id',
     '1', '-capacity', '1024G', '-emulation', 'OPEN-V'): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('add', 'lun', '-port', 'CL1-A-0', '-ldev_id', 1, '-lun_id', 0): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('add', 'lun', '-port', 'CL1-A-0', '-ldev_id', 0): (
        SUCCEED, ADD_LUN_LUN0_RESULT, STDERR),
    ('add', 'lun', '-port', 'CL1-A-0', '-ldev_id', 1): (
        SUCCEED, ADD_LUN_LUN0_RESULT, STDERR),
    ('add', 'lun', '-port', 'CL1-A-0', '-ldev_id', 6): (
        hbsd_horcm.EX_CMDRJE, STDOUT, hbsd_horcm._LU_PATH_DEFINED),
    ('add', 'snapshot', '-ldev_id', 7, 19, '-pool', 31, '-snapshot_name',
     'HBSD-snap', '-copy_size', 3): (
        hbsd_horcm.EX_CMDRJE, STDOUT, 'SSB=0x2E11,0x2205'),
    ('cat', '/etc/horcm500.conf'): (SUCCEED, CAT_RESULT, STDERR),
    ('delete', 'host_grp', '-port', 'CL3-A-0',
     'HBSD-' + CONFIG_MAP['my_ip']): (hbsd_horcm.EX_ENOOBJ, STDOUT, STDERR),
    ('delete', 'ldev', '-ldev_id', 2): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('delete', 'ldev', '-ldev_id', 3): (
        hbsd_horcm.EX_CMDRJE, STDOUT, 'SSB=0x2E20,0x0000'),
    ('delete', 'ldev', '-ldev_id', 29): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('delete', 'lun', '-port', 'CL1-A-0', '-ldev_id', 1): (
        hbsd_horcm.EX_CMDRJE, STDOUT, 'SSB=0xB958,0x0233'),
    ('delete', 'lun', '-port', 'CL1-A-0', '-ldev_id', 3): (
        hbsd_horcm.EX_ENOOBJ, STDOUT, STDERR),
    ('delete', 'lun', '-port', 'CL1-A-0', '-ldev_id', 4): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('delete', 'lun', '-port', 'CL1-A-1', '-ldev_id', 1): (
        hbsd_horcm.EX_ENOOBJ, STDOUT, STDERR),
    ('delete', 'lun', '-port', 'CL1-B-0', '-ldev_id', 3): (
        hbsd_horcm.EX_ENOOBJ, STDOUT, STDERR),
    ('delete', 'snapshot', '-ldev_id', 29): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('env', 'HORCMINST=203', 'horcmgr', '-check'): (1, STDOUT, STDERR),
    ('extend', 'ldev', '-ldev_id', 3, '-capacity', '128G'): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('get', 'hba_iscsi', '-port', 'CL1-A', 'HBSD-' + CONFIG_MAP['my_ip']): (
        SUCCEED, GET_HBA_ISCSI_CL1A_HOSTGRP_RESULT, STDERR),
    ('get', 'hba_iscsi', '-port', 'CL1-A-0'): (
        SUCCEED, GET_HBA_ISCSI_CL1A0_RESULT, STDERR),
    ('get', 'copy_grp'): (SUCCEED, GET_COPY_GRP_RESULT, STDERR),
    ('get', 'device_grp', '-device_grp_name', CG_MAP['cg1'] + 'P'): (
        SUCCEED, GET_DEVICE_GRP_MU1P_RESULT, STDERR),
    ('get', 'device_grp', '-device_grp_name', CG_MAP['cg1'] + 'S'): (
        SUCCEED, GET_DEVICE_GRP_MU1S_RESULT, STDERR),
    ('get', 'dp_pool'): (SUCCEED, GET_DP_POOL_RESULT, STDERR),
    ('get', 'pool', '-key', 'opt'): (SUCCEED, GET_POOL_KEYOPT_RESULT, STDERR),
    ('get', 'hba_wwn', '-port', 'CL1-A-0'): (
        SUCCEED, GET_HBA_WWN_CL1A0_RESULT, STDERR),
    ('get', 'hba_wwn', '-port', 'CL1-B-0'): (
        SUCCEED, GET_HBA_WWN_CL1B0_RESULT, STDERR),
    ('get', 'host_grp', '-port', 'CL1-A'): (
        SUCCEED, GET_HOST_GRP_CL1A_RESULT, STDERR),
    ('get', 'host_grp', '-port', 'CL1-B'): (
        SUCCEED, GET_HOST_GRP_CL1B_RESULT, STDERR),
    ('get', 'host_grp', '-port', 'CL1-A', '-key', 'host_grp'): (
        SUCCEED, GET_HOST_GRP_CL1A_KEY_RESULT, STDERR),
    ('get', 'host_grp', '-port', 'CL1-B', '-key', 'host_grp'): (
        SUCCEED, GET_HOST_GRP_CL1B_KEY_RESULT, STDERR),
    ('get', 'host_grp', '-port', 'CL3-A', '-key', 'host_grp'): (
        SUCCEED, GET_HOST_GRP_CL3A_KEY_RESULT, STDERR),
    ('get', 'ldev', '-ldev_list', 'undefined', '-cnt', '1'): (
        SUCCEED, GET_LDEV_LDEV_LIST_UNDEFINED, STDERR),
    ('get', 'ldev', '-ldev_id', 0, '-cnt', 2, '-key', 'front_end'): (
        SUCCEED, GET_LDEV_LDEV0_CNT2_FRONTEND_RESULT2, STDERR),
    ('get', 'ldev', '-ldev_id', 0, '-cnt', 10, '-key', 'front_end'): (
        SUCCEED, GET_LDEV_LDEV0_CNT10_FRONTEND_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 0, '-check_status', 'NOT', 'DEFINED'): (
        1, STDOUT, GET_LDEV_CHECKSTATUS_ERR),
    ('get', 'ldev', '-ldev_id', 0): (SUCCEED, GET_LDEV_LDEV0_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 2, '-check_status', 'NML', '-time', 120): (
        233, STDOUT, STDERR),
    ('get', 'ldev', '-ldev_id', 2, '-cnt', 2): (
        SUCCEED, GET_LDEV_LDEV2_CNT2_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 3): (SUCCEED, GET_LDEV_LDEV3_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 4): (SUCCEED, GET_LDEV_LDEV4_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 5): (SUCCEED, GET_LDEV_LDEV5_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 6): (SUCCEED, GET_LDEV_LDEV6_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 7): (SUCCEED, GET_LDEV_LDEV7_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 10): (SUCCEED, GET_LDEV_LDEV10_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 11): (SUCCEED, GET_LDEV_LDEV11_RESULT, STDERR),
    ('get', 'ldev', '-ldev_id', 12): (SUCCEED, GET_LDEV_LDEV12_RESULT, STDERR),
    ('get', 'lun', '-port', 'CL1-A-0'): (
        SUCCEED, GET_LUN_CL1A0_RESULT, STDERR),
    ('get', 'lun', '-port', 'CL1-B-0'): (
        SUCCEED, GET_LUN_CL1B0_RESULT, STDERR),
    ('get', 'port'): (SUCCEED, GET_PORT_RESULT, STDERR),
    ('get', 'port', '-port', 'CL1-A', '-key', 'opt'): (
        SUCCEED, GET_PORT_CL1A_KEY_RESULT, STDERR),
    ('get', 'port', '-port', 'CL1-B', '-key', 'opt'): (
        SUCCEED, GET_PORT_CL1B_KEY_RESULT, STDERR),
    ('get', 'port', '-port', 'CL3-A', '-key', 'opt'): (
        SUCCEED, GET_PORT_CL3A_KEY_RESULT, STDERR),
    ('get', 'port', '-port', 'CL3-B', '-key', 'opt'): (
        SUCCEED, GET_PORT_CL3B_KEY_RESULT, STDERR),
    ('get', 'snapshot', '-ldev_id', 4): (
        SUCCEED, GET_SNAPSHOT_LDEV4_RESULT, STDERR),
    ('get', 'snapshot', '-ldev_id', 7): (
        SUCCEED, GET_SNAPSHOT_LDEV7_RESULT, STDERR),
    ('get', 'snapshot', '-ldev_id', 8): (
        SUCCEED, GET_SNAPSHOT_LDEV8_RESULT, STDERR),
    ('get', 'snapshot', '-ldev_id', 9): (
        SUCCEED, GET_SNAPSHOT_LDEV9_RESULT, STDERR),
    ('get', 'snapshot', '-ldev_id', 11): (
        SUCCEED, GET_SNAPSHOT_LDEV11_RESULT, STDERR),
    ('horcmshutdown.sh', 203): (2, STDOUT, STDERR),
    ('horcmstart.sh', 202): (2, STDOUT, STDERR),
    ('modify', 'host_grp', '-port', 'CL3-A', 'HBSD-' + CONFIG_MAP['my_ip'],
     '-host_mode', 'LINUX', '-host_mode_opt', 83): (
        hbsd_horcm.EX_COMERR, STDOUT, STDERR),
    ('modify', 'ldev', '-ldev_id', 3, '-status', 'discard_zero_page'): (
        hbsd_horcm.EX_CMDIOE, STDOUT, STDERR),
    ('pairdisplay', '-CLI', '-d', '%s' % CONFIG_MAP['serial'], 10, 0,
     '-IM%s' % INST_NUMS[1]): (
        SUCCEED, PAIRDISPLAY_LDEV7_10_RESULT, STDERR),
    ('pairdisplay', '-CLI', '-d', '%s' % CONFIG_MAP['serial'],
     12, 0, '-IM%s' % INST_NUMS[1]): (
        SUCCEED, PAIRDISPLAY_LDEV7_12_RESULT, STDERR),
    ('pairevtwait', '-d', 'HBSD-ldev-0-1', '-nowait',
     '-ISI%s' % INST_NUMS[1]): (hbsd_horcm.PSUS, STDOUT, STDERR),
    ('pairevtwait', '-g', CG_MAP['cg0'], '-d', 'HBSD-ldev-0-1', '-nowaits',
     '-ISI%s' % INST_NUMS[1]): (hbsd_horcm.PSUS, STDOUT, STDERR),
    ('pairevtwait', '-g', CG_MAP['cg0'], '-d', 'HBSD-ldev-3-1', '-nowait',
     '-ISI%s' % INST_NUMS[1]): (hbsd_horcm.PSUS, STDOUT, STDERR),
    ('pairevtwait', '-d', CONFIG_MAP['serial'], '-nowaits',
     '-ISI%s' % INST_NUMS[1]): (hbsd_horcm.EX_ENAUTH, STDOUT, STDERR),
    ('pairevtwait', '-d', CONFIG_MAP['serial'], '-nowaits',
     '-ISI%s' % INST_NUMS[1]): (hbsd_horcm.SMPL, STDOUT, STDERR),
    ('pairevtwait', '-d', CONFIG_MAP['serial'], 1, '-nowaits',
     '-IM%s' % INST_NUMS[1]): (hbsd_horcm.COPY, STDOUT, STDERR),
    ('pairevtwait', '-d', CONFIG_MAP['serial'], 8, '-nowaits',
     '-IM%s' % INST_NUMS[1]): (hbsd_horcm.COPY, STDOUT, STDERR),
    ('pairevtwait', '-d', CONFIG_MAP['serial'], 10, '-nowaits',
     '-IM%s' % INST_NUMS[1]): (hbsd_horcm.SMPL, STDOUT, STDERR),
    ('pairevtwait', '-d', CONFIG_MAP['serial'], 12, '-nowaits',
     '-IM%s' % INST_NUMS[1]): (hbsd_horcm.SMPL, STDOUT, STDERR),
    ('raidqry', '-h'): (SUCCEED, RAIDQRY_RESULT, STDERR),
    ('tee', '/etc/horcm501.conf'): (1, STDOUT, STDERR),
    ('-login', 'userX', 'paswordX'): (hbsd_horcm.EX_ENAUTH, STDOUT, STDERR),
    ('-login', 'userY', 'paswordY'): (hbsd_horcm.EX_COMERR, STDOUT, STDERR),
}

ERROR_EXECUTE_TABLE = {
    ('get', 'dp_pool'): (SUCCEED, GET_DP_POOL_ERROR_RESULT, STDERR),
}

DEFAULT_CONNECTOR = {
    'ip': CONFIG_MAP['my_ip'],
    'wwpns': ['aaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbb'],
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
    'status': 'available',
}

TEST_VOLUME6 = {
    'id': 'test-volume6',
    'name': 'test-volume6',
    'provider_location': '6',
    'size': 128,
    'status': 'in-use',
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


def _access(*args, **kargs):
    return True


def _execute(*args, **kargs):
    cmd = args[1:-3] if args[0] == 'raidcom' else args
    result = EXECUTE_TABLE.get(cmd, CMD_SUCCEED)
    return result


def _error_execute(*args, **kargs):
    cmd = args[1:-3] if args[0] == 'raidcom' else args
    result = _execute(*args, **kargs)
    ret = ERROR_EXECUTE_TABLE.get(cmd)
    return ret if ret else result


def _time():
    global START_TIME
    START_TIME += (hbsd_horcm._EXEC_MAX_WAITTIME)
    return START_TIME


def _brick_get_connector_properties(*args, **kwargs):
    return DEFAULT_CONNECTOR


def _brick_get_connector_properties_error(*args, **kwargs):
    connector = dict(DEFAULT_CONNECTOR)
    del connector['initiator']
    return connector


def _connect_volume(*args, **kwargs):
    return {'path': u'/dev/disk/by-path/xxxx', 'type': 'block'}


def _disconnect_volume(*args, **kwargs):
    pass


def _copy_volume(*args, **kwargs):
    pass


def _volume_get(context, volume_id):
    return TEST_VOLUME_TABLE.get(volume_id)


def _volume_admin_metadata_get(context, volume_id):
    return {'fake_key': 'fake_value'}


def _volume_metadata_update(context, volume_id, metadata, delete):
    pass


def _snapshot_metadata_update(context, snapshot_id, metadata, delete):
    pass


def _fake_is_smpl(*args):
    return True


def _fake_run_horcmgr(*args):
    return hbsd_horcm._HORCM_RUNNING


def _fake_check_ldev_status(*args, **kwargs):
    return None


def _fake_exists(path):
    return False


class HBSDHORCMISCSIDriverTest(test.TestCase):
    """Test HBSDHORCMFCDriver."""

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
        'status': 'creating',
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

    def setUp(self):
        super(HBSDHORCMISCSIDriverTest, self).setUp()

        self.configuration = mock.Mock(conf.Configuration)
        self.ctxt = context.get_admin_context()
        self._setup_config()
        self._setup_driver()

    def _setup_config(self):
        self.configuration.config_group = "HORCM"

        self.configuration.volume_backend_name = "HORCMFC"
        self.configuration.volume_driver = (
            "cinder.volume.drivers.hitachi.hbsd_fc.HBSDFCDriver")
        self.configuration.reserved_percentage = "0"
        self.configuration.use_multipath_for_image_xfer = False
        self.configuration.enforce_multipath_for_image_xfer = False
        self.configuration.num_volume_device_scan_tries = 3
        self.configuration.volume_dd_blocksize = "1000"

        self.configuration.hitachi_storage_cli = "HORCM"
        self.configuration.hitachi_storage_id = CONFIG_MAP['serial']
        self.configuration.hitachi_pool = "30"
        self.configuration.hitachi_thin_pool = None
        self.configuration.hitachi_ldev_range = "0-1"
        self.configuration.hitachi_default_copy_method = 'FULL'
        self.configuration.hitachi_copy_speed = 3
        self.configuration.hitachi_copy_check_interval = 1
        self.configuration.hitachi_async_copy_check_interval = 1
        self.configuration.hitachi_target_ports = "CL1-A"
        self.configuration.hitachi_compute_target_ports = "CL1-A"
        self.configuration.hitachi_group_request = True
        self.configuration.hitachi_driver_cert_mode = False

        self.configuration.hitachi_use_chap_auth = False
        self.configuration.hitachi_auth_user = "HBSD-CHAP-user"
        self.configuration.hitachi_auth_password = "HBSD-CHAP-password"

        self.configuration.hitachi_horcm_numbers = INST_NUMS
        self.configuration.hitachi_horcm_user = "user"
        self.configuration.hitachi_horcm_password = "pasword"
        self.configuration.hitachi_horcm_add_conf = False
        self.configuration.hitachi_horcm_enable_resource_group = False
        self.configuration.hitachi_horcm_resource_name = "meta_resource"
        self.configuration.hitachi_horcm_name_only_discovery = False
        self.configuration.hitachi_horcm_pair_target_ports = "CL1-A"
        self.configuration.hitachi_horcm_disable_io_wait = False

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
    @mock.patch.object(time, 'time', side_effect=_time)
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
    @mock.patch.object(time, 'time', side_effect=_time)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()

        drv.do_setup(None)
        self.assertEqual(
            {('CL1-A', '0'): 'iqn-initiator.target'},
            drv.common.storage_info['iqns'])

    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(time, 'time', side_effect=_time)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_create_hostgrp(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        self.configuration.hitachi_target_ports = "CL3-B"

        drv.do_setup(None)

    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(time, 'time', side_effect=_time)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_create_hostgrp_error(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        self.configuration.hitachi_target_ports = "CL3-A"

        self.assertRaises(exception.HBSDError, drv.do_setup, None)

    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(time, 'time', side_effect=_time)
    @mock.patch.object(processutils, 'execute', side_effect=_execute)
    @mock.patch.object(os.path, 'exists', side_effect=_fake_exists)
    @mock.patch.object(os, 'access', side_effect=_access)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_failed_to_create_conf(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        self.configuration.hitachi_horcm_numbers = (500, 501)
        self.configuration.hitachi_horcm_add_conf = True

        self.assertRaises(exception.HBSDError, drv.do_setup, None)

    @mock.patch.object(hbsd_utils, 'DEFAULT_PROCESS_WAITTIME', 1)
    @mock.patch.object(hbsd_horcm, '_EXEC_MAX_WAITTIME', 1)
    @mock.patch.object(hbsd_horcm, '_EXEC_RETRY_INTERVAL', 1)
    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(time, 'time', side_effect=_time)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_failed_to_login(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        self.configuration.hitachi_horcm_user = "userX"
        self.configuration.hitachi_horcm_password = "paswordX"

        self.assertRaises(exception.HBSDError, drv.do_setup, None)

    @mock.patch.object(hbsd_utils, 'DEFAULT_PROCESS_WAITTIME', 1)
    @mock.patch.object(hbsd_horcm, '_EXEC_MAX_WAITTIME', 1)
    @mock.patch.object(hbsd_horcm, '_EXEC_RETRY_INTERVAL', 1)
    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(time, 'time', side_effect=_time)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_failed_to_command(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()
        self.configuration.hitachi_horcm_user = "userY"
        self.configuration.hitachi_horcm_password = "paswordY"

        self.assertRaises(exception.HBSDError, drv.do_setup, None)

    @mock.patch.object(hbsd_utils, 'DEFAULT_PROCESS_WAITTIME', 1)
    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties)
    @mock.patch.object(time, 'time', side_effect=_time)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        hbsd_horcm, '_run_horcmgr', side_effect=_fake_run_horcmgr)
    def test_do_setup_failed_to_horcmshutdown(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)
        self._setup_config()

        self.assertRaises(exception.HBSDError, drv.do_setup, None)

    @mock.patch.object(
        utils, 'brick_get_connector_properties',
        side_effect=_brick_get_connector_properties_error)
    @mock.patch.object(time, 'time', side_effect=_time)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_do_setup_initiator_iqn_not_found(self, *args):
        drv = hbsd_iscsi.HBSDISCSIDriver(
            configuration=self.configuration, db=db)

        self.assertRaises(exception.HBSDError, drv.do_setup, None)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_extend_volume(self, *args):
        self.driver.extend_volume(
            TEST_VOLUME_OBJ.get("test-volume0"), 256)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_extend_volume_volume_ldev_is_none(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.extend_volume,
            TEST_VOLUME_OBJ.get("test-volume2"), 256)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_extend_volume_volume_ldev_is_vvol(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.extend_volume,
            TEST_VOLUME_OBJ.get("test-volume5"), 256)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_extend_volume_volume_is_busy(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.extend_volume,
            TEST_VOLUME_OBJ.get("test-volume4"), 256)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_get_volume_stats(self, *args):
        stats = self.driver.get_volume_stats(True)
        self.assertEqual('Hitachi', stats['vendor_name'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_get_volume_stats_no_refresh(self, *args):
        stats = self.driver.get_volume_stats()
        self.assertEqual({}, stats)

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_error_execute)
    def test_get_volume_stats_failed_to_get_dp_pool(self, *args):

        stats = self.driver.get_volume_stats(True)
        self.assertEqual({}, stats)

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
    def test_create_volume_no_setting_ldev_range(self, *args):
        self.driver.common.storage_info['ldev_range'] = None

        ret = self.driver.create_volume(fake_volume.fake_volume_obj(self.ctxt))
        self.assertEqual('1', ret['provider_location'])

    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        hbsd_horcm.HBSDHORCM,
        '_check_ldev_status', side_effect=_fake_check_ldev_status)
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
            exception.VolumeIsBusy, self.driver.delete_volume,
            TEST_VOLUME_OBJ.get("test-volume4"))

    @mock.patch.object(hbsd_horcm, 'PAIR', hbsd_horcm.PSUS)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        db, 'snapshot_metadata_update', side_effect=_snapshot_metadata_update)
    @mock.patch.object(db, 'volume_get', side_effect=_volume_get)
    def test_create_snapshot_full(self, *args):
        self.driver.common.storage_info['ldev_range'] = [0, 9]
        ret = self.driver.create_snapshot(
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot7"))
        self.assertEqual('8', ret['provider_location'])

    @mock.patch.object(hbsd_horcm, 'PAIR', hbsd_horcm.PSUS)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    @mock.patch.object(
        db, 'snapshot_metadata_update', side_effect=_snapshot_metadata_update)
    @mock.patch.object(db, 'volume_get', side_effect=_volume_get)
    def test_create_snapshot_thin(self, *args):
        self.driver.common.storage_info['ldev_range'] = [0, 9]
        self.configuration.hitachi_thin_pool = 31
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
    @mock.patch.object(
        hbsd_horcm.HBSDHORCM, '_is_smpl', side_effect=_fake_is_smpl)
    def test_delete_snapshot_full_smpl(self, *args):
        self.driver.delete_snapshot(
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot7"))

    @mock.patch.object(hbsd_utils, 'DEFAULT_PROCESS_WAITTIME', 1)
    @mock.patch.object(hbsd_utils, 'execute', side_effect=_execute)
    def test_delete_snapshot_vvol_timeout(self, *args):
        self.assertRaises(
            exception.HBSDError, self.driver.delete_snapshot,
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
            exception.SnapshotIsBusy, self.driver.delete_snapshot,
            self.TEST_SNAPSHOT_TABLE.get("test-snapshot4"))

    @mock.patch.object(hbsd_horcm, 'PAIR', hbsd_horcm.PSUS)
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
        self.configuration.hitachi_ldev_range = [0, 9]
        self.configuration.hitachi_thin_pool = 31
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
        self.configuration.hitachi_target_ports = ["CL1-A", "CL1-B"]

        rc = self.driver.initialize_connection(
            TEST_VOLUME_OBJ.get("test-volume0"), DEFAULT_CONNECTOR)
        self.assertEqual('iscsi', rc['driver_volume_type'])
        self.assertEqual('iqn-initiator.target', rc['data']['target_iqn'])
        self.assertEqual('0', rc['data']['target_lun'])

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
        self.configuration.hitachi_target_ports = ["CL1-A", "CL1-B"]

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
