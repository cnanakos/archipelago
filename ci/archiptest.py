# Copyright (C) 2010-2014 GRNET S.A.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from cluster import *
import os
from shutil import copy2 as copy
import datetime

class TestCluster(Cluster):
    def execute_test(self, ci_dir, packages_dir):
        tests = os.path.join(ci_dir, '../tests')
        self.inject_file(tests, '/root')
        self.execute_command('mkdir  -p /etc/ceph/')
        ceph_conf = os.path.join(ci_dir, 'ceph.conf')
        self.inject_file(ceph_conf, '/etc/ceph/ceph.conf')
        ceph_apt = os.path.join(ci_dir, 'ceph.list')
        self.inject_file(ceph_apt, '/etc/apt/sources.list.d/')
        self.install_packages(['ceph-common', 'librados2'])
        self.inject_file(packages_dir, '/root')
        dev_apt = os.path.join(ci_dir, 'dev-grnet.list')
        self.inject_file(dev_apt, '/etc/apt/sources.list.d/')
        self.install_packages(['libxseg0', 'python-xseg', 'libxseg0-dbg'])
        cmd = """dpkg -i \
        python-archipelago_*_amd64.deb          \
        archipelago_*_amd64.deb                 \
        archipelago-dbg_*_amd64.deb             \
        archipelago-rados_*_amd64.deb           \
        archipelago-rados-dbg_*_amd64.deb       \
        archipelago-ganeti_*_amd64.deb"""
        remote_folder = os.path.normpath(packages_dir)
        remote_folder = os.path.basename(remote_folder)
        self.execute_command('cd /root/' + remote_folder + ' ; ' + cmd)
        self.install_packages(['blktap-utils'])
        self.execute_command('python /root/tests/tests.py -v', verbose=True)
#        self.execute_command('python /root/qa/tests.py -v FiledTest', verbose=True)
#        self.execute_command('python /root/qa/tests.py -v MapperdTest', verbose=True)
#        self.execute_command('python /root/qa/tests.py -v VlmcdTest', verbose=True)

        self.execute_command('mkdir  -p /srv/archip/blocks')
        self.execute_command('mkdir  -p /srv/archip/maps')
        self.execute_command('mkdir  -p /mnt/mountpoint')
        self.execute_command('archipelago start')
        self.execute_command('python /root/tests/basictest.py', verbose=True)

if __name__ == '__main__':
    now = datetime.datetime.now().strftime('%b-%d-%I%M%p-%G')
    node = 'archipelago-test ' + now
    token = os.environ['TOKEN']
    token = open(token).read().strip()
    packages_dir = os.environ['PACKAGES_DIR']
    image_id = os.environ['IMAGE_ID']
    image_id = open(image_id).read().strip()
    ci_dir = os.path.dirname(os.path.abspath(__file__))
    conffile = os.path.join(ci_dir, 'config')
    tmpfile = '/tmp/tmpconfig_' + now
    copy(conffile, tmpfile)
    conffile = tmpfile

    tc = TestCluster(conffile=conffile, token=token, servers=node, image_id=image_id)
    tc.create()
    tc.execute_test(ci_dir, packages_dir)
    os.unlink(conffile)
    tc.destroy()

