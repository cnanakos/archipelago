#!/usr/bin/env python

# Copyright 2012 GRNET S.A. All rights reserved.
#
# Redistribution and use in source and binary forms, with or
# without modification, are permitted provided that the following
# conditions are met:
#
#   1. Redistributions of source code must retain the above
#      copyright notice, this list of conditions and the following
#      disclaimer.
#
#   2. Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY GRNET S.A. ``AS IS'' AND ANY EXPRESS
# OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL GRNET S.A OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
# USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of GRNET S.A.
#


import os
import sys
from struct import unpack
from binascii import hexlify
from ctypes import (
    c_uint32,
    c_uint64,
    string_at,
)

from common import (
    Request,
    XsegSegment,
    Xseg_ctx,
    ArchipelagoConfig,
    ArchipelagoPeers,
    Error,
    exclusive,
    loaded_module,
    xseg_reply_hash,
    xseg_reply_info,
    ARCHIP_PREFIX,
    DEVICE_PREFIX,
)

from blktap import (
    VlmcTapdisk,
    TapdiskState,
)


@exclusive()
def get_mapped():
    return VlmcTapdisk.list()


def showmapped(**kwargs):
    mapped = get_mapped()
    if not mapped:
        print "No volumes mapped"
        print ""
        return 0

    try:
        max_len = len(max(mapped, key=lambda x:
                          len(x.volume) if x.volume else 0).volume)
    except TypeError:
        max_len = 0

    print "%*s %*s %*s %*s %*s" % (-10, "id", -max_len - 2, "image", -30,
                                   "device", -8, "state", -5, "PID")

    for m in mapped:
        print "%*s %*s %*s %*s %*s" % (-10, str(m.minor), -max_len - 2,
                                       m.volume, -30, m.device, -8,
                                       TapdiskState[m.state], -5, m.pid)
    return len(mapped)


def is_volume_mapped(volume):
    mapped = get_mapped()
    if not mapped:
        return None

    for m in mapped:
        d_id = m.minor
        target = m.volume
        if target == volume:
            return d_id
    return None


def is_device_mapped(device):
    mapped = get_mapped()
    if not mapped:
        return None

    for m in mapped:
        d_id = m.minor
        target = m.device
        if target == device:
            return d_id
    return None


def create(name, size=None, snap=None, cont_addr=False, **kwargs):
    if len(name) < 6:
        raise Error("Name should have at least len 6")
    if size is None and snap is None:
        raise Error("At least one of the size/snap args must be provided")

    if not snap:
        snap = ""
    if not size:
        size = 0
    else:
        size = size << 20

    ret = False
    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    mport = peers['mapperd'].portno_start
    req = Request.get_clone_request(xseg_ctx, mport, snap, clone=name,
                                    clone_size=size, cont_addr=cont_addr)
    req.submit()
    req.wait()
    ret = req.success()
    req.put()
    xseg_ctx.shutdown()
    if not ret:
        raise Error("vlmc creation failed")


def snapshot(name, snap_name=None, cli=False, **kwargs):
    if len(name) < 6:
        raise Error("Name should have at least len 6")

    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    vport = peers['vlmcd'].portno_start
    req = Request.get_snapshot_request(xseg_ctx, vport, name, snap=snap_name)
    req.submit()
    req.wait()
    ret = req.success()
    req.put()
    xseg_ctx.shutdown()

    if not ret:
        raise Error("vlmc snapshot failed")
    if cli:
        sys.stdout.write("Snapshot name: %s\n" % snap_name)


def hash(name, cli=False, **kwargs):
    if len(name) < 6:
        raise Error("Name should have at least len 6")

    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    mport = peers['mapperd'].portno_start
    req = Request.get_hash_request(xseg_ctx, mport, name)
    req.submit()
    req.wait()
    ret = req.success()
    if ret:
        xhash = req.get_data(xseg_reply_hash).contents
        hash_name = string_at(xhash.target, xhash.targetlen)
    req.put()
    xseg_ctx.shutdown()

    if not ret:
        raise Error("vlmc hash failed")
    if cli:
        sys.stdout.write("Hash name: %s\n" % hash_name)
        return hash_name


def remove(name, **kwargs):
    device = is_volume_mapped(name)
    if device is not None:
        raise Error("Volume %s mapped on device %s%s" % (name, DEVICE_PREFIX,
                    device))

    ret = False
    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    mport = peers['mapperd'].portno_start
    req = Request.get_delete_request(xseg_ctx, mport, name)
    req.submit()
    req.wait()
    ret = req.success()
    req.put()
    xseg_ctx.shutdown()
    if not ret:
        raise Error("vlmc removal failed")


@exclusive()
def map_volume(name, **kwargs):
    if not loaded_module("blktap"):
        raise Error("blktap module not loaded")

    device = is_volume_mapped(name)
    if device is not None:
        raise Error("Volume %s already mapped on device %s%s" % (name,
                    '/dev/xen/blktap-2/tapdev', device))

    try:
        device = VlmcTapdisk.create(name)
        if device:
            sys.stderr.write(device + '\n')
            return device.split(DEVICE_PREFIX)[1]
        raise Error("Cannot map volume '%s'.\n" % name)
    except Exception, reason:
        raise Error(name + ': ' + str(reason))


@exclusive()
def unmap_volume(name, **kwargs):
    if not loaded_module("blktap"):
        raise Error("blktap module not loaded")
    device = name
    try:
        if is_device_mapped(device) is not None:
            busy = VlmcTapdisk.busy_pid(device)
            mounted = VlmcTapdisk.is_mounted(device)
            if not busy and not mounted:
                VlmcTapdisk.destroy(device)
            else:
                if busy:
                    raise Error("Device is busy (PID: %s)." % busy)
                elif mounted:
                    raise Error("Device is mounted. Cannot unmap device.")
            return
        raise Error("Device doesn't exist")
    except Exception, reason:
        raise Error(device + ': ' + str(reason))


# FIXME:
def resize(name, size, **kwargs):
    raise NotImplementedError()


def lock(name, cli=False, **kwargs):
    if len(name) < 6:
        raise Error("Name should have at least len 6")

    name = ARCHIP_PREFIX + name

    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    mbport = peers['blockerm'].portno_start
    req = Request.get_acquire_request(xseg_ctx, mbport, name)
    req.submit()
    req.wait()
    ret = req.success()
    req.put()
    xseg_ctx.shutdown()
    if not ret:
        raise Error("vlmc lock failed")
    if cli:
        sys.stdout.write("Volume locked\n")


def unlock(name, force=False, cli=False, **kwargs):
    if len(name) < 6:
        raise Error("Name should have at least len 6")

    name = ARCHIP_PREFIX + name

    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    mbport = peers['blockerm'].portno_start
    req = Request.get_release_request(xseg_ctx, mbport, name, force=force)
    req.submit()
    req.wait()
    ret = req.success()
    req.put()
    xseg_ctx.shutdown()
    if not ret:
        raise Error("vlmc unlock failed")
    if cli:
        sys.stdout.write("Volume unlocked\n")


def open_volume(name, cli=False, **kwargs):
    if len(name) < 6:
        raise Error("Name should have at least len 6")

    ret = False
    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    vport = peers['vlmcd'].portno_start
    req = Request.get_open_request(xseg_ctx, vport, name)
    req.submit()
    req.wait()
    ret = req.success()
    req.put()
    xseg_ctx.shutdown()
    if not ret:
        raise Error("vlmc open failed")
    if cli:
        sys.stdout.write("Volume opened\n")


def close_volume(name, cli=False, **kwargs):
    if len(name) < 6:
        raise Error("Name should have at least len 6")

    ret = False
    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    vport = peers['vlmcd'].portno_start
    req = Request.get_close_request(xseg_ctx, vport, name)
    req.submit()
    req.wait()
    ret = req.success()
    req.put()
    xseg_ctx.shutdown()
    if not ret:
        raise Error("vlmc close failed")
    if cli:
        sys.stdout.write("Volume closed\n")


def info(name, cli=False, **kwargs):
    if len(name) < 6:
        raise Error("Name should have at least len 6")

    ret = False
    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    xseg_ctx = Xseg_ctx(xseg_segment)
    mport = peers['mapperd'].portno_start
    req = Request.get_info_request(xseg_ctx, mport, name)
    req.submit()
    req.wait()
    ret = req.success()
    if ret:
        size = req.get_data(xseg_reply_info).contents.size
    req.put()
    xseg_ctx.shutdown()
    if not ret:
        raise Error("vlmc info failed")
    if cli:
        sys.stdout.write("Volume %s: size: %d\n" % (name, size))


#def mapinfo(name, verbose=False, **kwargs):
    #if len(name) < 6:
        #raise Error("Name should have at least len 6")

    #if config['STORAGE'] == "rados":
        #import rados
        #cluster = rados.Rados(conffile=config['CEPH_CONF_FILE'])
        #cluster.connect()
        #ioctx = cluster.open_ioctx(config['RADOS_POOL_MAPS'])
        #BLOCKSIZE = 4 * 1024 * 1024
        #try:
            #mapdata = ioctx.read(ARCHIP_PREFIX + name, length=BLOCKSIZE)
        #except Exception:
            #raise Error("Cannot read map data")
        #if not mapdata:
            #raise Error("Cannot read map data")
        #pos = 0
        #size_uint32t = sizeof(c_uint32)
        #version = unpack("<L", mapdata[pos:pos + size_uint32t])[0]
        #pos += size_uint32t
        #size_uint64t = sizeof(c_uint64)
        #size = unpack("Q", mapdata[pos:pos + size_uint64t])[0]
        #pos += size_uint64t
        #blocks = size / BLOCKSIZE
        #nr_exists = 0
        #print ""
        #print "Volume: " + name
        #print "Version: " + str(version)
        #print "Size: " + str(size)
        #for i in range(blocks):
            #exists = bool(unpack("B", mapdata[pos:pos + 1])[0])
            #if exists:
                #nr_exists += 1
            #pos += 1
            #block = hexlify(mapdata[pos:pos + 32])
            #pos += 32
            #if verbose:
                #print block, exists
        #print "Actual disk usage: " + str(nr_exists * BLOCKSIZE),
        #print '(' + str(nr_exists) + '/' + str(blocks) + ' blocks)'

    #elif config['STORAGE'] == "files":
        #raise Error("Mapinfo for file storage not supported")
    #else:
        #raise Error("Invalid storage")


#def list_volumes(**kwargs):
    #if isinstance(peers['blockerm'], Sosd):
        #import rados
        #cluster = rados.Rados(conffile=config['CEPH_CONF_FILE'])
        #cluster.connect()
        #ioctx = cluster.open_ioctx(peers['blockerm'].pool)
        #oi = rados.ObjectIterator(ioctx)
        #for o in oi:
            #name = o.key
            #if name.startswith(ARCHIP_PREFIX) and not name.endswith('_lock'):
                #print name[len(ARCHIP_PREFIX):]
    #elif config['STORAGE'] == "files":
        #raise Error("Vlmc list not supported for files yet")
    #else:
        #raise Error("Invalid storage")
