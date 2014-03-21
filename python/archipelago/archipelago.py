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


import sys
import time

from common import (
    FIRST_COLUMN_WIDTH,
    SECOND_COLUMN_WIDTH,
    ctext,
    pretty_print,
    loaded_module,
    load_module,
    Error,
    XsegSegment,
    ArchipelagoConfig,
    ArchipelagoPeers,
)
from vlmc import showmapped as vlmc_showmapped
from vlmc import get_mapped as vlmc_get_mapped
from blktap import VlmcTapdisk


def start_peer(peer, cli=False):
    if peer.is_running():
        raise Error("Cannot start peer %s. Peer already running" % peer.role)
    if cli:
        s = "Starting %s " % peer.role
        sys.stdout.write(s.ljust(FIRST_COLUMN_WIDTH))
    try:
        peer.start()
    except Error as e:
        if cli:
            sys.stdout.write(ctext("FAILED".ljust(SECOND_COLUMN_WIDTH), 'red'))
            sys.stdout.write("\n")
        raise e
    except Exception as e:
        if cli:
            print e
            sys.stdout.write(ctext("FAILED".ljust(SECOND_COLUMN_WIDTH), 'red'))
            sys.stdout.write("\n")
        raise Error("Cannot start %s" % peer.role)

#TODO configurable
    i = 0
    while not peer.is_running():
        time.sleep(0.1)
        i += 1
        if i > 30:  # 3secs
            if cli:
                sys.stdout.write(ctext("FAILED".ljust(SECOND_COLUMN_WIDTH),
                                       'red'))
                sys.stdout.write("\n")
            raise Error("Couldn't start %s" % peer.role)

    if cli:
        sys.stdout.write(ctext("OK".ljust(SECOND_COLUMN_WIDTH), 'green'))
        sys.stdout.write("\n")


def stop_peer(peer, cli=False):
    try:
        peer.stop()
    except Error:
        if cli:
            pretty_print(peer.role, ctext("Not running", 'yellow'))
        return
    if cli:
        s = "Stopping %s " % peer.role
        sys.stdout.write(s.ljust(FIRST_COLUMN_WIDTH))

    i = 0
    while peer.get_pid():
        time.sleep(0.1)
        i += 1
        if i > 150:
            if cli:
                sys.stdout.write(ctext("FAILED".ljust(SECOND_COLUMN_WIDTH),
                                       'red'))
                sys.stdout.write("\n")
            raise Error("Failed to stop peer %s." % peer.role)
    if cli:
        sys.stdout.write(ctext("OK".ljust(SECOND_COLUMN_WIDTH), 'green'))
        sys.stdout.write("\n")


def peer_running(peer, cli):
    try:
        if peer.is_running():
            if cli:
                pretty_print(peer.role, ctext('running', 'green'))
            return True
        else:
            if cli:
                pretty_print(peer.role, ctext('not running', 'red'))
            return False
    except Error:
        if cli:
            pretty_print(peer.role, ctext("Has valid pidfile but does not "
                                          "seem to be active", 'yellow'))
        return False


def start_peers(peers, config, cli=False):
    for r, _ in config['roles']:
        p = peers[r]
        start_peer(p, cli)


def stop_peers(peers, config, cli=False):
    for r, _ in reversed(config['roles']):
        p = peers[r]
        stop_peer(p, cli)


def start(user=False, role=None, cli=False, **kwargs):
    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    if role:
        try:
            p = peers[role]
        except KeyError:
            raise Error("Invalid peer %s" % role)
        return start_peer(p, cli)

    if user:
        #xseg_segment.create()
        start_peers(peers, config, cli)
        mapped = vlmc_get_mapped()
        if mapped:
            for m in mapped:
                if VlmcTapdisk.is_paused(m.device):
                    VlmcTapdisk.unpause(m.device)
        return

    if status() > 0:
        raise Error("Cannot start. Try stopping first")

    if cli:
        print "===================="
        print "Starting archipelago"
        print "===================="
        print ""
    try:
        #xseg_segment.create()
        #time.sleep(0.5)
        start_peers(peers, config, cli)
        load_module("blktap", None)
    except Exception as e:
        if cli:
            print ctext(e, 'red')
        stop(user, role, cli)


def stop(user=False, role=None, cli=False, **kwargs):
    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    if role:
        try:
            p = peers[role]
        except KeyError:
            raise Error("Invalid peer %s" % role)
        return stop_peer(p, cli)
    if user:
        mapped = vlmc_get_mapped()
        if mapped:
            for m in mapped:
                if not VlmcTapdisk.is_paused(m.device):
                    VlmcTapdisk.pause(m.device)
        stop_peers(peers, config, cli)
        return xseg_segment.destroy()
    #check devices
    if cli:
        print "===================="
        print "Stoping archipelago"
        print "===================="
        print ""
    if not loaded_module("blktap"):
        stop_peers(peers, config, cli)
        time.sleep(0.5)
        xseg_segment.destroy()
        return

    if cli:
        if vlmc_showmapped() > 0:
            raise Error("Cannot stop archipelago. Mapped volumes exist")
    else:
        mapped = vlmc_get_mapped()
        if mapped and len(mapped) > 0:
            raise Error("Cannot stop archipelago. Mapped volumes exist")
    stop_peers(peers, config, cli)
    time.sleep(0.5)
    xseg_segment.destroy()


def status(cli=False, **kwargs):
    r = 0
    config = ArchipelagoConfig(kwargs.get('config')).get_config()
    xseg_segment = XsegSegment(config)
    archip_peers = ArchipelagoPeers(config)
    archip_peers.construct_peers(xseg_segment)
    peers = archip_peers.get_peers()
    if not loaded_module("blktap"):
        for role, _ in reversed(config['roles']):
            p = peers[role]
            if peer_running(p, cli):
                r += 1
        if cli:
            pretty_print("blktap", ctext('Not loaded', 'red'))
        return r

    if cli:
        if vlmc_showmapped() > 0:
            r += 1
    else:
        mapped = vlmc_get_mapped()
        if mapped and len(mapped) > 0:
            r += 1
    if loaded_module("blktap"):
        if cli:
            pretty_print("blktap", ctext('Loaded', 'green'))
        #r += 1
    else:
        if cli:
            pretty_print("blktap", ctext('Not loaded', 'red'))
    for role, _ in reversed(config['roles']):
        p = peers[role]
        if peer_running(p, cli):
            r += 1
    return r


def restart(**kwargs):
    stop(**kwargs)
    start(**kwargs)
