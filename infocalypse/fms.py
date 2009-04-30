""" Code to support sending and receiving update notifications via fms.

    Copyright (C) 2009 Darrell Karbott

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public
    License as published by the Free Software Foundation; either
    version 2.0 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    General Public License for more details.

    You should have received a copy of the GNU General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

    Author: djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks
"""

import nntplib
import StringIO

from fcpclient import get_usk_hash, get_version, is_usk_file, \
     get_usk_for_usk_version


MSG_TEMPLATE = """From: %s
Newsgroups: %s
Subject: %s

%s"""

# msg_tuple = (sender, group, subject, text)
def send_msgs(fms_host, fms_port, msg_tuples):
    """ Send messages via fms.
    msg_tuple format is: (sender, group, subject, text)
    """

    server = nntplib.NNTP(fms_host, fms_port)

    try:
        for msg_tuple in msg_tuples:
            raw_msg = MSG_TEMPLATE % (msg_tuple[0],
                                      msg_tuple[1],
                                      msg_tuple[2],
                                      msg_tuple[3])
            in_file = StringIO.StringIO(raw_msg)
            print raw_msg
            try:
                #server.post(in_file)
                pass
            finally:
                in_file.close()
    finally:
        server.quit()


class IFmsMessageSink:
    def __init__(self):
        pass

    def wants_msg(self, group, item):
        return True

    def recv_fms_msg(self, group, item, lines):
        pass

def recv_msgs(fms_host, fms_port, msg_sink, groups):
    """ Read messages from fms. """
    server = nntplib.NNTP(fms_host, fms_port)
    try:
        for group in groups:
            result = server.group(group)
            if result[1] == '0':
                continue
            # Doesn't return msg lines as shown in python doc?
            # http://docs.python.org/library/nntplib.html
            # Is this an fms bug?
            result, items = server.xover(result[2], result[3])
            if result.split(' ')[0] != '224':
                # REDFLAG: untested code path
                raise Exception(result)
            for item in items:
                if not msg_sink.wants_msg(group, item):
                    continue
                result = server.article(item[0])
                if result[0].split(' ')[0] != '220':
                    # REDFLAG: untested code path
                    raise Exception(result[0])
                pos = result[3].index('')
                lines = []
                if pos != -1:
                    lines = result[3][pos + 1:]
                msg_sink.recv_fms_msg(group, item, lines)
    finally:
        server.quit()

############################################################
# Infocalypse specific stuff.
############################################################
def clean_nym(fms_id):
    pos = fms_id.index('@')
    if pos == -1:
        return fms_id

    return fms_id[pos + 1:]

def to_msg_string(updates, announcements=None):
    """ Dump updates and announcements in a format which can
        be read by parse. """
    if updates is None:
        updates = []

    if announcements is None:
        announcements = []

    # Make sure we always get the same string rep.
    updates = list(updates)
    updates.sort()
    announcements = list(announcements)
    announcements.sort()

    text = ''
    for value in announcements:
        assert is_usk_file(value)
        text += "A:%s\n" % value

    for update in updates:
        assert is_hex_string(update[0], 12)
        assert update[1] >= 0
        text += "U:%s:%i\n" % (update[0], update[1])

    return text

# A grepper, not a parser...
def parse(text, is_lines=False):
    """ Parse updates and announcements from raw text. """
    if is_lines:
        lines = text
    else:
        lines = text.split('\n')

    announcements = set([])
    updates = set([])

    for line in lines:
        line = line.strip() # Handle crlf bs on Windoze.
        fields = line.split(':')
        if fields[0] == 'U' and len(fields) >= 3:
            try:
                if is_hex_string(fields[1]):
                    updates.add((fields[1], int(fields[2])))
            except ValueError:
                continue
        elif fields[0] == 'A' and len(fields) >= 2:
            try:
                if is_usk_file(fields[1]):
                    announcements.add(fields[1])
                    # Implicit update.
                    updates.add((get_usk_hash(fields[1]), get_version(fields[1])))
            except ValueError:
                continue
        # else, silently fail... hmmmm

    # Perhaps a bit too metrosexual...
    # Make sure you always get the same tuple for a given text.
    updates = list(updates)
    updates.sort()
    announcements = list(announcements)
    announcements.sort()
    return (tuple(updates), tuple(announcements))


def strip_names(trust_map):
    clean = {}
    for nym in trust_map:
        cleaned = clean_nym(nym)
        if nym in clean:
            print "strip_name -- nym appears multiple times w/ different " \
                  + "name part: " + nym
        clean[cleaned] = list(set(list(trust_map[nym])
                                  + clean.get(cleaned, [])))
    return clean

# REDFLAG: Trust map ids are w/o names
# 'isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks', not
# 'djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks'
class USKIndexUpdateParser(IFmsMessageSink):
    """ Class which accumulates USK index update notifications
        from fms messages. """
    def __init__(self, trust_map):
        self.trust_map = strip_names(trust_map)
        self.updates = {}

    def wants_msg(self, group, items):
        if len(items[5]) != 0:
            # Skip replies
            return False

        if clean_nym(items[2]) not in self.trust_map:
            #print "Not trusted: ", items[2]
            # Sender not authoritative on any USK.
            return False

        return True

    def recv_fms_msg(self, group, items, lines):
        """ recv_messages message callback implementation. """
        allowed_hashes = self.trust_map[clean_nym(items[2])]

        #print "---\nSender: %s\nSubject: %s\n" % (items[2], items[1])
        for update in parse(lines, True)[0]:
            if update[0] in allowed_hashes:
                # Only update if the nym is trusted *for the specific USK*.
                #print "UPDATING ---\nSender: %s\nSubject: %s\n" % (items[2], items[1])
                self.handle_update(update)

    def handle_update(self, update):
        """ INTERNAL: Handle a single update. """
        index = update[1]
        value = self.updates.get(update[0], index)
        if index >= value:
            self.updates[update[0]] = index

    def updated(self, previous=None):
        """ Returns a USK hash -> index map for USKs which
            have been updated. """
        if previous is None:
            previous = {}
        ret = {}
        for usk_hash in self.updates:
            if not usk_hash in previous:
                ret[usk_hash] = self.updates[usk_hash]
                continue
            if self.updates[usk_hash] > previous[usk_hash]:
                ret[usk_hash] = self.updates[usk_hash]

        return ret

class USKAnnouncementParser(IFmsMessageSink):
    """ Class which accumulates USK announcement notifications
        from fms messages. """
    # None means accept all announcements.
    def __init__(self, trust_map = None):
        if not trust_map is None:
            trust_map = strip_names(trust_map)
        self.trust_map = trust_map
        self.usks = {}

    def wants_msg(self, group, items):
        if len(items[5]) != 0:
            # Skip replies
            return False

        if self.trust_map is None:
            return True

        if clean_nym(items[2]) not in self.trust_map:
            #print "Not trusted: ", items[2]
            # Sender not authoritative on any USK.
            return False

        return True

    def recv_fms_msg(self, group, items, lines):
        #print "---\nSender: %s\nSubject: %s\n" % (items[2], items[1])
        for usk in parse(lines, True)[1]:
            self.handle_announcement(items[2], usk)

    def handle_announcement(self, sender, usk):
        """ INTERNAL: Handle a single announcement """
        usk = get_usk_for_usk_version(usk, 0)
        entry = self.usks.get(usk, [])
        if not sender in entry:
            entry.append(sender)
        self.usks[usk] = entry

HEX_CHARS = frozenset(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                       'a', 'b', 'c', 'd', 'e', 'f'])
# REQUIRES: Lowercase!
def is_hex_string(value, length=12):
    if not length is None:
        if len(value) != length:
            raise ValueError("Expected hex string of length: %i" % length)
    for char in value:
        if not char in HEX_CHARS:
            return False
    return True

############################################################

DEFAULT_SUBJECT = 'Ignore'
def make_update_msg(fms_id, group, updates, announcements=None,
                    subject=DEFAULT_SUBJECT):
    print "updates: ",  updates
    print "announcements: ", announcements

    # fms doesn't want to see the full id?
    fms_id = fms_id.split('@')[0]
    text = to_msg_string(updates, announcements)
    return (fms_id, group, subject, text)

############################################################

MSG_FMT = """---
Sender : %s
Subject: %s
Date   : %s
Group  : %s
%s
---
"""
class MsgSink(IFmsMessageSink):
    def __init__(self):
        IFmsMessageSink.__init__(self)

    def recv_fms_msg(self, group, item, lines):
        print MSG_FMT % (item[2], item[1], item[3], group, '\n'.join(lines))

def smoke_test():
    #    trust_map = {'djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks':
    #                 ('be68e8feccdd', ),}


    trust_map = {'falafel@IxVqeqM0LyYdTmYAf5z49SJZUxr7NtQkOqVYG0hvITw':
                 ('1' * 12, ),
                 'SDiZ@17fy9sQtAvZI~nwDt5xXJkTZ9KlXon1ucEakK0vOFTc':
                 ('2' * 12, ),
                 }

    parser = USKIndexUpdateParser(trust_map)
    recv_msgs('127.0.0.1', 11119, parser, ('test',))
    print
    print "fms updates:"
    print parser.updated()
    print
    print
    parser = USKAnnouncementParser(trust_map)
    recv_msgs('127.0.0.1', 11119, parser, ('test',))
    print
    print "fms announcements:"
    print parser.usks
    print
    print

    values0 = ((('be68e8feccdd', 23), ('e246cc31bc42', 3)),
               ('USK@kRM~jJVREwnN2qnA8R0Vt8HmpfRzBZ0j4rHC2cQ-0hw,'
                + '2xcoQVdQLyqfTpF2DpkdUIbHFCeL4W~2X1phUYymnhM,AQACAAE/'
                + 'infocalypse.hgext.R1/12', ))

    # Includes implicit update from announcement.
    values2 = ((('be68e8feccdd', 12), ('be68e8feccdd', 23), ('e246cc31bc42', 3)),
               ('USK@kRM~jJVREwnN2qnA8R0Vt8HmpfRzBZ0j4rHC2cQ-0hw,'
                + '2xcoQVdQLyqfTpF2DpkdUIbHFCeL4W~2X1phUYymnhM,AQACAAE/'
                + 'infocalypse.hgext.R1/12',))

    # From tuple to string
    print "---"
    print values0

    text = to_msg_string(values0[0], values0[1])
    print "---"
    # And back
    print text
    values1 = parse(text)
    print "---"
    print values1
    # Not values0 because of implicit update.
    assert values1 == values2

    msg = make_update_msg('djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks'
                          'test',
                          'test',
                          values0[0],
                          values0[1])
    send_msgs('127.0.0.1', 11119, (msg, ))

if __name__ == "__main__":
    smoke_test()