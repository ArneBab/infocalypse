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

import os
import sys
import StringIO
import time

from fcpclient import get_usk_hash, get_version, is_usk_file, \
     get_usk_for_usk_version

from validate import is_hex_string

# Similar HACK is used in config.py
import knownrepos # Just need a module to read __file__ from

try:
    #raise ImportError('fake error to test code path')
    __import__('nntplib')
except ImportError, err:
    # djk20090506 tested this code path.
    # nntplib doesn't ship with the Windoze binary hg distro.
    # so we do some hacks to use a local copy.
    #print
    #print "No nntplib? This doesn't look good."
    PARTS = os.path.split(os.path.dirname(knownrepos.__file__))
    if PARTS[-1] != 'infocalypse':
        print "nntplib is missing and couldn't hack path. Giving up. :-("
    else:
        PATH = os.path.join(PARTS[0], 'python2_5_files')
        sys.path.append(PATH)
    #print ("Put local copies of python2.5 ConfigParser.py, "
    #       + "nntplib.py and netrc.py in path...")
    #print
# REDFLAG: Research.
# Can't catch ImportError? Always aborts. ???
import nntplib

def get_connection(fms_host, fms_port, user_name):
    """ Create an fms NNTP connection. """
    return nntplib.NNTP(fms_host, fms_port, user_name)

MSG_TEMPLATE = """From: %s
Newsgroups: %s
Subject: %s

%s"""

# Please use this function for good and not evil.
def send_msgs(server, msg_tuples, send_quit=False):
    """ Send messages via fms.
    msg_tuple format is: (sender, group, subject, text, send_callback)

    send_callback is optional.

    If it is present and not None send_callback(message_tuple)
    is invoked after each message is sent.

    It is legal to include additional client specific fields.
    """

    for msg_tuple in msg_tuples:
        raw_msg = MSG_TEMPLATE % (msg_tuple[0],
                                  msg_tuple[1],
                                  msg_tuple[2],
                                  msg_tuple[3])
        in_file = StringIO.StringIO(raw_msg)
        try:
            server.post(in_file)

            if len(msg_tuple) > 4 and not msg_tuple[4] is None:
                # Sent notifier
                msg_tuple[4](msg_tuple)

            if send_quit:
                server.quit()
        finally:
            in_file.close()

def get_nntp_trust(server, kind, fms_id):
    """ INTERNAL: Helper to make a single XGETTRUST request. """
    assert not server is None
    result = server.shortcmd("XGETTRUST %s %s" %
                             (kind, fms_id)).split(' ')
    try:
        code = int(result[0])
    except ValueError:
        raise nntplib.NNTPError("Couldn't parse return code from XGETTRUST.")

    if code < 200 or code > 299:
        raise nntplib.NNTPError("Unexpected return code[%i] from XGETTRUST." %
                             code)
    if result[1] == 'null':
        return None

    return int(result[1])

def get_trust(server, fms_id):
    """ INTERNAL: Fetch trust values via multiple XGETTRUST calls. """
    return tuple([get_nntp_trust(server, kind, fms_id) for kind in
                  ('MESSAGE','TRUSTLIST', 'PEERMESSAGE', 'PEERTRUSTLIST')])

class TrustCache:
    """ Cached interface to FMS trust values. """

    # REQUIRES: server was connected with auth_fms_id we want trust info from.
    def __init__(self, server, timeout_secs=1*60*60):
        # fms_id -> (timeout_secs, trust_tuple)
        self.table = {}
        self.timeout_secs = timeout_secs
        self.server = server

    def flush(self):
        """ Flush the cache. """
        self.table = {}

    def prefetch_trust(self, fms_ids):
        """ Fetch and cache trust values as nescessary.

            If you know the required fms_ids call this
            once with the ids before get_trust() to
            minimize load on the FMS server. """

        for fms_id in fms_ids:
            if (not self.table.get(fms_id, None) is None and
                self.table[fms_id][0] > time.time()):
                print "%s cached for %i more secs. (prefetch)" % (
                    fms_id, (self.table[fms_id][0] - time.time()))
                continue
            self.table[fms_id] = (time.time() + self.timeout_secs,
                                  get_trust(self.server, fms_id))
    def get_trust(self, fms_id):
        """ Return (MESSAGE, TRUSTLIST, PEERMESSAGE, PEERTRUSTLIST)
            trust values.

            Can contain None entries if the trust was 'null'. """

        cached = self.table.get(fms_id, None)
        if cached is None or cached[0] < time.time():
            self.prefetch_trust((fms_id, ))
        assert fms_id in self.table
        print "%s cached for %i more secs. (get)" % (
            fms_id, (self.table[fms_id][0] - time.time()))

        return self.table[fms_id][1]

class IFmsMessageSink:
    """ Abstract interface for an fms message handler. """
    def __init__(self):
        pass

    def wants_msg(self, group, items):
        """ Return True if the message should be passed to recv_fms_msg,
            False, otherwise.

            items is an nntplib xover items tuple.
            """
        # Avoid pylint R0922
        # raise NotImplementedError()
        pass

    def recv_fms_msg(self, group, items, lines):
        """ Handle an fms message.

            items is an nntplib xover items tuple.
        """
        # Avoid pylint R0922
        # raise NotImplementedError()
        pass


def article_range(first, last, old_last):
    """ INTERNAL: Helper to determine which articles are required. """
    first = int(first)
    last = int(last)

    if old_last is None: # first fetch
        return (first, last)

    to_fetch = last - old_last
    if to_fetch == 0:
        return (last, last)

    # I doubt this is a problem in practice, but if it is, at
    # least fail explicitly.

    # Couldn't find info on wrapping in RFC 977
    assert to_fetch > 0

    return (last - to_fetch + 1, last)

def recv_msgs(server, msg_sink, groups, max_articles=None, send_quit=False):
    """ Read messages from fms. """

    if max_articles is None:
        max_articles = {}

    for group in groups:
        if max_articles.get(group, 'dummy') == 'dummy':
            #print "ADDING ", group
            max_articles[group] = None

    for group in groups:
        recv_group_msgs(server, group, msg_sink, max_articles)

    if send_quit:
        server.quit()

def recv_group_msgs(server, group, msg_sink, max_articles):
    """ INTERNAL: Helper dispatches messages for a single group. """
    if not group or group.strip() == '':
        raise ValueError("Empty group names are not allowed.")

    try:
        result = server.group(group)
    except nntplib.NNTPTemporaryError, err1:
        # Ignore 411 errors which happen before the local FMS
        # instance has learned about the group.
        print "Skipped: %s because of error: %s" % (group, str(err1))
        return

    if result[1] == '0':
        return

    first, last = article_range(result[2], result[3],
                                max_articles[group])

    #print "READING %s: (%i, %i, %i)" % \
    #      (group, first, last, max(max_articles[group], -1))
    if not max_articles[group] is None and last <= max_articles[group]:
        #print "No articles to fetch."
        #print "continue(0)"
        return  # Already fetched.

    # Doesn't return msg lines as shown in python doc?
    # http://docs.python.org/library/nntplib.html
    # Is this an fms bug?
    result, items = server.xover(str(first), str(last))

    if result.split(' ')[0] != '224':
        # REDFLAG: untested code path
        raise Exception(result)

    for item in items:
        if not msg_sink.wants_msg(group, item):
            #print "continue(1)"
            continue # Hmmmm... were does this continue?
        try:
            result = server.article(item[0])
        except nntplib.NNTPProtocolError, nntp_err:
            # REDFLAG:
            # djk20091224 I haven't seen this trip in a month or so.
            # Research:
            # 0) Database corruption?
            # 1) FMS bug?
            # 2) nntplib bug?
            #
            # djk20091023 If I use execquery.htm to on the message ID
            # that causes this I get nothing back. == db corruption?
            print "SAW NNTPProtocolError: ", items[4]
            if str(nntp_err) !=  '.':
                print "CAN'T HACK AROUND IT. Sorry :-("
                raise
            print "TRYING TO HACK AROUND IT..."
            msg_sink.recv_fms_msg(group, item, [])
            print "continue(2)"
            continue

        if result[0].split(' ')[0] != '220':
            # REDFLAG: untested code path
            raise Exception(result[0])
        pos = result[3].index('')
        lines = []
        if pos != -1:
            lines = result[3][pos + 1:]
        msg_sink.recv_fms_msg(group, item, lines)

    # Only save if all the code above ran without error.
    max_articles[group] = last

############################################################
# Infocalypse specific stuff.
############################################################
# REDFLAG: LATER, move this into fmscmd.py?

# REDFLAG: Research, when exactly?
# Can sometimes see fms ids w/o  human readable part.
def clean_nym(fms_id):
    """ Returns the line noise part of an fms id, after the '@'. """
    pos = fms_id.index('@')
    if pos == -1:
        return fms_id

    return fms_id[pos + 1:]

def to_msg_string(updates, announcements=None,
                  separator='\n'):
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

    # Hmmm... extra loops for assert paranoia.
    for value in announcements:
        assert is_usk_file(value)

    for update in updates:
        assert is_hex_string(update[0], 12)
        assert update[1] >= 0

    tmp = [separator.join(["A:%s" % value for value in announcements]),
           separator.join(["U:%s:%i" % (update[0], update[1])
                           for update in updates])]
    while '' in tmp:
        tmp.remove('')

    return separator.join(tmp)

def parse_updates(fields, updates):
    """ Helper function parses updates. """
    if fields[0] != 'U' or len(fields) < 3:
        return False

    while len(fields) > 0 and fields[0] == 'U' and len(fields) >= 3:
        try:
            if is_hex_string(fields[1]):
                updates.add((fields[1], int(fields[2])))
                fields = fields[3:]
            else:
                break
        except ValueError:
            break
    # Doesn't imply success, just that we tried.
    return True

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
        if parse_updates(fields, updates):
            continue

        if fields[0] == 'A' and len(fields) >= 2:
            try:
                if is_usk_file(fields[1]):
                    announcements.add(fields[1])
                    # Implicit update.
                    updates.add((get_usk_hash(fields[1]),
                                 get_version(fields[1])))
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
    """ Returns a trust map without human readable names in the keys. """
    clean = {}
    for nym in trust_map:
        cleaned = clean_nym(nym)
        if nym in clean:
            print "strip_name -- nym appears multiple times w/ different " \
                  + "name part: " + nym
        clean[cleaned] = list(set(list(trust_map[nym])
                                  + clean.get(cleaned, [])))
    return clean

# Hmmmm... broke into a separate func to appease pylint.
def get_changed(max_trusted, max_untrusted, version_table):
    """ INTERNAL: Helper function used by USKNotificationParser.get_updated. """
    changed = {}
    untrusted = {}
    for usk_hash in version_table:
        if usk_hash in max_trusted:
            changed[usk_hash] = max_trusted[usk_hash]

        if usk_hash in max_untrusted:
            if usk_hash in changed:
                if max_untrusted[usk_hash][0] > changed[usk_hash]:
                    # There was a trusted update, but the untrusted one
                    # was higher.
                    untrusted[usk_hash] = max_untrusted[usk_hash]
            else:
                # No trusted updated
                untrusted[usk_hash] =  max_untrusted[usk_hash]

    return (changed, untrusted)

# REDFLAG: Trust map ids are w/o names.
#
# clean_fms_id -> (set(uri,..), hash-> index, set(full_fms_id, ...))
class USKNotificationParser(IFmsMessageSink):
    """ IFmsMessageSink reads and saves all updates and announcements """
    def __init__(self, trust_map=None):
        IFmsMessageSink.__init__(self)
        self.table = {}
        self.trust_map = trust_map

    def wants_msg(self, dummy, items):
        """ Return True if the message should be passed to recv_fms_msg,
            False, otherwise.

            items is an nntplib xover items tuple.
            """
        # Skip replies, accept everything else.
        if len(items[5]) != 0:
            return False

        if self.trust_map is None:
            return True

        return items[2] in self.trust_map

    def recv_fms_msg(self, dummy, items, lines):
        """ IFmsMessageSink implementation. """
        #print "---\nSender: %s\nSubject: %s\n" % (items[2], items[1])
        clean_id = clean_nym(items[2])
        new_updates, new_announcements = parse(lines, True)
        #if len(new_updates) > 0 or len(new_announcements) > 0:
        #    print "---\nSender: %s\nSubject: %s\n" % (items[2], items[1])

        for update in new_updates:
            self.handle_update(clean_id, items[2], update[0], update[1])

        for announcement in new_announcements:
            self.handle_announcement(clean_id, items[2], announcement)

    def add_default_repos(self, default_repos):
        """ Add table entries from a [(fms_id, usk), ...] list. """
        for repo_entry in default_repos:
            clean_id = clean_nym(repo_entry[0])
            usk_hash = get_usk_hash(repo_entry[1])
            self.handle_announcement(clean_id, repo_entry[0], repo_entry[1])
            # Implicit in announcement
            self.handle_update(clean_id, repo_entry[0], usk_hash,
                               get_version(repo_entry[1]))

    # parse() handles implicit updates in annoucements
    def handle_announcement(self, clean_id, fms_id, usk):
        """ INTERNAL: process a single announcement. """
        entry = self.table.get(clean_id, (set([]), {}, set([])))
        entry[0].add(get_usk_for_usk_version(usk, 0))
        entry[2].add(fms_id)

        self.table[clean_id] = entry

    def handle_update(self, clean_id, fms_id, usk_hash, index):
        """ INTERNAL: process a single update. """
        if index < 0:
            print "handle_update -- skipped negative index!"
            return

        entry = self.table.get(clean_id, (set([]), {}, set([])))
        prev_index = entry[1].get(usk_hash, 0)
        if index > prev_index:
            prev_index = index
        entry[1][usk_hash] = prev_index
        entry[2].add(fms_id)

        self.table[clean_id] = entry

    # REDFLAG: Doesn't deep copy. Passing out refs to stuff in table.
    def invert_table(self):
        """ INTERNAL: Return (clean_id -> fms_id,  usk->set(clean_id, ...),
            repo_hash->usk->set(clean_id, ...)) """
        # clean_id -> fms_id
        fms_id_map = {}
        # usk -> clean_id
        announce_map = {}
        # repo_hash -> clean_id
        update_map = {}

        for clean_id in self.table:
            table_entry = self.table[clean_id]

            # Backmap to the human readable fms ids
            fms_id_map[clean_id] = self.get_human_name(clean_id, table_entry)

            # Accumulate all announcements
            for usk in table_entry[0]:
                entry = announce_map.get(usk, set([]))
                entry.add(clean_id)
                announce_map[usk] = entry

            # Accumulate all updates
            for usk_hash in table_entry[1]:
                entry = update_map.get(usk_hash, set([]))
                entry.add(clean_id)
                update_map[usk_hash] = entry

        return (fms_id_map, announce_map, update_map)

    def get_human_name(self, clean_id, table_entry=None):
        """ INTERNAL: Return a full FMS id from a clean_id. """
        ret = None
        if table_entry is None:
            table_entry = self.table[clean_id]

        for fms_id in table_entry[2]:
            fields = fms_id.split('@')
            if len(fields[0].strip()) > 0:
                ret = fms_id
                break # break inner loop.
        if ret is None:
            # REDFLAG: Nail down when this can happen.
            print "??? saw an fms id with no human readable part ???"
            print list(table_entry[2])[0]
            ret = list(table_entry[2])[0]
        return ret

    # changed, untrusted
    def get_updated(self, trust_map, version_table):
        """ Returns trusted and untrusted changes with respect to
            the version table. """

        clean_trust = strip_names(trust_map)
        # usk_hash -> index
        max_trusted = {}
        # usk_hash -> (index, fms_id)
        max_untrusted = {}
        for clean_id in self.table:
            table_entry = self.table[clean_id]
            for usk_hash in table_entry[1]:
                if not usk_hash in version_table:
                    continue # Not a repo we care about.

                index = table_entry[1][usk_hash]
                if index <= version_table[usk_hash]:
                    continue # Not news. Already know about that index.

                if (clean_id in clean_trust and
                    usk_hash in clean_trust[clean_id]):
                    # Trusted update
                    if not usk_hash in max_trusted:
                        max_trusted[usk_hash] = index
                    elif index > max_trusted[usk_hash]:
                        max_trusted[usk_hash] = index
                else:
                    # Untrusted update
                    fms_id = self.get_human_name(clean_id, table_entry)
                    if not usk_hash in max_untrusted:
                        max_untrusted[usk_hash] = (index, fms_id)
                    elif index > max_untrusted[usk_hash]:
                        max_untrusted[usk_hash] = (index, fms_id)

        # changed is usk_hash->index
        # untrusted is usk_hash->(index, fms_id)
        return get_changed(max_trusted, max_untrusted, version_table)

def show_table(parser, out_func):
    """ Dump the announcements and updates in a human readable format. """
    fms_id_map, announce_map, update_map = parser.invert_table()

    usks = announce_map.keys()
    usks.sort()

    for usk in usks:
        usk_hash = get_usk_hash(usk)
        out_func("USK Hash: %s\n" % usk_hash)
        out_func("USK: %s\n" % usk)
        out_func("Announced by:\n")
        for clean_id in announce_map[usk]:
            out_func("   %s\n" % fms_id_map[clean_id])
        out_func("Updated by:\n")
        for clean_id in update_map[usk_hash]:
            out_func("   %i:%s\n" % (parser.table[clean_id][1][usk_hash],
                                     fms_id_map[clean_id]))
        out_func("\n")
############################################################

DEFAULT_SUBJECT = 'Ignore'
def make_update_msg(fms_id, group, updates, announcements=None,
                    subject=DEFAULT_SUBJECT):
    """ Test function to make message tuples. """
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

def smoke_test():
    """ Smoke test the functions in this module. """

    # REDFLAG: tests for USKNotificationParser ???
    values0 = ((('be68e8feccdd', 23), ('e246cc31bc42', 3)),
               ('USK@kRM~jJVREwnN2qnA8R0Vt8HmpfRzBZ0j4rHC2cQ-0hw,'
                + '2xcoQVdQLyqfTpF2DpkdUIbHFCeL4W~2X1phUYymnhM,AQACAAE/'
                + 'infocalypse.hgext.R1/12', ))

    # Includes implicit update from announcement.
    values2 = ((('be68e8feccdd', 12), ('be68e8feccdd', 23),
                ('e246cc31bc42', 3)),
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

    # Test sig style update strings.
    text = to_msg_string(values0[0], None, ':')
    print text
    values3 = parse(text)
    assert values3 == (values0[0], ())

    # msg = make_update_msg('djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks'
    #                           'infocalypse.tst',
    #                           'infocalypse.tst',
    #                           values0[0],
    #                           values0[1])
    # DOH! This goes over the wire
    #send_msgs('127.0.0.1', 11119, (msg, ))

if __name__ == "__main__":
    smoke_test()
