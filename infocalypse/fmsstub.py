#pylint: disable-msg=C0111
import os
import stat
import time
import traceback

from fms import MSG_TEMPLATE
from fcpconnection import make_id

def read_msg(full_path, default_sender, default_subject, default_group):
    article_num = os.stat(full_path)[stat.ST_MTIME]
    msg_id = "<fake_%s>" % str(article_num)
    reading_header = True
    blank_count = 0
    headers = {}
    lines = []
    for line in open(full_path, 'rb').readlines():
        line = line.strip()
        #print "LINE:", line
        if reading_header:
            if line.strip() == '':
                blank_count += 1
                if blank_count > 0: #DCI: get rid of useless code
                    reading_header = False
                    #print "SAW END OF HEADERS"
                    continue
            else:
                blank_count = 0

            fields = line.split(':')
            if len(fields) < 2:
                continue

            headers[fields[0].lower().strip()] = ':'.join(fields[1:]).strip()
            continue # on purpose.

        lines.append(line)

    # fake xover article tuple + group
    #(article number, subject, poster, date, id, references, size, lines)
    return (article_num,
            headers.get('subject', default_subject),
            headers.get('from', default_sender),
            None, # unused
            msg_id,
            (),
            None, # unused
            lines, # fms doesn't return these
            headers.get('newsgroups', default_group),)

FAKE_TRUST = 65 # Trust value returned for all queries.
class NNTPStub:
    def quit(self):
        print "NNTPStub.quit -- called."
        traceback.print_stack()
        #raise Exception("DCI: forcing stack trace")
    def shortcmd(self, cmd):
        assert cmd.startswith("XGETTRUST")
        return "200 %i" % FAKE_TRUST

class FMSStub:
    def __init__(self, base_dir, group, sender_lut=None):
        self.base_dir = os.path.join(base_dir, '__msg_spool__')
        self.group = group
        if sender_lut is None:
            sender_lut = {}
        self.sender_lut = sender_lut
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    def get_connection(self, fms_host, fms_port, user_name):
        """ Create a fake fms NNTP connection. """
        return NNTPStub()

    def send_msgs(self, dummy_server, msg_tuples, send_quit=False):
        if not os.path.exists(self.base_dir):
            print "FMSStub.send_msg -- THE MESSAGE SPOOL DIR DOESN'T EXIST!"
            raise IOError("Message spool directory doesn't exist.")

        for msg_tuple in msg_tuples:
            # HACK: use lut to map partial -> full fms ids.
            # print "msg_tuple[0]: ", msg_tuple[0]
            # print "sender_lut: ", self.sender_lut
            sender = self.sender_lut.get(msg_tuple[0].split('@')[0],
                                         msg_tuple[0])
            print "sender: ", sender
            if sender != msg_tuple[0]:
                print "fmsstub: FIXED UP %s->%s" % (msg_tuple[0], sender)

            if sender.find('@') == -1:
                raise IOError("Couldn't fixup fms_id: %s. Add it to the LUT."
                              % sender)

            full_path = os.path.join(self.base_dir,
                                     'out_going_%s.txt' % make_id())
            out_file = open(full_path, 'wb')
            try:
                out_file.write(MSG_TEMPLATE % (sender,
                                               msg_tuple[1],
                                               msg_tuple[2],
                                               msg_tuple[3]))
                time.sleep(0.25) # Hack to make sure that modtimes are unique.
            finally:
                out_file.close()

    # OK to bend the rules a little for testing stubs.
    def recv_msgs(self, dummy_server, msg_sink, groups,
                  max_articles=None, dummy_send_quit=False):
        #print "FMSStub.recv_msgs -- called"
        assert not max_articles is None
        assert tuple(groups) == (self.group, )
        if not self.group in max_articles:
            max_articles[self.group] = 0

        by_mtime = {}
        for name in os.listdir(self.base_dir):
            #print name
            mod_time = os.stat(os.path.join(self.base_dir,
                                            name))[stat.ST_MTIME]

            if mod_time in by_mtime:
                print "The msg ID hack in FMSStub failed!!!"
                print "MANUALLY DELETE MSG FILE: ", name

            assert not mod_time in by_mtime
            by_mtime[mod_time] = name

        if len(by_mtime) < 1:
            #print "BAILING OUT, no files."
            return

        times = by_mtime.keys()
        times.sort()
        if times[-1] <= max_articles[self.group]:
            #print "BAILING OUT, no new files."
            return

        for mod_time in times:
            if mod_time <= max_articles[self.group]:
                #print "Skipping, ", mod_time
                continue
            max_articles[self.group] = max(max_articles[self.group], mod_time)
            items = read_msg(os.path.join(self.base_dir, by_mtime[mod_time]),
                             'djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks',
                             'unknown_subject',
                             self.group)
            if items[-1]  != self.group:
                continue

            if not msg_sink.wants_msg(self.group, items):
                print "fmsstub: Rejected by sink: %s" % by_mtime[mod_time]
                continue

            msg_sink.recv_fms_msg(self.group, items, items[-2])
