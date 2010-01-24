#! /usr/bin/env python
"""Quick-quick implementation of WikiWikiWeb in Python
"""
# Modifications: Copyright (C) 2009 Darrell Karbott

# --- original piki copyright notice ---
# Copyright (C) 1999, 2000 Martin Pool <mbp@humbug.org.au>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
# USA

#pylint: disable-msg=C0111, W0331, W0106, C0103
__version__ = '$Revision: 1.62 $'[11:-2];


import cgi, codecs, sys, string, os, re, errno, time
from os import path, environ
from socket import gethostbyaddr
from time import localtime, strftime
# NOTE: cStringIO doesn't work for unicode.
from StringIO import StringIO
import fileoverlay
filefuncs = None

# File to redirect sys.stderr to.
#STDERR_FILE = '/tmp/piki_err' # REDFLAG: Comment out this line
STDERR_FILE = None # Silently drop all output to stderr

PIKI_PNG = 'pikipiki-logo.png'
PIKI_CSS = 'piki.css'
ACTIVELINK = 'activelink.png'
#FAVICON = 'favicon.ico'

# HTTP server doesn't need to serve any other files to make piki work.
PIKI_REQUIRED_FILES = (PIKI_PNG, PIKI_CSS, ACTIVELINK)

# class UnicodeStringIO(StringIO):
#     """ Debugging hack fails early when non low-ASCII 8-bit strings are
#         printed. """
#     def __init__(self, arg=u''):
#         StringIO.__init__(self, arg)
#     def write(self, bytes):
#         if not isinstance(bytes, unicode):
#             # Non-unicode strings should be 7-bit ASCII.
#             # This will raise if the are not.
#             bytes = bytes.decode('ascii')
#         return StringIO.write(self,bytes)

scrub_links = False
LINKS_DISABLED_PAGE = "LinksDisabledWhileEditing"

def scrub(link_text, ss_class=None, force=False):
    """ Cleanup href values so the work with Freenet. """
    if (not scrub_links) and (not force):
        return link_text

    # Handle pages which haven't been written yet gracefully.
    if ss_class == 'nonexistent':
        return "NotWrittenYet"

    if link_text.startswith('/'):
        link_text = link_text[1:]

    if link_text.startswith('freenet:'):
        # HACK: facist fproxy html validator chokes on freenet: links?
        link_text = "/" + link_text[len('freenet:'):]
    # We lean on the fproxy filter to catch external links.
    # hmmm... Do better?
    return link_text

def emit_header():
    print "Content-type: text/html; charset=utf-8"
    print

# Regular expression defining a WikiWord (but this definition
# is also assumed in other places.
word_re_str = r"\b([A-Z][a-z]+){2,}\b"
word_anchored_re = re.compile('^' + word_re_str + '$')
versioned_page_re_str = (r"\b(?P<wikiword>([A-Z][a-z]+){2,})" +
                         r"(_(?P<version>([a-f0-9]{40,40})))?\b")
versioned_page_re = re.compile('^' + versioned_page_re_str + '$')
command_re_str = "(search|edit|fullsearch|titlesearch)\=(.*)"

# Formatting stuff --------------------------------------------------


def get_scriptname():
    return environ.get('SCRIPT_NAME', '')


def send_title(text, link=None, msg=None, is_forked=False):
    print "<head><title>%s</title>" % text
    if css_url:
        print '<link rel="stylesheet" type="text/css" href="%s">' % \
              scrub(css_url)
    print "</head>"
    print '<body><h1>'
    if get_logo_string():
        print link_tag('RemoteChanges', get_logo_string())
    if link:
        classattr = ''
        if is_forked:
            classattr = ' class="forkedtitle" '
        print '<a%s href="%s">%s</a>' % (classattr, scrub(link), text)
    else:
        print text
    print '</h1>'
    if msg: print msg
    print '<hr>'



def link_tag(params, text=None, ss_class=None):
    if text is None:
        text = params                   # default
    if ss_class:
        classattr = 'class="%s" ' % ss_class
    else:
        classattr = ''

    return '<a %s href="%s">%s</a>' % (classattr,
                                       scrub("%s/%s" % (get_scriptname(),
                                                        params), ss_class),
                                       text)

# Search ---------------------------------------------------

def do_fullsearch(needle):
    send_title('Full text search for "%s"' % (needle))

    needle_re = re.compile(needle, re.IGNORECASE)
    hits = []
    all_pages = page_list()
    for page_name in all_pages:
        body = Page(page_name).get_raw_body()
        count = len(needle_re.findall(body))
        if count:
            hits.append((count, page_name))

    # The default comparison for tuples compares elements in order,
    # so this sorts by number of hits
    hits.sort()
    hits.reverse()

    print "<UL>"
    for (count, page_name) in hits:
        print '<LI>' + Page(page_name).link_to()
        print ' . . . . ' + `count`
        print ['match', 'matches'][count <> 1]
    print "</UL>"

    print_search_stats(len(hits), len(all_pages))


def do_titlesearch(needle):
    # TODO: check needle is legal -- but probably we can just accept any
    # RE

    send_title("Title search for \"" + needle + '"')
    
    needle_re = re.compile(needle, re.IGNORECASE)
    all_pages = page_list()
    hits = filter(needle_re.search, all_pages)

    print "<UL>"
    for filename in hits:
        print '<LI>' + Page(filename).link_to()
    print "</UL>"

    print_search_stats(len(hits), len(all_pages))


def print_search_stats(hits, searched):
    print "<p>%d hits " % hits
    print " out of %d pages searched." % searched


def do_edit(pagename):
    Page(pagename).send_editor()

def do_viewsource(pagename):
    Page(pagename).send_editor(True)

def do_viewunmodifiedsource(pagename):
    assert not is_versioned(pagename)
    if not filefuncs.exists(Page(pagename)._text_filename(),
                            True):
        # Doesn't exist!
        send_title("Page Doesn't Exist!", None,
                   "The original version doesn't have a %s page." %
                   pagename)
    else:
        Page(pagename).send_editor(True, True)

def do_savepage(pagename):
    global form
    pg = Page(pagename)
    text = ''
    if 'savetext' in form:
        text = form['savetext'].value
    if text.strip() == '':
        text = ''
        msg = """<b>Locally deleting blank page.</b>"""
    else:
        msg = """<b>Saved local changes. They won't be applied to the
              wiki in Freenet until you explictly <em>submit</em> them. </b>"""

    # Decode the utf8 text from the browser into unicode.
    text = text.decode('utf8')

    # Normalize to UNIX line terminators.
    pg.save_text(text.replace('\r\n', '\n'))

    pg.send_page(msg=msg)

def do_removepage(page_name):
    if not is_versioned(page_name):
        msg = """<b>Locally removed page.</b>"""
    else:
        msg = """<b>Locally marked fork as resolved.</b>"""
    pg = Page(page_name)
    pg.save_text('')
    pg.send_page(msg=msg)

def do_unmodified(pagename):
    if not filefuncs.exists(Page(pagename)._text_filename(),
                            True):
        # Doesn't exist!
        send_title("Page Doesn't Exist!", None,
                   "The original version doesn't have a %s page." %
                   pagename)
    else:
        # Send original.
        Page(pagename).send_page('Original Version', True)

def do_deletelocal(pagename):
    filefuncs.remove_overlay(Page(pagename)._text_filename())

    send_title("Removed Local Edits", None,
               "Removed local edits to %s page." %
               pagename)
    print "Local changes to %s have been deleted. <p>" % Page(
        pagename).link_to()
    print "Here's a link to the %s." % Page('FrontPage').link_to()

def make_index_key():
    s = '<p><center>'
    links = map(lambda ch: '<a href="#%s">%s</a>' % (ch, ch),
                string.lowercase)
    s = s + string.join(links, ' | ')
    s = s + '</center><p>'
    return s


def page_list(include_versioned=False):
    if include_versioned:
        return filter(versioned_page_re.match,
                      filefuncs.list_pages(text_dir))
    return filter(word_anchored_re.match,
                  filefuncs.list_pages(text_dir))

# ----------------------------------------------------------
# Macros
def _macro_TitleSearch():
    return _macro_search("titlesearch")

def _macro_FullSearch():
    return _macro_search("fullsearch")

def _macro_search(type):
    if form.has_key('value'):
        default = form["value"].value.encode('utf8')
    else:
        default = ''
    return """<form method=get accept-charset="UTF-8">
    <input name=%s size=30 value="%s"> 
    <input type=submit value="Go">
    </form>""" % (type, default)

def _macro_GoTo():
    return """<form method=get accept-charset="UTF-8"><input name=goto size=30>
    <input type=submit value="Go">
    </form>"""
    # isindex is deprecated, but it gives the right result here

def _macro_WordIndex():
    s = make_index_key()
    pages = list(page_list())
    map = {}
    word_re = re.compile('[A-Z][a-z]+')
    for name in pages:
        for word in word_re.findall(name):
            try:
                map[word].append(name)
            except KeyError:
                map[word] = [name]

    all_words = map.keys()
    all_words.sort()
    last_letter = None
    for word in all_words:
        letter = string.lower(word[0])
        if letter <> last_letter:
            s = s + '<a name="%s"><h3>%s</h3></a>' % (letter, letter)
            last_letter = letter

        s = s + '<b>%s</b><ul>' % word
        #links = map[word] # <-- Has duplicate links!
        #
        # Quick and dirty fix for muliple link BUG. Revisit.
        links = list(set(map[word]))
        links.sort()
        last_page = None
        for name in links:
            if name == last_page: continue
            s = s + '<li>' + Page(name).link_to()
        s = s + '</ul>'
    return s


def _macro_TitleIndex():
    s = make_index_key()
    pages = list(page_list())
    pages.sort()
    current_letter = None
    for name in pages:
        letter = string.lower(name[0])
        if letter <> current_letter:
            s = s + '<a name="%s"><h3>%s</h3></a>' % (letter, letter)
            current_letter = letter
        else:
            s = s + '<br>'
        s = s + Page(name).link_to()
    return s


def _macro_ActiveLink():
    return '<img src="%s" />' % scrub('/' + ACTIVELINK)


def get_unmerged_versions(overlay, wikitext_dir, page_names):
    # name -> version list
    ret = {}
    for name in page_names:
        ret[name] = set([]) # hmmm paranoia? list ok?

    # REDFLAG: O(N) in total number of pages.  hmmmm...
    for name in overlay.list_pages(wikitext_dir):
        fields = name.split('_')
        if len(fields) < 2:
            continue
        if not fields[0].strip() in page_names:
            continue
        if len(fields[1].strip()) != 40:
            continue
        # hmmmm... validate?
        ret[fields[0].strip()].add(fields[1].strip())

    for name in ret.keys()[:]: # hmmm copy required?
        ret[name] = list(ret[name])
        ret[name].sort()

    return ret

def fork_link(overlay, text_dir_, name, version):
    full_name = '%s_%s' % (name, version)
    css_class = "removedfork"
    if overlay.exists(os.path.join(text_dir_,
                                   full_name)):
        css_class = "existingfork"

    if scrub_links and css_class == "removedfork":
        # Prevent broken linx when dumping freesite.
        full_name = 'AlreadyResolved'

    return link_tag(full_name, '(' + version[:12] + ')',
                    css_class)

def get_fork_html(overlay, text_dir_, name, table):

    return ''.join([fork_link(overlay, text_dir_, name, ver)
                    for ver in table[name]])

def _macro_LocalChanges():
    if not filefuncs.is_overlayed():
        return "<br>Not using overlayed editing!<br>"

    local = set([])
    for name in filefuncs.list_pages(text_dir):
        if filefuncs.has_overlay(os.path.join(text_dir,
                                              name)):
            local.add(name.split('_')[0])
    local = list(local)
    local.sort()
    if len(local) == 0:
        return "<br>No locally edited pages.<br>"

    fork_table = get_unmerged_versions(filefuncs, text_dir, local)
    return '<br>' + '<br>'.join(["%s %s" %
                                 (Page(name).link_to(),
                                  get_fork_html(filefuncs, text_dir,
                                                name, fork_table))
                                 for name in local]) + '<br>'

def get_page_ref(page_name):
    match = versioned_page_re.match(page_name)
    if not match:
        return "ILLEGAL_NAME"
    name = match.group('wikiword')
    ver = match.group('version')
    if not ver:
        return Page(page_name).link_to()

    return "<em>%s(%s)</em>" % (name, ver[:12])

def _macro_RemoteChanges():
    words = ('created', 'modified', 'removed', 'forked')
    reject_reasons = {
        0:"Unknown",
        1:"Server couldn't read submission CHK",
        2:"Insufficient trust",
        3:"Already applied", # Fully applied
        4:"Already applied", # Partially applied.  (not used anymore)
        5:"Conflict",
        6:"Illegal or Malformed submission"
        }

    def file_changes(changes):
        tmps = []

        for index, change in enumerate(changes):
            if len(change) == 0:
                continue
            if index == len(words) - 1:
                # Special case forked files.
                wiki_names = change.keys()
                wiki_names.sort()

                tmps.append("%s:%s" % (words[index],
                                       ','.join([(Page(name).link_to() + " " +
                                                  get_fork_html(filefuncs,
                                                                text_dir,
                                                                name,
                                                                change))
                                                 for name in wiki_names])))
                continue

            tmps.append("%s:%s" % (words[index],
                                   ','.join([get_page_ref(name)
                                             for name in change])))
        return ','.join(tmps)

    def accept_summary(entry, time_tuple):
        return ('<strong>%s accepted from %s</strong><br>%s</br>' %
                (time.strftime(changed_time_fmt, time_tuple),
                 entry[2],
                 file_changes(entry[4])))

    def reject_summary(entry, time_tuple):
        return ('<strong>%s rejected from %s</strong><br>%s</br>' %
                (time.strftime(changed_time_fmt, time_tuple),
                 entry[2],
                 reject_reasons.get(int(entry[4]),
                                    "unknown code:%i" % int(entry[4]))))
    accepted, rejected = read_log_file_entries(data_dir, 20)
    by_time = [(entry[1], True, entry) for entry in accepted]
    for entry in rejected:
        by_time.append((entry[1], False, entry))
    by_time.sort(reverse=True) # Since 2.4. Ok.

    buf = StringIO()
    ratchet_day = None
    for sort_tuple in by_time:
        entry = sort_tuple[2]
        # year, month, day, DoW
        time_tuple = time.gmtime(float(entry[1]))
        day = tuple(time_tuple[0:3])
        if day <> ratchet_day:
            #buf.write('<h3>%s</h3>' % strftime(date_fmt, time_tuple))
            buf.write('<h3>%s</h3>' % strftime(date_fmt, time_tuple))
            ratchet_day = day
        if sort_tuple[1]:
            buf.write(accept_summary(entry, time_tuple))
        else:
            buf.write(reject_summary(entry, time_tuple))
    return buf.getvalue()


def _macro_BookMark():
    # REDFLAG: Revisit.
    # Config file is in the directory above the data_dir directory,
    # so I don't want to depend on that while running.
    # Used info.txt file from the head end instead.

    full_path = os.path.join(data_dir, 'info.txt')
    try:
        in_file = codecs.open(full_path, 'rb', 'ascii')
        usk, desc, link_name = in_file.read().splitlines()[:3]

    except ValueError:
        return "[BookMark macro failed: couldn't parse data from info.txt]"
    except IOError:
        return "[BookMark macro failed: couldn't read data from info.txt]"
    except UnicodeError:
        # REDFLAG: Untested code path.
        return "[BookMark macro failed: illegal encoding in info.txt]"

    if (has_illegal_chars(usk) or
        has_illegal_chars(desc) or
        has_illegal_chars(link_name)):
        return "[BookMark macro failed: illegal html characters in info.txt]"

    if not scrub_links:
        return '<a href="%s">%s</a>' % (LINKS_DISABLED_PAGE, link_name)

    return ('<a href="/?newbookmark=%s&amp;desc=%s">%s</a>'
            % (usk, desc, link_name))


# ----------------------------------------------------------

# REDFLAG: faster way to do this? does it matter?
def has_illegal_chars(value):
    """ Catch illegal characters in image macros. """
    for char in ('<', '>', '&', '\\', "'", '"'):
        if value.find(char) != -1:
            return True
    return False

class PageFormatter:
    """Object that turns Wiki markup into HTML.

    All formatting commands can be parsed one line at a time, though
    some state is carried over between lines.
    """
    def __init__(self, raw):
        self.raw = raw
        self.is_em = self.is_b = 0
        self.list_indents = []
        self.in_pre = 0


    def _emph_repl(self, word):
        if len(word) == 3:
            self.is_b = not self.is_b
            return ['</b>', '<b>'][self.is_b]
        else:
            self.is_em = not self.is_em
            return ['</em>', '<em>'][self.is_em]

    def _rule_repl(self, word):
        s = self._undent()
        if len(word) <= 4:
            s = s + "\n<hr>\n"
        else:
            s = s + "\n<hr size=%d>\n" % (len(word) - 2 )
        return s

    def _word_repl(self, word):
        return Page(word).link_to()


    def _url_repl(self, word):
        if not scrub_links:
            return '<a href="%s">%s</a>' % (LINKS_DISABLED_PAGE, word)

        return '<a href="%s">%s</a>' % (scrub(word), word)

    def _img_repl(self, word):
        # REDFLAG:  Can't handle URIs with '|'. Do better.
        # [[[freenet:keyvalue|alt text|title text]]]
        word = word[3:-3] # grrrrr... _macro_repl is doing this too.
        fields = word.strip().split('|')
        if has_illegal_chars(word):
            return (" <br>[ILLEGAL IMAGE MACRO IN WIKITEXT: " +
                    " illegal characters! ]<br> ")

        uri = scrub(fields[0], None, True)

        # ONLY static images are allowed!
        if not (uri.startswith("/CHK@") or uri.startswith("/SSK@")):
            return (" <br>[ILLEGAL IMAGE MACRO IN WIKITEXT: " +
                    " only CHK@ and SSK@ images allowed! ]<br> ")

        if not scrub_links:
            uri = "" # Images disabled when editing.
        alt_attrib = ' alt="[WIKITEXT IMAGE MACRO MISSING ALT TAG!]" '
        title_attrib = ""
        if len(fields) > 1 and len(fields[1].strip()) > 0:
            alt_attrib = ' alt="%s" ' % fields[1]
        if len(fields) > 2 and len(fields[2].strip()) > 0:
            title_attrib = ' title="%s" ' % fields[2]

        return ' <img src="%s"%s%s/> ' % (uri, alt_attrib, title_attrib)

    def _ent_repl(self, s):
        return {'&': '&amp;',
                '<': '&lt;',
                '>': '&gt;'}[s]

    def _li_repl(self, match):
        return '<li>'


    def _pre_repl(self, word):
        if word == '{{{' and not self.in_pre:
            self.in_pre = 1
            return '<pre>'
        elif self.in_pre:
            self.in_pre = 0
            return '</pre>'
        else:
            return ''

    def _macro_repl(self, word):
        macro_name = word[2:-2]
        # TODO: Somehow get the default value into the search field
        return apply(globals()['_macro_' + macro_name], ())


    def _indent_level(self):
        return len(self.list_indents) and self.list_indents[-1]

    def _indent_to(self, new_level):
        s = ''
        while self._indent_level() > new_level:
            del(self.list_indents[-1])
            s = s + '</ul>\n'
        while self._indent_level() < new_level:
            self.list_indents.append(new_level)
            s = s + '<ul>\n'
        return s

    def _undent(self):
        res = '</ul>' * len(self.list_indents)
        self.list_indents = []
        return res


    def replace(self, match):
        for type, hit in match.groupdict().items():
            if hit:
                return apply(getattr(self, '_' + type + '_repl'), (hit,))
        else:
            raise "Can't handle match " + `match`

    def print_html(self):
        # For each line, we scan through looking for magic
        # strings, outputting verbatim any intervening text
        scan_re = re.compile(
            r"(?:(?P<emph>'{2,3})"
            + r"|(?P<ent>[<>&])"
            + r"|(?P<word>\b(?:[A-Z][a-z]+){2,}\b)"
            + r"|(?P<rule>-{4,})"
            + r"|(?P<img>\[\[\[(freenet\:[^\]]+)\]\]\])"
            + r"|(?P<url>(freenet|http)\:[^\s'\"]+\S)"
            + r"|(?P<li>^\s+\*)"

            + r"|(?P<pre>(\{\{\{|\}\}\}))"
            + r"|(?P<macro>\[\[(TitleSearch|FullSearch|WordIndex"
                            + r"|TitleIndex|ActiveLink"
                            + r"|LocalChanges|RemoteChanges|BookMark|GoTo)\]\])"
            + r")")
        blank_re = re.compile("^\s*$")
        bullet_re = re.compile("^\s+\*")
        indent_re = re.compile("^\s*")
        eol_re = re.compile(r'\r?\n')
        raw = string.expandtabs(self.raw)
        for line in eol_re.split(raw):
            if not self.in_pre:
                # XXX: Should we check these conditions in this order?
                if blank_re.match(line):
                    print '<p>'
                    continue
                indent = indent_re.match(line)
                print self._indent_to(len(indent.group(0)))
            print re.sub(scan_re, self.replace, line)
        if self.in_pre: print '</pre>'
        print self._undent()

# ----------------------------------------------------------
class Page:
    def __init__(self, page_name):
        self.page_name = page_name

    def wiki_name(self):
        return self.page_name.split('_')[0]

    def version(self):
        fields = self.page_name.split('_')
        if len(fields) < 2:
            return ''
        return fields[1]

    def split_title(self):
        # look for the end of words and the start of a new word,
        # and insert a space there
        fields = self.page_name.split('_')
        version = ""
        if len(fields) > 1:
            version = "(" + fields[1][:12] + ")"

        return re.sub('([a-z])([A-Z])', r'\1 \2', fields[0]) + version


    def _text_filename(self):
        return path.join(text_dir, self.page_name)


    def exists(self):
        return filefuncs.exists(self._text_filename())

    def link_to(self):
        word = self.page_name
        if self.exists():
            return link_tag(word)
        else:
            if nonexist_qm:
                return link_tag(word, '?', 'nonexistent') + word
            else:
                return link_tag(word, word, 'nonexistent')


    def get_raw_body(self, unmodified=False):
        try:
            return filefuncs.read(self._text_filename(), 'rb', unmodified)
        except IOError, er:
            if er.errno == errno.ENOENT:
                # just doesn't exist, use default
                return 'Describe %s here.' % self.page_name
            else:
                raise er

    def handled_versioned_page(self, msg=None, unmodified=False):
        if not is_versioned(self.page_name):
            return
        msg = None # Hmmmm...
        full_path = os.path.join(text_dir, self.page_name)
        removed = not filefuncs.exists(full_path, True)
        resolved = filefuncs.has_overlay(full_path)

        link = get_scriptname() + '?fullsearch=' + self.wiki_name()
        send_title(self.split_title(), link, msg, bool(self.version()))
        if unmodified:
            PageFormatter(self.get_raw_body(unmodified)).print_html()
        else:
            if removed:
                print "<b>Already resolved.</b>"
            elif resolved:
                print "<b>Locally marked resolved.</b>"
            else:
                PageFormatter(self.get_raw_body(unmodified)).print_html()

        self.send_footer(True,
                         self._last_modified(),
                         self._text_filename(), unmodified)

        return True

    def send_footer(self, versioned, mod_string=None, page_path=None,
                    unmodified=False):

        #base = get_scriptname() # Hmmm... forget what this was for.
        print '<hr>'
        if is_read_only(data_dir, self.page_name):
            print "<em>The bot owner has marked this page read only.</em>"
            print (('<br><a href="?viewunmodifiedsource=%s">'  %
                    self.page_name) + '[View page source]</a><br>')
            return

        if unmodified:
            print ("<em>Read only original version " +
                   "of a locally modified page.</em>")
            print (('<br><a href="?viewunmodifiedsource=%s">'  %
                    self.page_name) + '[View page source]</a><br>')
            return

        if versioned:
            if page_path is None:
                # Hmmmm...
                return

            if filefuncs.has_overlay(page_path):
                print (('<br><a href="?unmodified=%s">'  % self.page_name) +
                       '[Show original version]</a><br>')
                print (('<a href="?deletelocal=%s">' % self.page_name) +
                       '[Mark unresolved, without confirmation!]</a><br>')

            else:
                if filefuncs.exists(page_path, True):
                    print "<em>This is an unmerged fork of another page!</em>"
                    print (('<br><a href="?viewsource=%s">' %
                            self.page_name) +
                           '[View page source]</a><br>')
                    print (('<br><a href="?removepage=%s">' %
                            self.page_name) +
                           '[Locally mark resolved, ' +
                           'without confirmation!]</a><br>')

            print "<p><em>Wiki dir: %s </em>" % data_dir
            return

        if not page_path is None and filefuncs.has_overlay(page_path):
            print "<strong>This page has local edits!</strong><br>"

        if not page_path is None:
            name = os.path.split(page_path)[1]
            fork_table = get_unmerged_versions(filefuncs, text_dir,
                                               (name,))
            if len(fork_table[name]) > 0:
                print ("<strong>This page has forks: %s!</strong><br>"  %
                       get_fork_html(filefuncs, text_dir, name, fork_table))

        print link_tag('?edit='+name, 'EditText')
        print "of this page"
        if mod_string:
            print "(last modified %s)" % mod_string
        print '<br>'
        print link_tag('FindPage?value='+name, 'FindPage')
        print " by browsing, searching, or an index"

        if page_path is None:
            print "<p><em>Wiki dir: %s </em>" % data_dir
            return

        if filefuncs.has_overlay(page_path):
            print (('<br><a href="?unmodified=%s">' % name) +
                   '[Show original version]</a><br>')
            print (('<a href="?removepage=%s">' % name) +
                   '[Locally delete this page without confirmation!]</a><br>')
            print (('<a href="?deletelocal=%s">' % name) +
                   '[Undo local edits without confirmation!]</a><br>')

        print "<p><em>Wiki dir: %s </em>" % data_dir


    def send_page(self, msg=None, unmodified=False):
        if self.handled_versioned_page(msg, unmodified):
            return

        link = get_scriptname() + '?fullsearch=' + self.wiki_name()
        send_title(self.split_title(), link, msg, bool(self.version()))
        PageFormatter(self.get_raw_body(unmodified)).print_html()
        self.send_footer(False, self._last_modified(),
                         self._text_filename(), unmodified)

    def _last_modified(self):
        if not self.exists():
            return None
        modtime = localtime(filefuncs.modtime(self._text_filename()))
        return strftime(datetime_fmt, modtime)

    # hmmm... change function name?
    def send_editor(self, read_only=False, unmodified=False):
        title = 'Edit '
        read_only_value=''
        if read_only:
            title = "View Page Source: "
            read_only_value = 'readonly'

        send_title(title + self.split_title())
        # IMPORTANT: Ask browser to send us utf8
        print '<form method="post" action="%s" accept-charset="UTF-8">' % (get_scriptname())
        print '<input type=hidden name="savepage" value="%s">' % \
              (self.page_name)
        # Encode outgoing raw wikitext into utf8
        raw_body = string.replace(self.get_raw_body(unmodified),
                                  '\r\n', '\n')
        print """<textarea wrap="virtual" name="savetext" rows="17"
                 cols="120" %s >%s</textarea>""" % (
                 read_only_value, raw_body)
        if not read_only:
            print """<br><input type=submit value="Save">
                     <input type=reset value="Reset">
                  """
        print "<br>"
        print "</form>"
        if not read_only:
            print "<p>" + Page('EditingTips').link_to()

    def _write_file(self, text):
        filefuncs.write(self._text_filename(), text, 'wb')

    def save_text(self, newtext):
        self._write_file(newtext)
        remote_name = environ.get('REMOTE_ADDR', '')

# See set_data_dir_from_cfg(), reset_root_dir
data_dir = None
text_dir = None
cgi.logfile = None

def get_logo_string():
    # Returning '' is allowed
    return '<img src="%s" border=0 alt="pikipiki">' % scrub('/' + PIKI_PNG)

#changed_time_fmt = ' . . . . [%I:%M %p]'
changed_time_fmt = '[%I:%M %p]'
#date_fmt = '%a %d %b %Y'
date_fmt = '%a %d %b %Y UTC'
datetime_fmt = '%a %d %b %Y %I:%M %p'
show_hosts = 0                          # show hostnames?
css_url = '/' + PIKI_CSS         # stylesheet link, or ''
nonexist_qm = 0                         # show '?' for nonexistent?

def serve_one_page():

    emit_header()

    try:
        global form
        form = cgi.FieldStorage()

        handlers = { 'fullsearch':  do_fullsearch,
                     'titlesearch': do_titlesearch,
                     'edit':        do_edit,
                     'viewsource':  do_viewsource,
                     'viewunmodifiedsource':  do_viewunmodifiedsource,
                     'savepage':    do_savepage,
                     'unmodified':  do_unmodified,
                     'deletelocal': do_deletelocal,
                     'removepage':  do_removepage}

        for cmd in handlers.keys():
            if form.has_key(cmd):
                apply(handlers[cmd], (form[cmd].value.decode('utf8'),))
                break
        else:
            path_info = environ.get('PATH_INFO', '')

            if form.has_key('goto'):
                query = form['goto'].value.decode('utf8')
            elif len(path_info) and path_info[0] == '/':
                query = path_info[1:] or 'FrontPage'
            else:
                query = environ.get('QUERY_STRING', '') or 'FrontPage'

            #word_match = re.match(word_re_str, query)
            word_match = re.match(versioned_page_re_str, query)
            #sys.stderr.write("query: %s [%s]\n" % (repr(query),
            #                                       repr(word_match)))
            if word_match:
                word = word_match.group('wikiword')
                if not word_match.group('version') is None:
                    word = "%s_%s" % (word, word_match.group('version'))
                Page(word).send_page()
            else:
                print "<p>Can't work out query \"<pre>" + query + "</pre>\""

    except:
        cgi.print_exception()

    sys.stdout.flush()


############################################################
def is_versioned(page_name):
    match = versioned_page_re.match(page_name)
    if match is None:
        return False
    return bool(match.group('version'))

############################################################
# See wikibot.py, update_change_log

# Hmmmm... too much code.
# Returns WikiName->(version, ...) table
def make_fork_list(versioned_names):
    if len(versioned_names) == 0:
        return ()

    table = {}
    for name in versioned_names:
        result = versioned_page_re.match(name)
        assert not result is None
        wiki_name = result.group('wikiword')
        assert not wiki_name is None
        version = result.group('version')
        assert not version is None
        entry = table.get(wiki_name, [])
        entry.append(version)
        table[wiki_name] = entry

    for value in table.values():
        value.sort()

    return table

def is_read_only(base_dir, page_name):
    full_path = os.path.join(base_dir, 'readonly.txt')
    if not os.path.exists(full_path):
        return False
    in_file = open(full_path, 'rb')
    try:
        return page_name in [value.strip()
                             for value in in_file.read().splitlines()]
    finally:
        in_file.close()

def read_log_file_entries(base_dir, max_entries):
    accepted = []
    full_path = os.path.join(base_dir, 'accepted.txt')
    if os.path.exists(full_path):
        in_file = open(full_path, 'rb')
        try:
            changes = {}
            # LATER: find/write reverse line iterator?
            for line in reversed(in_file.readlines()):
                if len(accepted) >= max_entries:
                    break
                fields = line.split(':')
                if fields[0] in ('C', 'M', 'R', 'F'):
                    for index in range(1, len(fields)):
                        fields[index] = fields[index].strip()
                    changes[fields[0]] = fields[1:]
                else:
                    fields = fields[:4]
                    fields.append((changes.get('C', ()),
                                   changes.get('M', ()),
                                   changes.get('R', ()),
                                   make_fork_list(changes.get('F', ()))))
                    accepted.append(tuple(fields))
                    changes = {}
        finally:
            in_file.close()

    rejected = []
    full_path = os.path.join(base_dir, 'rejected.txt')
    if os.path.exists(full_path):
        in_file = open(full_path, 'rb')
        try:
            changes = {}
            # LATER: find/write reverse line iterator?
            for line in reversed(in_file.readlines()):
                if len(rejected) >= max_entries:
                    break
                rejected.append(tuple(line.split(':')[:5]))
        finally:
            in_file.close()

    return tuple(accepted), tuple(rejected)


class FreenetPage(Page):
    def __init__(self, page_name):
        Page.__init__(self, page_name)


    def send_footer(self, versioned, dummy_mod_string=None,
                    page_path=None,
                    dummy_unmodified=False):
        print "<hr>"
        print "%s %s %s" % (link_tag('FrontPage', 'FrontPage'),
                            link_tag('TitleIndex', 'TitleIndex'),
                            link_tag('WordIndex', 'WordIndex'))
        if not page_path is None and not versioned:
            name = os.path.split(page_path)[1]
            fork_table = get_unmerged_versions(filefuncs, text_dir,
                                               (name,))
            if len(fork_table[name]) > 0:
                print (("<hr><strong>This page has forks: %s! " %
                        get_fork_html(filefuncs, text_dir, name, fork_table))
                       +
                       "Please consider merging them.</strong><br>")

def reset_root_dir(root_dir, overlayed=False):
    global data_dir, text_dir, filefuncs
    if not os.path.exists(root_dir) or not os.path.isdir(root_dir):
        raise IOError("Base wiki dir doesn't exist: %s" % root_dir)

    data_dir = root_dir
    text_dir = path.join(root_dir, 'wikitext')
    if not os.path.exists(text_dir) or not os.path.isdir(text_dir):
        raise IOError("Wikitext dir doesn't exist: %s" % text_dir)

    cgi.logfile = path.join(data_dir, 'cgi_log')
    filefuncs = fileoverlay.get_file_funcs(root_dir, overlayed)
    if overlayed:
        # Only overlay 'wikitext', not 'www'
        full_path = filefuncs.overlay_path(text_dir)
        if not os.path.exists(full_path):
            os.makedirs(full_path)

CFG_FILE = 'fnwiki.cfg'
WIKIROOT = 'wiki_root'
# REDFLAG: Hacks to make this work in windows binary mercurial distro?
from ConfigParser import ConfigParser
def set_data_dir_from_cfg(base_path=None):
    if base_path is None:
        # REDFLAG: test on windoze.
        base_path = os.getcwd()
    cfg_file = os.path.join(base_path, CFG_FILE)
    parser = ConfigParser()
    parser.read(cfg_file)
    if not parser.has_section('default'):
        raise IOError("Can't read default section from config file: %s." % cfg_file)

    if parser.has_option('default','wiki_root'):
        root_dir = os.path.join(base_path, parser.get('default', 'wiki_root'))
    else:
        root_dir = os.path.join(base_path, WIKIROOT)

    overlayed = True
    if parser.has_option('default','overlayedits'):
        overlayed = parser.getboolean('default','overlayedits')

    reset_root_dir(root_dir, overlayed)

import shutil
def create_default_wiki(base_path):
    if os.path.exists(base_path):
        raise IOError("The directory already exists.")

    shutil.copytree(os.path.join(os.path.dirname(__file__),
                                 'default_files'),
                    base_path)

def dump(output_dir, wiki_root, overlayed=False):
    global form, scrub_links

    form = {}
    scrub_links = True
    reset_root_dir(wiki_root, overlayed)

    old_out = sys.stdout
    try:
        pages = list(page_list(True))
        for name in pages:
            file_name = os.path.join(output_dir, name)
            out = codecs.open(file_name, "wb", 'utf8') # Write utf8
            try:
                page = FreenetPage(name)
                sys.stdout = out
                print '<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">'
                page.send_page()
                sys.stdout.flush()
                out.close()
                sys.stdout = old_out
            finally:
                out.close()
                sys.stdout = old_out
    finally:
        sys.stdout = old_out

    if not os.path.exists(os.path.join(data_dir, 'NotWrittenYet')):
        out = open(os.path.join(output_dir, 'NotWrittenYet'), 'wb')
        out.write("That page doesn't exist in the wiki yet!\n")
        out.close()

    if not os.path.exists(os.path.join(data_dir, 'AlreadyResolved')):
        out = open(os.path.join(output_dir, 'AlreadyResolved'), 'wb')
        out.write("That fork was already resolved.\n")
        out.close()

    # .css, .png
    www_dir = os.path.join(data_dir, 'www')
    for name in PIKI_REQUIRED_FILES:
        shutil.copyfile(os.path.join(www_dir, name),
                        os.path.join(output_dir, name))

# "builtin" when execfile()'d by servepiki.py
if __name__ == "__main__" or __name__ == "__builtin__":

    if not STDERR_FILE is None:
        sys.stderr = open(STDERR_FILE, 'ab')
    else:
        # Disable stderr. hmmm...
        sys.stderr = StringIO()

    # Redirect "print" output into a StringIO so
    # we can encode the html as UTF-8.
    real_out = sys.stdout
    buf = StringIO()
    sys.stdout = buf
    try:
        set_data_dir_from_cfg()
        serve_one_page()
    finally:
        sys.stdout = real_out
        print buf.getvalue().encode('utf8')



