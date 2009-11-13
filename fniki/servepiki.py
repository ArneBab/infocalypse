#! /usr/bin/env python

# ATTRIBUTION: Pierre Quentel
# http://code.activestate.com/recipes/511454/
# LICENSE: MIT
# http://code.activestate.com/help/terms/
# http://www.opensource.org/licenses/mit-license.php
#
# Modifications: Copyright (C) 2009 Darrell Karbott

# djk20091109 -- I modified this file to run piki from the local dir
#                and do nothing else.
#                DONT TRY TO USE IT AS A GENERIC SERVER.

import SimpleAsyncServer

# =============================================================
# An implementation of the HTTP protocol, supporting persistent
# connections and CGI
# =============================================================
import sys
import os
import traceback
import datetime
import mimetypes
import urlparse
import urllib
import cStringIO
import re

import piki

# Absolute path to the cgi python script.
SCRIPT_PATH = piki.__file__
if SCRIPT_PATH[-3:] == 'pyc':
    # We need the python source, *NOT* the compiled code.
    SCRIPT_PATH = SCRIPT_PATH[:-1]

# Name *without* any '/' chars
SCRIPT_NAME = 'piki'

SCRIPT_REGEX = re.compile(r'/%s($|[\?/])' % SCRIPT_NAME)

class HTTP(SimpleAsyncServer.ClientHandler):
    # parameters to override if necessary
    root = os.getcwd()  # the directory to serve files from

    # djk20091109 HACK. *only* runs piki script from this directory.
    # Don't need cgi_directories.
    # cgi_directories = ['/cgi-bin']  # subdirectories for cgi scripts

    script_name = None
    script_path = None
    script_regex = None

    logging = True      # print logging info for each request ?
    blocksize = 2 << 16 # size of blocks to read from files and send

    def request_complete(self):
        """In the HTTP protocol, a request is complete if the "end of headers"
        sequence ('\r\n\r\n') has been received
        If the request is POST, stores the request body in a StringIO before
        returning True"""
        terminator = self.incoming.find('\r\n\r\n')
        if terminator == -1:
            return False
        lines = self.incoming[:terminator].split('\r\n')
        self.requestline = lines[0]
        try:
            self.method,self.url,self.protocol = lines[0].strip().split()
        except:
            self.method = None # indicates bad request
            return True
        # put request headers in a dictionary
        self.headers = {}
        for line in lines[1:]:
            k,v = line.split(':',1)
            self.headers[k.lower().strip()] = v.strip()
        # persistent connection
        close_conn = self.headers.get("connection","")
        if (self.protocol == "HTTP/1.1"
            and close_conn.lower() == "keep-alive"):
            self.close_when_done = False
        # parse the url
        scheme,netloc,path,params,query,fragment = urlparse.urlparse(self.url)
        self.path,self.rest = path,(params,query,fragment)

        if self.method == 'POST':
            # for POST requests, read the request body
            # its length must be specified in the content-length header
            content_length = int(self.headers.get('content-length',0))
            body = self.incoming[terminator+4:]
            # request is incomplete if not all message body received
            if len(body)<content_length:
                return False
            f_body = cStringIO.StringIO(body)
            f_body.seek(0)
            sys.stdin = f_body # compatibility with CGI

        return True

    def make_response(self):
        """Build the response : a list of strings or files"""
        if self.method is None: # bad request
            return self.err_resp(400,'Bad request : %s' %self.requestline)
        resp_headers, resp_body, resp_file = '','',None
        if not self.method in ['GET','POST','HEAD']:
            return self.err_resp(501,'Unsupported method (%s)' %self.method)
        else:
            file_name = self.file_name = self.translate_path()
            # djk20091109 Keep trailing PATH_INFO for script from tripping 404.
            if ((not self.managed()) and
                (not os.path.exists(file_name) or not os.path.isfile(file_name))):
                if self.path.strip() == '/':
                    # Redirect instead of 404ing for no path.
                    return self.redirect_resp('/%s/' % HTTP.script_name,
                                              'Redirecting to %s cgi.' % HTTP.script_name)
                return self.err_resp(404,'File not found')
            elif self.managed():
                response = self.mngt_method()
            else:
                ext = os.path.splitext(file_name)[1]
                c_type = mimetypes.types_map.get(ext,'text/plain')
                resp_line = "%s 200 Ok\r\n" %self.protocol
                size = os.stat(file_name).st_size
                resp_headers = "Content-Type: %s\r\n" %c_type
                resp_headers += "Content-Length: %s\r\n" %size
                resp_headers += '\r\n'
                if self.method == "HEAD":
                    resp_string = resp_line + resp_headers
                elif size > HTTP.blocksize:
                    resp_string = resp_line + resp_headers
                    resp_file = open(file_name,'rb')
                else:
                    resp_string = resp_line + resp_headers + \
                        open(file_name,'rb').read()
                response = [resp_string]
                if resp_file:
                    response.append(resp_file)
        self.log(200)
        return response

    def translate_path(self):
        """Translate URL path into a path in the file system"""
        return os.path.join(HTTP.root,*self.path.split('/'))

    def managed(self):
        """Test if the request can be processed by a specific method
        If so, set self.mngt_method to the method used
        This implementation tests if the script is in a cgi directory"""
        if self.is_cgi():
            self.mngt_method = self.run_cgi
            return True
        return False

    # djk20091109 HACKED to run only piki.
    def is_cgi(self):
        """Only run the piki cgi."""
        return bool(HTTP.script_regex.match(self.path.strip()))

    def run_cgi(self):
        # set CGI environment variables
        self.make_cgi_env()
        # redirect print statements to a cStringIO

        save_stdout = sys.stdout
        sys.stdout = cStringIO.StringIO()
        # run the script
        try:
            # djk20091109 There was a bug here. You need the {} in order to run
            # global functions.
            #
            #execfile(self.file_name)

            # djk20091109 HACKED to run only piki script.
            execfile(HTTP.script_path, {})
        except:
            sys.stdout = cStringIO.StringIO()
            sys.stdout.write("Content-type:text/plain\r\n\r\n")
            traceback.print_exc(file=sys.stdout)

        response = sys.stdout.getvalue()
        if self.method == "HEAD":
            # for HEAD request, don't send message body even if the script
            # returns one (RFC 3875)
            head_lines = []
            for line in response.split('\n'):
                if not line:
                    break
                head_lines.append(line)
            response = '\n'.join(head_lines)
        sys.stdout = save_stdout # restore sys.stdout
        # close connection in case there is no content-length header
        self.close_when_done = True
        resp_line = "%s 200 Ok\r\n" %self.protocol
        return [resp_line + response]

    def make_cgi_env(self):
        """Set CGI environment variables"""
        env = {}
        env['SERVER_SOFTWARE'] = "AsyncServer"
        env['SERVER_NAME'] = "AsyncServer"
        env['GATEWAY_INTERFACE'] = 'CGI/1.1'
        env['DOCUMENT_ROOT'] = HTTP.root
        env['SERVER_PROTOCOL'] = "HTTP/1.1"
        env['SERVER_PORT'] = str(self.server.port)

        env['REQUEST_METHOD'] = self.method
        env['REQUEST_URI'] = self.url
        env['PATH_TRANSLATED'] = self.translate_path()

        #env['SCRIPT_NAME'] = self.path
        # djk20091109 HACK
        env['SCRIPT_NAME'] = '/' + HTTP.script_name
        # djk20091109 BUG? I think this was just wrong.
        #env['PATH_INFO'] = urlparse.urlunparse(("","","",self.rest[0],"",""))
        env['PATH_INFO'] = self.path[len("/" + HTTP.script_name):]
        env['QUERY_STRING'] = self.rest[1]
        if not self.host == self.client_address[0]:
            env['REMOTE_HOST'] = self.host
        env['REMOTE_ADDR'] = self.client_address[0]
        env['CONTENT_LENGTH'] = str(self.headers.get('content-length',''))
        for k in ['USER_AGENT','COOKIE','ACCEPT','ACCEPT_CHARSET',
            'ACCEPT_ENCODING','ACCEPT_LANGUAGE','CONNECTION']:
            hdr = k.lower().replace("_","-")
            env['HTTP_%s' %k.upper()] = str(self.headers.get(hdr,''))
        os.environ.update(env)

    def err_resp(self,code,msg):
        """Return an error message"""
        resp_line = "%s %s %s\r\n" %(self.protocol,code,msg)
        self.close_when_done = True
        self.log(code)
        return [resp_line]

    def redirect_resp(self, url, msg):
        """Return a 301 redirect"""
        resp_line = "%s %s %s\r\n" %(self.protocol,301,msg)
        resp_line += "Location: %s\r\n" % url
        self.close_when_done = True
        self.log(301)
        return [resp_line]

    def log(self,code):
        """Write a trace of the request on stderr"""
        if HTTP.logging:
            date_str = datetime.datetime.now().strftime('[%d/%b/%Y %H:%M:%S]')
            sys.stderr.write('%s - - %s "%s" %s\n' %(self.host,
                date_str,self.requestline,code))

def default_out_func(text):
    print text

def serve_wiki(port=8081, bind_to='localhost', out_func=default_out_func):
    #out_func("server_wiki running under: %s" % str(sys.version))
    out_func("Reading parameters from fniki.cfg...")
    piki.set_data_dir_from_cfg()
    out_func("Running wiki from:")
    out_func(piki.text_dir + " (wiki text)")
    www_dir = os.path.join(piki.data_dir, 'www')
    out_func(www_dir + " (.css, .png)")
    print
    bound_to = bind_to
    if bound_to == '':
        bound_to = 'all interfaces!'

    out_func("Starting HTTP server on port %s, bound to: %s " %
             (port, bound_to))
    out_func("Press Ctrl+C to stop")

    # Change to 'localhost' to '' to bind to all interface. Not recommended.
    server = SimpleAsyncServer.Server(bind_to, port)

    # Must set these.
    HTTP.script_name = SCRIPT_NAME
    HTTP.script_path = SCRIPT_PATH
    HTTP.script_regex = SCRIPT_REGEX
    
    HTTP.logging = False
    HTTP.root = www_dir # for .css, .png

    try:
        SimpleAsyncServer.loop(server,HTTP)
    except KeyboardInterrupt:
        # djk20091109 Just wrong. Did I grab the wrong file for the base class??? hmmmm...
        #
        # for s in server.client_handlers:
        #    server.close_client(s) # obviously wrong.

        # Correct now?
        for s in SimpleAsyncServer.client_handlers.copy():
            SimpleAsyncServer.client_handlers[s].close()


        out_func('Ctrl+C pressed. Closing')

if __name__=="__main__":
    serve_wiki(8081)
