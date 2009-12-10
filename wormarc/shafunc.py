""" Deal with move of SHA1 hash lib from sha to hashlib module.
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

try:
    #raise ImportError("fake") # Tested under 2.6 using this.
    from hashlib import sha1 as newshafunc
    #print "LOADED NEW"
    def new_sha(value=None):
        """ Make new SHA1 instance using hashlib module. """
        if value == None:
            return newshafunc()
        return newshafunc(value)

except ImportError:
    # Fall back so that code still runs on pre 2.6 systems.
    import sha as oldshamod
    #print "LOADED OLD"
    def new_sha(value=None):
        """ Make new SHA1 instance using old sha module. """
        if value == None:
            return oldshamod.new()
        return oldshamod.new(value)

# from shafunc import new_sha
# def main():
#     text = 'OH HAI'
#     a = new_sha()
#     a.update(text)
#     b = new_sha(text)
#     print a.hexdigest()
#     print b.hexdigest()

# main()
