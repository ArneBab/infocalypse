""" Smoke test topkey.

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


from topkey import top_key_tuple_to_bytes, bytes_to_top_key_tuple, \
     dump_top_key_tuple

BAD_CHK1 = ('CHK@badroutingkey155JblbGup0yNSpoDJgVPnL8E5WXoc,'
            +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
BAD_CHK2 = ('CHK@badroutingkey255JblbGup0yNSpoDJgVPnL8E5WXoc,'
            +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
BAD_CHK3 = ('CHK@badroutingkey355JblbGup0yNSpoDJgVPnL8E5WXoc,'
            +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
BAD_CHK4 = ('CHK@badroutingkey455JblbGup0yNSpoDJgVPnL8E5WXoc,'
            +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
BAD_CHK5 = ('CHK@badroutingkey555JblbGup0yNSpoDJgVPnL8E5WXoc,'
            +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
BAD_CHK6 = ('CHK@badroutingkey655JblbGup0yNSpoDJgVPnL8E5WXoc,'
            +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
BAD_CHK7 = ('CHK@badroutingkey755JblbGup0yNSpoDJgVPnL8E5WXoc,'
            +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')

TOP = ((BAD_CHK6,),
       ((10, ('0' * 40, '1' * 40, '2' * 40), ('a' * 40, 'b' * 40,),
        (BAD_CHK1,), True, True),
       (20, ('3' * 40,), ('c' * 40,),
        (BAD_CHK2,), False, True),
       (30, ('3' * 40,), ('d' * 40,),
         (BAD_CHK3, BAD_CHK4), True, False),
       (40, ('2' * 40,), ('e' * 40,),
        (BAD_CHK5,), False, False),
       ))

def smoke_test_topkey():
    """ Smoke test top key functions. """
    # To binary rep...
    bytes0 = top_key_tuple_to_bytes(TOP)
    bytes1 = top_key_tuple_to_bytes(TOP, 0xcc)

    # Check salting.
    assert bytes0 != bytes1
    assert len(bytes0) == len(bytes1)

    # ... and back
    assert bytes_to_top_key_tuple(bytes0)[0] == TOP
    assert bytes_to_top_key_tuple(bytes1)[0] == TOP

    dump_top_key_tuple(TOP)

if __name__ == "__main__":
    smoke_test_topkey()
