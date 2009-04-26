""" Smoke test graph.

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


# We can be a little sloppy for test code.
# pylint: disable-msg=R0915, R0914
import binascii
import os
import random
import shutil

from binascii import unhexlify

from mercurial import hg, ui
from bundlecache import BundleCache
from graph import UpdateGraph, \
     build_version_table, UpdateGraphException, \
     pull_bundle, FIRST_INDEX, hex_version, UpToDate
from graphutil import parse_graph, graph_to_string, get_rollup_bounds, \
     minimal_graph
from chk import bytes_to_chk, CHK_SIZE

# Fix these paths as necessary
CACHE_DIR = '/tmp/bundle_cache' # MUST exist
TST_REPO_DIR = '/tmp/TST_REPO' # MUST not exist

# Must exist and contain the hg repo unbundled from:
# CHK@5fdjppBySiW2uFGd1nZNFD6U9xaadSPYTut9C3CdZa0,
#      33fQQwKXcjnhpAqI3nJw9kvjXYL~R9kVckAoFqbQ4IY,AAIC--8
#
# (Inserted from hgsvn_freenet_9f093d2c85c3.hg)
HGSVN_FREENET_REPO = '/tmp/HGSVN_FREENET'

VER_1 = '0000000000000000000000000000000000000000'
VER_2 = '1111111111111111111111111111111111111111'
VER_3 = '2222222222222222222222222222222222222222'
VER_4 = '3333333333333333333333333333333333333333'
VER_5 = '4444444444444444444444444444444444444444'
VER_6 = '5555555555555555555555555555555555555555'

def fake_chks():
    """ Return a random CHK. """
    size = CHK_SIZE - 5
    while True:
        #yield bytes_to_chk('\x00\x02\x02\xff\xff'
        #                   + ''.join(map(chr, map(random.randrange,
        #                                          [0] * size, [256] * size))))
        yield bytes_to_chk('\x00\x02\x02\xff\xff'
                           + ''.join([chr(random.randrange(0, 256)) for dummy
                                      in range(0, size)]))



def set_chks(graph, edges, chks):
    """ Set the chks for edges to random values. """
    for edge in edges:
        length = graph.get_length(edge)
        graph.set_chk(edge[:2], edge[2], length, chks.next())

def test_presentation():
    """ Smoke test graph_to_string and parse_graph. """
    graph = UpdateGraph()
    print "EMPTY:"
    print graph_to_string(graph)
    print "Adding index: ", graph.add_index([VER_1, ], [VER_2, ])
    print "Adding index: ", graph.add_index([VER_2, ], [VER_3, VER_4])
    print "Adding index: ", graph.add_index([VER_3, VER_2], [VER_5, ])
    chks = fake_chks()
    graph.add_edge((-1, 0), (100, chks.next()))
    graph.add_edge((1, 2), (200, chks.next()))
    graph.add_edge((-1, 2), (500, chks.next()))
    text = graph_to_string(graph)
    print
    print text
    print
    graph1 = parse_graph(text)
    print
    text1 = graph_to_string(graph1)
    print "Round trip:"
    print text1
    assert text == text1

def test_update(repo_dir):
    """ OBSOLETE? """
    ui_ = ui.ui()
    repo = hg.repository(ui_, repo_dir)
    cache = BundleCache(repo, ui_, CACHE_DIR)
    cache.remove_files()
    graph = UpdateGraph()
    graph.update(repo, ui, [1, 2], cache)
    print graph_to_string(graph)
    print
    print
    graph.update(repo, ui, [3, 4], cache)

    print graph_to_string(graph)
    print
    print
    graph.update(repo, ui, [6, ], cache)

    print graph_to_string(graph)

def test_update_real(repo_dir, version_list=None, full=False):
    """ Smoke test graph.update(). """
    ui_ = ui.ui()
    repo = hg.repository(ui_, repo_dir)
    cache = BundleCache(repo, ui_, CACHE_DIR)
    cache.remove_files()
    graph = UpdateGraph()
    if version_list is None:
        latest = repo['tip'].rev()
        version_list = [[ordinal, ] for ordinal in range(0, latest + 1)]

    chks = fake_chks()
    for vers in version_list:
        print "UPDATING TO: ", vers
        new_edges = graph.update(repo, ui, vers, cache)
        for edge in new_edges:
            length = graph.get_length(edge)
            graph.set_chk(edge[:2], edge[2], length, chks.next())

        # REDFLAG: should call minimal_graph for "real" behavior
        text = graph_to_string(graph)
        print "GRAPH_LEN: ", len(text)
        print text

    if full:
        print "UPDATING TO: latest heads"
        try:
            new_edges = graph.update(repo, ui, None, cache)
            for edge in new_edges:
                length = graph.get_length(edge)
                graph.set_chk(edge[:2], edge[2], length, chks.next())

            # REDFLAG: should call minimal_graph for "real" behavior
            text = graph_to_string(graph)
            print "GRAPH_LEN: ", len(text)
            print text
        except UpToDate:
            print "Already has the head revs."

    return (graph, repo, cache)

def test_minimal_graph(repo_dir, version_list, file_name=None):
    """ Smoke test minimal_graph(). """
    ui_ = ui.ui()
    if file_name is None:
        graph, repo, cache = test_update_real(repo_dir, version_list, True)
        open('/tmp/latest_graph.txt', 'wb').write(graph_to_string(graph))
    else:
        repo = hg.repository(ui_, repo_dir)
        cache = BundleCache(repo, ui_, CACHE_DIR)
        cache.remove_files()
        graph = parse_graph(open(file_name, 'rb').read())
        print "--- from file: %s ---" % file_name
        print graph_to_string(graph)
    version_map = build_version_table(graph, repo)

    # Incomplete, but better than nothing.
    # Verify that the chk bounds are the same after shrinking.
    chk_bounds = {}
    initial_edges = graph.get_top_key_edges()
    for edge in initial_edges:
        chk_bounds[graph.get_chk(edge)] = (
            get_rollup_bounds(graph, repo, edge[0] + 1, edge[1], version_map))

    print "CHK BOUNDS:"
    for value in chk_bounds:
        print value
        print "  ", chk_bounds[value]
    print
    sizes = (512, 1024, 2048, 4096, 16 * 1024)
    for max_size in sizes:
        try:
            print "MAX:", max(version_map.values())
            small = minimal_graph(graph, repo, version_map, max_size)
            print "--- size == %i" % max_size
            print graph_to_string(small)

            small.rep_invariant(repo, True) # Full check
            chks = chk_bounds.keys()
            path = small.get_top_key_edges()
            print "TOP KEY EDGES:"
            print path
            for edge in path:
                # MUST rebuild the version map because the indices changed.
                new_map = build_version_table(small, repo)
                bounds = get_rollup_bounds(small, repo, edge[0] + 1,
                                           edge[1], new_map)
                print "CHK:", small.get_chk(edge)
                print "BOUNDS: ", bounds
                assert chk_bounds[small.get_chk(edge)] == bounds
                print "DELETING: ", edge, small.get_chk(edge)
                chks.remove(small.get_chk(edge))
            assert len(chks) == 0
        except UpdateGraphException, err:
            print "IGNORED: ", err

def versions_str(version_list):
    """ Format a list of 40 digit hex versions for humans. """
    return ' '.join([version[:12] for version in version_list])

# def test_rollup(repo_dir):
#     version_list = [[1, ], [2, 3], [4, ], [7, ], [8, ], [9, 10]]
#     graph, repo, dummy = test_update_real(repo_dir, version_list)
#     print graph_to_string(graph)
#     print
#     version_map = build_version_table(graph, repo)
#     reverse_map = {}
#     for version in version_map:
#         index = version_map[version]
#         entry = reverse_map.get(index, set([]))
#         entry.add(version)
#         reverse_map[index] = entry

#     indices = reverse_map.keys()
#     indices.sort()
#     print "VERSION MAP:"
#     for index in indices:
#         print "%i:" % index
#         for version in reverse_map[index]:
#             print "   ", version

#     for index in range(0, graph.latest_index + 1):
#         parents, heads = get_rollup_bounds(graph, repo, 0, index, version_map)
#         print "%i: %s" % (index, versions_str(heads))
#         print "   ", versions_str(parents)

#         # Final roll
#         parents, heads = get_rollup_bounds(graph, repo, 3, 5,
#                                            version_map)

#         print "edge: %s" % versions_str(heads)
#         print "     ", versions_str(parents)

#     for index in range(0, graph.latest_index + 1):
#         heads = get_heads(graph, index)
#         print "[%i]:%s" % (index, versions_str(heads))


def hexlify_file(in_file):
    """ Dump the hex rep of a file. """
    data = binascii.hexlify(open(in_file, 'rb').read())
    while len(data):
        chunk = data[:64]
        print '+ "%s"' % chunk
        data = data[len(chunk):]


HGSVN_FREENET_REVS = (
    '1c33735d20b6',
    '815ca7018bf6',
    '3e2436fe3928',
    'ae954303d1dd',
    '723bd8953983',
    '40c11333daf8',
    '12eb1c81a1ec',
    'b02a0c6a859a',
    'a754303da0e5',
    '20c6ae2419bf',
    '2f8a676bb4c3',
    'c27794351de7',
    '1ef183fa59f6',
    '6479bf5a3eb9',
    '3f912b103873',
    '3a2204acb83c',
    '3aaad6480514',
    '76419812d2b7',
    'b5ba8a35c801',
    'ffcad749bed9',
    '31acbabecc22',
    'f5dd66d8e676',
    '3726fc577853',
    'efe0d89e6e8f',
    '87a25acc07e3',
    'ba9f937fed0e',
    '28f5a21c8e6e',
    'b0797db2ab16',
    'bc2e0f5d4a90',
    '3c9bb5cf63f6',
    'ffd58f1accde',
    '39ed8f43e65d',
    'b574671511ab',
    'b01c9e1811a8',
    '158e1087d72b',
    'ac5cfb495ef7',
    '32af1ded78d6',
    '950457d1f557',
    '42552b6d3b36',
#    '8ec2ceb1545d',
    )

ROLLUP_TEST_HG = (
    "48473130425a6839314159265359fb23d3670000be7fffffffffffdfffffffff"
    + "fff7effffffffffffffffffffffffffffffffdffffffdfe00b7d97c6d8346f2a"
    + "a8001806d52a9dc0000000200000000000000000000000000000000d34000000"
    + "000000000000000000000000d034929fa9a9a3d53c934d3f5469903ca6268c46"
    + "8da9934d1a00686468d34d18434d0699068c0134000d34321a6101880d0c4610"
    + "d01a61346801a34683201a1a0000d4d026d0264429a4f5004c04c03400013000"
    + "02600069300000000130046000d0046269800000013469800000000d1000a604"
    + "3553daa68d03d400343401a07ea80034000d34d068d000034001a06800000190"
    + "00d001a0001a0001a00d00f500200000000000000000000000000000000d3400"
    + "0000000000000000000000000000d0152a44533406129a69ed1368536a9fa4f0"
    + "9a60a78984d354fc14f29a9fea9e9a9ea329fa0d0c53268d3c149ec93c990f48"
    + "69a1a69a53f53349e933527e493d3689a6a6d3c93d0a7b53d32989a793d4d26d"
    + "340d29b6a9f88694f1a9e8d2901f6ddff8f5ec575ff465bd3fab2dc5755caa32"
    + "4b441ed5c323427a9a4c5af0fc4860d6dd17fb7cf99b3ccc2aeda2c7cb39d4aa"
    + "9bc89edd7f5125aa4b7ddf5f586c3698ead8d358835139365eef4d1735cdea5c"
    + "af2dc4408cd4825724082fdc126cde4c81320dc5dd8258c8b53b8a6bcc400212"
    + "24254c08ccab995733a68ab9578801b19002531c250dbf70e6411833bc444350"
    + "33304a049e3881247361014a4958d6929430744a56992df40a747d7ac91248c0"
    + "2510914e144945122850851424a2502461a50d6adf15ae6d5118ba05144e3c5f"
    + "d49551449181675497595bb8ecea27d54e896f62e364a95e4719d1c904e21ac2"
    + "23860c1883ab4215072d98bb108921a0ccb22220f3d0add0bb4d09984c6da8c9"
    + "e4f0fc0ba79c886089ccc8cc806add24224f0a4aa7766862ea8982c43924308f"
    + "022a618464dd6535f99b6bde4ba37c33d09915a45b57eb9ff28cc52d43d58cb2"
    + "c1ea2d5066aad487e8a09858d6292166e0ad1773c09e2c6d1908ddd5374b5b25"
    + "1b754fc0e6b0b43ff6a97477bc2f428a4a14605997e28a8a9a9e8631332af6a3"
    + "1eb634aea0c3b3d18df493571b672adb57451a0f2ea9a48fbadb2c3b9516b538"
    + "e89e0bf8ba7c2f329808f7d6e744af7223bd77ca738d1cec77f1f82db6530702"
    + "d737c6f60dd6401e02f0822eb9444ea0e32d43345ac5e54640113682c0108f0b"
    + "46bad9addf8b11af8dadafb4daaea64dc79faa2d1d9455beb3f447e43a68dd2e"
    + "31ae4b679aa32d16b5575d179544170adba450770cc522786609698eadfd23ef"
    + "3a5a393c6356e9dd64dd3c2788c6689f4380f9d3fbad64f2f492455ceff86f29"
    + "d7631273541b382feaa080bfa911e201986a8101abdf6dfbfc1a34111ad98788"
    + "704e3049274ae3308ae664c838da4b88e103cd6c047c605590a08b542f730ea1"
    + "eeb5771ba2919b8e5c23f3408d8f44d246730ad9f24eb5c6cc4eceb2807605d8"
    + "91121052327622bf04e080a6f50cbaaad69442b0f4c90e4de935f531180d1158"
    + "b8c997712e4538c1fb10f9c2dfcf4b00955b5b056fcb1d64ed6402ef840dd962"
    + "a6805f1c81e28a4a546428db81c848028320f0715b904402e46a118acec8bc06"
    + "a0c3db4850b05d09cdb1745cc71b2f1122178b5ac9134828312f11508b66b7a6"
    + "427687aa78e1b0691cfa6ad4c87f51ada6f84a6a96c5db654e734dad72cd3761"
    + "70dc1a64834037a508761100a8b5a86e0c01c64d7d5d0719e28b4390642022b1"
    + "2be5c30f2d02f865009d3230080181004eb862d7285ed6b1958b9e61ed144e03"
    + "4b81138200269601355eb4605ba660dd28a58c7cbc7908bdd5a4e13456db50f9"
    + "1b570685151ac3532c704c0ab93c5166aa10e9ac848bc60d0f0a136d812915ad"
    + "81980303d4b27285182d2a0c2e11b0cd2d4b85c85899bcb60709ae01a3575ca0"
    + "f0aa08d1ef18f5163242cc4a4259434c6a1587212c740d606a7408aff741dd24"
    + "e352dc24b4b6554816a405ff59330e2c6504e2b155a3272113785872570a21ec"
    + "8624ba27494109d1c232da401df528ce41b91baa24a7581a707d62cc8bfbf1fc"
    + "d037d1de2063a06ee1f7ba3702408cef175cc75e09589ebc641270125830b641"
    + "6a0a8131817062a16038444403c68c04cb6fded01ae0f11091443941816608d3"
    + "26d968b08467032c7032b24232a3e6c6ec738f5822b5e20e556eadcf585d984a"
    + "fc8442af283048c961acb138084b2fb178d4345a8a2a8da4196ce80d36295182"
    + "c1b858525499005156173a792ebc4a61a5729269baac9dac8242afbe0a6d2362"
    + "430192df6c6f58ac6260a25646513689f80129388708404ea2aeba9554a9e536"
    + "8e71ecc6fe3dd7931f65ec46363f83db6aa8a28aa5852fdef2231b1e2ba5b9e3"
    + "948df4d96ef4abab1ca8c2db549cfcf22340d0d8bebbd3e5af0f156c7dbe1ba8"
    + "68debafce13ab61df95ce641d4af6b8ffcf0fdc984d0c5a97dd1e0a39e8f477b"
    + "dc47389e52ad94a3417de43d49a88d4daa36f1b758464a2959b90b434aca64b9"
    + "88f10d82dfbb649d9bc13f3b08c4b3cde3b73ca7b0d0ab5725939cf1edd1b85c"
    + "fa4526ce38abf7abf4c5d9848f7b4ec239575fcef82fd98c8ac75397374d4c79"
    + "2b04ebe74b3d5699a97aefc33b499ced958c3b1bc66815eab8d23471b8f9e363"
    + "de5ef573fc3daec18e535d22eb06e5ee69a5094508a28f3e306ccee9d4c5c0af"
    + "756c57637c63eefd852bb52fadc90dff920ecf52d0c6a8f1691c2a16b9a23063"
    + "06ad8a4aec588bad4b80f007f21b6a66bb0d3a7202a6db7004dce201244d2b7b"
    + "a1c72076e812a120385279dff4a4db913b45d5634d6cc015b025cb707899b013"
    + "1c6c19255e906a18c48bb3b05569eab42cfd48596ae36024a1ec80b6b3887324"
    + "bb4b1cde2d5cd1c4846e23fb164d68ba62e355114d4cb4b558bb2926ad95ada9"
    + "10b6906faadc902dc8c26d396279d39a10a278a1ac25a1b4c9c9639413033429"
    + "1140a4293086281018913e64325594d55e4517134b2f524d8757bacaa42ce0b0"
    + "ba026b2f0e09174db4b6618d8328811888b92504b2d3187441b011adaf5331de"
    + "bfbc308a3509f3822215de9595158a7d018930ba41e1747d609887d8e6b53786"
    + "b5e22420983160a4af3073e1cd2186db0e85717366d152d89d8a0c5c2a44c24c"
    + "5328c0c890f60ac02dc29172abb4188d5431ed70a2b63058ba90aaa4b7456624"
    + "264209517b658e126ccd74d6d46c625f4043142415dad8541980ea9256113491"
    + "110864b38431af70934444633e107a77209c02c8b12ea59e48cce1fb542f97fd"
    + "dc7975ee1cd726b9c9fe6cbed294bbb948e6bbb3b24c7491ef15a861e311476f"
    + "1d3bea29f2ce510f390bff329654d0d5e21f0b6b1cec6c18043ee626463e8a0a"
    + "b89631b8c6dc6a1669f246a57ad5cfb7f458768f58f4ac582b775ffa663a9f52"
    + "a0280d345118d53e657e9d37af7f6fb276bca7fa5f2973dcc54e4792bc2c5555"
    + "79c9238ceea308f9d709aef77e57fc7a6aa50a4a350de25ce2a6a6b1cccf59ac"
    + "5276b17d665ad996923cc5eb6b30e7fbb3687f0db3e68e42e05b63f54ff2b279"
    + "b174772ca3191e333a9edf0238876d5df69c452f7dbf7eb54ef22ca79f2e6a88"
    + "c7c7f77865f55113e659c7eb763fe741868a3ecc53c6c5f5b53efbe07f67ee97"
    + "0a162ad3871b454f09cc392d5ad8decdff7564c12bd8f79ea51a78f6985534e0"
    + "ba658caf8be354eea3bb8c84e5be35af6370b2ee4cb7033549d3f54ae6fa3e1d"
    + "ce61b759ac951cbeb766ad8eecfeb1c0564d67e9e145fcf65b2b357a97239d6b"
    + "566d33f2c7d335ed92bdf14584f398a9b964e3d1fa89472f61f61fb65f7511e8"
    + "297e78d4b3714e2ef1477d44dac50cdb41c64d9c6466d1dcad26ce3fc2a5954f"
    + "18e025ee3f036cfcd2b3731a73d96763bcc8de721728f1d9f4e856a9776a9a97"
    + "dd57e89c556fb77cb7df13d57612f050ab6763111c18bb3f0ba4748cc3ca5b63"
    + "1ce24d0b68e146fdcfbee1cdc6fe2be6563dc8f53afb7e2d817bd97a3b88cc3f"
    + "13ce2d4d84705e7a77d612f4cd38eeb22edd6fb5170609f62324b5c67a3091cf"
    + "9c752ef23b2705d69736963d1bc3e2958db5de9e93fa1cbc53bdb03819bb65d5"
    + "a6bcf9b8bc5b7706328a9e7be552b6bb19965d9df3ed278cae741ad2895945a6"
    + "2e4cb33f15115b1948a53191bd2f4dc9c568a5b12c6366efdbceae28c79896d2"
    + "39b619dc3dc6196e9969dbc67de9be48e8e3a337ef9d62cf9f41ef35cdc3ad9d"
    + "b4699f1aa52caf3fb93026ca38ae3aa79f5c5e229ec5ab53cd6565d63fed9d76"
    + "f55bde61c88f95f5638ce55716bfe38c44d74ece2c716af8c84790a73932bd81"
    + "3a469f492fef816f786e99d42af7d9e71e69de5bf1de6d11b88b645c9fd5e39d"
    + "e9c8981786ae8f357b9eeb44c9baaabcd9d725a3b1736e4350d5c5e9fb2338ce"
    + "342bc6be8e3cfbca75cd645a5ac8fa57b378a3f9c5ba799729e8b4f197f09d09"
    + "efadb1a397460cfc897466a5628b8c652297b1314d3b62d546e990decd547fe4"
    + "ab798f0a7f26f8bf2ff191b4a603f6b79332b6be28f972af9188c3ede7271a47"
    + "df658ff68d8bb58c44c51d2baece4c5a6fcf53151e93d66a5c4564725a5eb70d"
    + "a23a05f16733f1805ee2eecf4de33297168e94a3dd755167388aef6ebb60a783"
    + "a14fe98bfc6c1eb1e03a0f3b3f146858964cfc6f49aa7ff8bb9229c28487d91e"
    + "9b38")

def setup_rollup_test_repo(dir_name):
    """ INTERNAL: Setup for rollup test. """
    assert dir_name.endswith('TST_REPO')
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)

    assert not os.path.exists(dir_name)
    os.makedirs(dir_name)
    ui_ = ui.ui()
    repo = hg.repository(ui_, dir_name, True)
    bundle_path = os.path.join(dir_name, 'bundle.hg')
    bundle_file = open(bundle_path, 'wb')
    bundle_file.write(unhexlify(ROLLUP_TEST_HG))
    bundle_file.close()
    pull_bundle(repo, ui_, bundle_path)
    return (repo, ui_)

def dump_version_map(version_map):
    """ Print a version map table in a human readable format. """
    reverse_map = {}
    for version in version_map:
        index = version_map[version]
        entry = reverse_map.get(index, set([]))
        entry.add(version)
        reverse_map[index] = entry

    indices = reverse_map.keys()
    indices.sort()
    print "---Version map---"
    for index in indices:
        print "%i:" % index
        for version in reverse_map[index]:
            print "   ", version

# Only compares first 12 digits so full ids can be compared
# against short ones.
def check_result(result_a, result_b):
    """ INTERNAL: Helper function. """
    assert len(result_a) == 2
    assert len(result_b) == 2
    assert len(result_a[0]) == len(result_b[0])
    assert len(result_a[1]) == len(result_b[1])
    for outer in range(0, 2):
        for inner in range(0, len(result_a[outer])):
            if result_a[outer][inner][:12] != result_b[outer][inner][:12]:
                print "MISMATCH:"
                print result_a
                print result_b
                assert False

def dump_changesets(repo):
    """ Print all the changesets in a repo. """
    print "---"
    max_rev = repo['tip'].rev()
    for rev in range(-1, max_rev + 1):
        print hex_version(repo, rev)
    print "---"
# There are many, many ways to fail.
# More testing would be good.

EXPECTED_VERSION_MAP = {
    '0000000000000000000000000000000000000000':-1,
    '716c293192c7b2e26860ade38e1b279e905cd197':0,
    '2aa9c462481a05287e7b97d7abc48ca53b24b33c':1,
    '4636fd812094cf54aeb58c7f5edf35d19ebe79e3':1,
    '076aec9f34c96c62a3069a98af1927bf710430b4':1,
    '62a72a238ffc748c11d115d8ceab44daf517fd76':2,
    '4409936ef21f3cb20e443b6ec37110978fccb484':2,
    '90374996e95f994e8925301fb91252fe509661e6':3,
    '75a57040197d15bde53148157403284727bcaba4':3,
    'a2c749d99d546c3db4f1fdb8c5c77ec9bef30aeb':3,
    '138466bcf8027a765b77b13a456598e74fe65115':4,
    '8ddce595000df42d9abbc81c7654fa36457b2081':4,
    'fd1e6832820b1a7a33f6e69d3ca561c09af2e015':5,
    '7429bf7b11f56d016a1cd7b7ec5cf130e48108b7':6,
    'f6248cd464e3f8fc8bd9a03dcc78943615d0e148':4,
    'fcc2e90dbf0dc2736c119c6ae995edf092f3f6cb':7,
    '03c047d036ca0f3aab3a47451fbde2b02026ee99':8,
    '9eaabc277b994c299aa4341178b416728fd279ff':9,
    '2f6c65f64ce59060c08dae82b7dcbeb8e4d2d976':9,
}

def test_rollup():
    """ Smoke test get_rollup_bounds(). """
    repo, ui_ = setup_rollup_test_repo(TST_REPO_DIR)
    dump_changesets(repo)
    cache = BundleCache(repo, ui_, CACHE_DIR)
    cache.remove_files()
    graph = UpdateGraph()

    chks = fake_chks()
    # 0 Single changeset
    edges = graph.update(repo, ui_, ['716c293192c7', ], cache)
    set_chks(graph, edges, chks)
    # 1 Multiple changesets
    edges = graph.update(repo, ui_, ['076aec9f34c9', ], cache)
    set_chks(graph, edges, chks)
    # 2 Multiple heads, single base
    edges = graph.update(repo, ui_, ['62a72a238ffc', '4409936ef21f'], cache)
    set_chks(graph, edges, chks)
    # 3 Multiple bases, single head
    edges = graph.update(repo, ui_, ['a2c749d99d54', ], cache)
    set_chks(graph, edges, chks)
    # 4
    edges = graph.update(repo, ui_, ['f6248cd464e3', ], cache)
    set_chks(graph, edges, chks)
    # 5
    edges = graph.update(repo, ui_, ['fd1e6832820b', ], cache)
    set_chks(graph, edges, chks)
    # 6
    edges = graph.update(repo, ui_, ['7429bf7b11f5', ], cache)
    set_chks(graph, edges, chks)
    # 7
    edges = graph.update(repo, ui_, ['fcc2e90dbf0d', ], cache)
    set_chks(graph, edges, chks)
    # 8
    edges = graph.update(repo, ui_, ['03c047d036ca', ], cache)
    set_chks(graph, edges, chks)

    # 9
    edges = graph.update(repo, ui_, ['2f6c65f64ce5', ], cache)
    set_chks(graph, edges, chks)

    print
    print graph_to_string(graph)
    version_map = build_version_table(graph, repo)

    dump_version_map(version_map)
    assert version_map == EXPECTED_VERSION_MAP

    graph.rep_invariant(repo, True) # Verify contiguousness.

    print "From earliest..."
    for index in range(0, graph.latest_index + 1):
        parents, heads = get_rollup_bounds(graph, repo, 0, index, version_map)
        print "(%i->%i): %s" % (0, index, versions_str(heads))
        print "       ", versions_str(parents)


    print "To latest..."
    for index in range(0, graph.latest_index + 1):
        parents, heads = get_rollup_bounds(graph, repo, index,
                                           graph.latest_index,
                                           version_map)
        print "(%i->%i): %s" % (index, graph.latest_index, versions_str(heads))
        print "       ", versions_str(parents)


    # Empty
    try:
        get_rollup_bounds(graph, repo, FIRST_INDEX, FIRST_INDEX,
                          version_map)
    except AssertionError:
        # Asserted as expected for to_index == FIRST_INDEX
        print "Got expected assertion."

    # Rollup of one changeset index.
    result = get_rollup_bounds(graph, repo, 0, 0, version_map)
    check_result(result, (('000000000000', ), ('716c293192c7',)))

    # Rollup of multiple changeset index.
    result = get_rollup_bounds(graph, repo, 1, 1, version_map)
    check_result(result, (('716c293192c7', ), ('076aec9f34c9',)))

    # Rollup of with multiple heads.
    result = get_rollup_bounds(graph, repo, 1, 2, version_map)
    check_result(result, (('716c293192c7', ), ('4409936ef21f','62a72a238ffc')))

    # Rollup of with multiple bases.
    result = get_rollup_bounds(graph, repo, 3, 4, version_map)
    check_result(result, (('4409936ef21f', '62a72a238ffc', ),
                          ('f6248cd464e3',)))

    # Rollup with head pulled in from earlier base.
    result = get_rollup_bounds(graph, repo, 3, 8, version_map)
    print result
    check_result(result, (('4409936ef21f', '62a72a238ffc', ),
                          ('03c047d036ca', '7429bf7b11f5')))

    # Rollup after remerge to a single head.
    result = get_rollup_bounds(graph, repo, 0, 9, version_map)
    print result
    check_result(result, (('000000000000', ), ('2f6c65f64ce5', )))

if __name__ == "__main__":
    test_presentation()
    VERSIONS = [(ver, ) for ver in HGSVN_FREENET_REVS]
    test_minimal_graph(HGSVN_FREENET_REPO, VERSIONS)
    test_rollup()

# Testing minimal graph.
# - contains the right edges [Inspection. not aut]
# x revision bounds of edges don't change
#   - backmap from chks
# ? indexes are contiguous [Lean on graph.rep_invariant)()]
#   a) ordinals -- easy
#   b) changesets -- hard
#      ?? depend on version map
# CONCERNS:
# - creating real use test cases
#   - so hard that I will "waste" a lot of time without finding bugs

