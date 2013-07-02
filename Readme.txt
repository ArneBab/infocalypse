Infocalypse: Anonymous DVCS over Freenet
========================================

The Infocalypse 2.0 hg extension is an extension for Mercurial that allows you to create, publish 
and maintain incrementally updateable repositories in Freenet.

Your code is then hosted decentrally and anonymously, making it just as censorship-resistant as 
all other content in Freenet.

It includes additional redundancy, fetch-optimization to reduce the number of downloads required 
for getting the code, safe reinsert to keep the repository - by any user, not just the uploader - 
and many other features.

And it works transparently: To publish, just clone to freenet:

    hg clone REPO freenet://USK@/REPO

It supports WoT too:

    hg clone REPO freenet://wot_nick@public_key_hash/repo

    - Push / pull by name
    - Pull requests

Dependencies for full WoT support:
    - PyYAML http://pyyaml.org/wiki/PyYAMLDocumentation
    - lib-pyFreenet https://github.com/freenet/lib-pyFreenet-official
    - DefusedXML https://pypi.python.org/pypi/defusedxml/