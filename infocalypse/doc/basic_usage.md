# Basic Usage

Clone a repo into freenet with a new key:

    hg clone localrepo USK@/repo

*(Write down the insert key and request key after the upload! Localrepo is an existing [Mercurial](http://mercurial.selenic.com) repository)*

Clone a repo into or from freenet *(respective key known)*:

    hg clone localrepo freenet://USK@<insert key>/repo.R1/0
    hg clone freenet://USK@<request key>/repo.R1/0 [localpath]

Push or pull new changes:

    hg push freenet://USK@<insert key>/repo.R1/0
    hg pull freenet://USK@<request key>/repo.R1/0
   
   
For convenient copy-pasting of freenet keys, you can omit the “freenet://” here, or use freenet:USK@… instead.
   
Also, as shown in the first example, you can let infocalypse generate a new key for your repo:
   
    hg clone localrepo USK@/repo

*mind the “USK@/” (slash after @ == missing key). Also see the missing .R1/0 after the repo-name and the missing freenet://. Being able to omit those on repository creation is just a convenience feature - but one which helps me a lot.*

You can also add the keys to the `<repo>/.hg/hgrc`:
   
    [paths]
    example = freenet://USK@<request key>/repo.R1/0
    example-push = freenet://USK@<insert key>/repo.R1/0
    # here you need the freenet:// !
   
then you can simply use
   
    hg push example-push
   
and 
   
    hg pull example
