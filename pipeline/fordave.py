# Pedro Alcocer
#
# dumbo start fordave.py -input /user/pealco/wikipedia_split_parsed_deduped_dgs  -output /user/pealco/dave_corpus -overwrite yes -hadoop h -memlimit 4294967296

import os, sys
from glob import glob

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import nltk
from nltk.parse import DependencyGraph

from dumbo.lib import *
from collections import defaultdict


### Path updates.

def plaintext(dg):
    return " ".join([node["word"] for node in dg.nodelist[1:]])

def mapper(article, dg):
    if " but he " in plaintext(dg):
        yield "BUT",  plaintext(dg)
    
    if " when he " in plaintext(dg):
        yield "WHEN", plaintext(dg)
    
    
    
    
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, identityreducer)
    job.run()
