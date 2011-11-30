# Pedro Alcocer
#
# Wikipedia wordcount
# Call with:
# dumbo start wordcount.py -input /user/pealco/wikipedia_split_parsed_deduped_dgs  -output /user/pealco/disagreement_wordcount -overwrite yes -hadoop h -memlimit 4294967296

import os, sys
from glob import glob

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import nltk
from nltk.parse import DependencyGraph

from dumbo.lib import *
from collections import defaultdict


### Path updates.

nltk.data.path += ["/fs/clip-software/nltk-2.0b9-data"]
from nltk.corpus import wordnet as wn

def mapper(data):
    # initialize
    H = defaultdict(int)
    
    # map
    for article, dg in data:
        for node in dg.nodelist[1:]:
            if wn.synsets(node["word"]):
                H[node["word"]] += 1

    # close
    for word in H:
        yield word, H[word]
    
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, sumreducer)
    job.run()
