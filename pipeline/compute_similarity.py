# Pedro Alcocer
#
# Wikipedia wordcount
# Call with:
# dumbo start compute_similarity.py -input /user/pealco/disagreement_subj_int_pairs  -output /user/pealco/disagreement_lin_similarity -overwrite yes -hadoop h -memlimit 4294967296

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
from nltk.corpus import wordnet_ic

def mapper(pair, grammaticality):
    brown_ic = wordnet_ic.ic('ic-brown.dat')
    subject, intervener = pair
    
    try:
        subject_synset = wn.synsets(subject)[0]
        intervener_synset = wn.synsets(intervener)[0]
    except:
        pass
        
    try:
        similarity = subject_synset.lin_similarity(intervener_synset, brown_ic)
        yield grammaticality, similarity
    except:
        pass
    
    
    

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, identityreducer)
    job.run()
