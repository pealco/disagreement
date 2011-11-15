# Pedro Alcocer
# May 9, 2010
# Finding agreement errors in Wikipedia using Hadoop

import sys
from glob import glob
sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import re
from nltk.parse import DependencyGraph

from dumbo.lib import *


VERBS = ["is", "are", "was", "were"]

def remove_long_sentences(article, sentence_dg):
    if len(sentence_dg.nodelist) <= 20:
        yield article, sentence_dg

def select_verbs(article, sentence_dg):
    if sentence_dg.root["word"] in VERBS:
        yield article, sentence_dg
        
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(remove_long_sentences,  identityreducer)
    job.additer(select_verbs,           identityreducer)
    job.run()
