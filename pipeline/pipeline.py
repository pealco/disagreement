# Pedro Alcocer
# May 9, 2010
# Finding agreement errors in Wikipedia using Hadoop

import sys
from glob import glob
sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import re
from nltk.parse import DependencyGraph
from nltk.corpus import wordnet as wn

from dumbo.lib import *

MAX_LENGTH = 20
VERBS = ["is", "are", "was", "were"]

EXPECTED_NUMBER = { "was": "NN",
                    "is" : "NN",
                    "were": "NNS",
                    "are" : "NNS"
                    }



def root_dependencies(dg): 
    return [dg.get_by_address(node) for node in dg.root["deps"]]

def dependencies(graph, node): 
    return [graph.get_by_address(dep) for dep in graph.get_by_address(node["address"])["deps"]]

def find_subject(dg):
    return [node for node in root_dependencies(dg) if node["rel"] == "SBJ"]
    
# Filters

def remove_long_sentences(article, sentence_dg):
    if len(sentence_dg.nodelist) <= MAX_LENGTH:
        yield article, sentence_dg

def select_verbs(article, sentence_dg):
    if sentence_dg.root["word"] in VERBS:
        yield article, sentence_dg

def find_disagreement(article, sentence_dg):
    verb = sentence_dg.root
    deps = root_dependencies(sentence_dg)
    subject = find_subject(sentence_dg)
    
    if subject[0]["tag"] in ("NN", "NNS") and (subject[0]["tag"] != EXPECTED_NUMBER[verb["word"]]):
        yield article, sentence_dg

def wordnet_filter(article, sentence_dg):
    """Yields only sentence with subjects that are in wordnet."""
    subject = find_subject(dg)
    
    if wn.synsets(subject):
        yield article, sentence_dg



if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
#    job.additer(remove_long_sentences,  identityreducer)
#    job.additer(select_verbs,           identityreducer)
#    job.additer(find_disagreement,      identityreducer)
    job.additer(wordnet_filter,      identityreducer)
    job.run()
