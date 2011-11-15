# Pedro Alcocer
# May 9, 2010
# Finding agreement errors in Wikipedia using Hadoop
# Call with:
# dumbo start pipeline.py -input /user/pealco/wikipedia_split_parsed_deduped_dgs  -output /user/pealco/disagreement_pipeline_test -overwrite yes -hadoop h -memlimit 4294967296 -numreducetasks 100

import os, sys
from glob import glob
sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import re

import nltk
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

def plaintext(dg):
    return " ".join([node["word"] for node in dg.nodelist[1:]])
    
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

class wordnet_filter():
    """Yields only sentence with subjects that are in wordnet."""
    
    def __init__(self):
        nltk.data.path += ["/fs/clip-software/nltk-2.0b9-data"]
        nltk.data.path += [os.getcwd()]
        #wn = WordNetCorpusReader(nltk.data.find('wordnet.zip'))
        
        
    def __call__(self, article, sentence_dg):
        subject = find_subject(sentence_dg)[0]["word"]
        if wn.synsets(subject):
            yield article, sentence_dg

def stopword_filter(article, sentence_dg):
    stop_nouns = ["number", "majority", "percent", "total", "none", "pair", "part", "km", "mm"
                  "species", "series",
                  "fish", "deer", "cattle", "sheep" "proginy"]
    subject = find_subject(sentence_dg)
    if subject[0]["word"] not in stop_nouns:
        yield article, sentence_dg

def root_is_verb_filter(article, sentence_dg):
    """Makes sure that the root is a verb."""
    if sentence_dg.root['tag'][0] == 'V':
        yield article, sentence_dg

def preposition_filter(article, sentence_dg):
    subject = find_subject(sentence_dg)
    subject_deps = subject[0]['deps']
    if any([sentence_dg.get_by_address(dep)['tag'] == 'IN' for dep in subject_deps]):
        yield article, sentence_dg

def cc_in_subject_filter(article, sentence_dg):
    """ The subject should not contain coordination."""
    subject = find_subject(sentence_dg)
    subject_deps = subject[0]['deps']
    if not any([sentence_dg.get_by_address(dep)['tag'] == 'CC' for dep in subject_deps]):
        yield article, sentence_dg
    
# Output converters
def linecount(article, sentence_dg):
    yield "*", 1

def convert_to_plaintext(article, sentence_dg):
    yield article, plaintext(sentence_dg)

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(remove_long_sentences,  identityreducer)
    job.additer(select_verbs,           identityreducer)
    job.additer(stopword_filter,        identityreducer)
    job.additer(root_is_verb_filter,    identityreducer)
    job.additer(cc_in_subject_filter,   identityreducer)    
    job.additer(find_disagreement,      identityreducer)
    job.additer(wordnet_filter,         identityreducer)
    job.additer(preposition_filter,      identityreducer)
    job.additer(convert_to_plaintext,   identityreducer)
    #job.additer(linecount, sumreducer, combiner=sumreducer)
    job.run()
