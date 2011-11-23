# Pedro Alcocer
# May 9, 2010
# Finding agreement errors in Wikipedia using Hadoop
# Call with:
# dumbo start pipeline.py -input /user/pealco/wikipedia_split_parsed_deduped_dgs  -output /user/pealco/disagreement_pipeline_copula -overwrite yes -hadoop h -memlimit 4294967296 -numreducetasks 100 -file braubt_tagger.pkl

import os, sys
from glob import glob
sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import re

import nltk
nltk.data.path += ["/fs/clip-software/nltk-2.0b9-data"]

from nltk.parse import DependencyGraph
from nltk.corpus import wordnet as wn
from nltk.corpus import brown

from dumbo.lib import *

from cPickle import load


MAX_LENGTH = 20
VERBS = ["is", "are", "was", "were"]

NUMBER = {  "VBZ" : "SG",
            "VBP" : "PL",
            "VB"  : "PL",
            "NN"  : "SG",
            "NNS" : "PL" }

from functools import partial

class _compfunc(partial):
    def __lshift__(self, y):
        f = lambda *args, **kwargs: self.func(y(*args, **kwargs)) 
        return _compfunc(f)

    def __rshift__(self, y):
        f = lambda *args, **kwargs: y(self.func(*args, **kwargs)) 
        return _compfunc(f)

def composable(f):
    return _compfunc(f)


def quit_on_failure(func):
    def wrapper(data):
        if data:
            func(data)
        else:
            return False


def root_dependencies(dg): 
    return [dg.get_by_address(node) for node in dg.root["deps"]]

def dependencies(graph, node): 
    return [graph.get_by_address(dep) for dep in graph.get_by_address(node["address"])["deps"]]

def find_subject(dg):
    return [node for node in root_dependencies(dg) if node["rel"] == "SBJ"]

def plaintext(dg):
    return " ".join([node["word"] for node in dg.nodelist[1:]])
    
# Filters

@composable
@quit_on_failure
def remove_long_sentences(data):
    article, sentence_dg = data
    if len(sentence_dg.nodelist) <= MAX_LENGTH:
        yield article, sentence_dg

@composable
@quit_on_failure
def select_verbs(data):
    article, sentence_dg = data
    if sentence_dg.root["word"] in VERBS:
        yield article, sentence_dg

@composable
def find_disagreement(data):
    article, sentence_dg = data
    subject = find_subject(sentence_dg)
    subject_tag = subject[0]["tag"]
    verb = sentence_dg.root
    verb_tag = sentence_dg.root["tag"]
    
    if subject_tag in NUMBER and verb_tag in NUMBER:
        if NUMBER[subject_tag] != NUMBER[verb_tag]:
            yield article, sentence_dg

@composable
@quit_on_failure
def wordnet_filter(data):
    article, sentence_dg = data
    """Yields only sentence with subjects that are in wordnet."""    
    subject = find_subject(sentence_dg)[0]["word"]
    if wn.synsets(subject):
        yield article, sentence_dg

@composable
@quit_on_failure
def stopword_filter(data):
    article, sentence_dg = data
    stop_nouns = ["number", "majority", "percent", "total", "none", "pair", "part", "km", "mm"
                  "species", "series", "variety", "rest", "percentage"
                  "fish", "deer", "cattle", "sheep" "proginy"]
    subject = find_subject(sentence_dg)
    if subject[0]["word"] not in stop_nouns:
        yield article, sentence_dg

@composable
@quit_on_failure
def root_is_verb_filter(data):
    """Makes sure that the root is a verb."""
    article, sentence_dg = data
    if sentence_dg.root['tag'][0] == 'V':
        yield article, sentence_dg

@composable
@quit_on_failure
def preposition_filter(data):
    article, sentence_dg = data
    subject = find_subject(sentence_dg)
    subject_deps = subject[0]['deps']
    if any([sentence_dg.get_by_address(dep)['tag'] == 'IN' for dep in subject_deps]):
        yield article, sentence_dg

@composable
@quit_on_failure
def cc_in_subject_filter(data):
    """ The subject should not contain coordination."""
    article, sentence_dg = data
    subject = find_subject(sentence_dg)
    subject_deps = subject[0]['deps']
    if not any([sentence_dg.get_by_address(dep)['tag'] == 'CC' for dep in subject_deps]):
        yield article, sentence_dg


class modify_tags():
    def __init__(self):
        input = open('braubt_tagger.pkl', 'rb')
        self.tagger = load(input)
        input.close()
    
    def retag(self, sentence_dg):
        raw = plaintext(sentence_dg)
        tokens = nltk.word_tokenize(raw)
        return self.tagger.tag(tokens)
        
    def __call__(self, article, sentence_dg):
        retagged_sentence = self.retag(sentence_dg)
        subject = find_subject(sentence_dg)
        
        subject_address = subject[0]['address']
        verb_address    = sentence_dg.root['address']
        
        try:
            verb_word, new_verb_tag = retagged_sentence[verb_address]
            subject_word, new_subject_tag = retagged_sentence[subject_address]
        except IndexError:
            return
        
        sentence_dg.get_by_address(verb_address)['tag'] = new_verb_tag
        sentence_dg.get_by_address(subject_address)['tag'] = new_subject_tag
        
        yield article, sentence_dg
        
    
# Output converters
def linecount(article, sentence_dg):
    yield "*", 1

@composable
def convert_to_plaintext(data):
    if data:
        article, sentence_dg = data
        yield article, plaintext(sentence_dg)

# Composed pipeline

def pipeline(article, sentence_dg):
    data = (article, sentence_dg)
    yield (remove_long_sentences >> select_verbs >> stopword_filter >> root_is_verb_filter >> cc_in_subject_filter >> find_disagreement >> wordnet_filter >> preposition_filter >> convert_to_plaintext)(data)

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    #job.additer(remove_long_sentences,  identityreducer)
    #job.additer(select_verbs,           identityreducer)
    #job.additer(stopword_filter,        identityreducer)
    #job.additer(root_is_verb_filter,    identityreducer)
    #job.additer(cc_in_subject_filter,   identityreducer)    
    ##job.additer(modify_tags,       identityreducer)    
    #job.additer(find_disagreement,      identityreducer)
    #job.additer(wordnet_filter,         identityreducer)
    #job.additer(preposition_filter,     identityreducer)
    #job.additer(convert_to_plaintext,   identityreducer)
    #job.additer(linecount, sumreducer, combiner=sumreducer)
    job.additer(pipeline, identityreducer)
    job.run()
