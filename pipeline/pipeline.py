# Pedro Alcocer
# May 9, 2010
# Finding agreement errors in Wikipedia using Hadoop
# Call with:
# dumbo start pipeline.py -input /user/pealco/wikipedia_split_parsed_deduped_dgs  -output /user/pealco/disagreement_pipeline_copula -overwrite yes -hadoop h -memlimit 4294967296 -file braubt_tagger.pkl
#-numreducetasks 100 

import os, sys
from glob import glob

sys.stderr.flush()

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import re
from cPickle import load
from functools import partial

import nltk
from nltk.parse import DependencyGraph
from nltk.corpus import wordnet as wn

from dumbo.lib import *

### Path updates.

nltk.data.path += ["/fs/clip-software/nltk-2.0b9-data"]

## Constants.

MAX_LENGTH = 20
VERBS = ["is", "are", "was", "were"]

NUMBER = {  "VBZ" : "SG",
            "VBP" : "PL",
            "VB"  : "PL",
            "NN"  : "SG",
            "NNS" : "PL" }
            
#input = open('braubt_tagger.pkl', 'rb')
#TAGGER = load(input)
#input.close()            

### Function composition.

class _compfunc(partial):
    def __add__(self, y):
        f = lambda *args, **kwargs: y(self.func(*args, **kwargs)) 
        return _compfunc(f)

def composable(f):
    return _compfunc(fail_gracefully(f))
    
def compose(functions):
    return reduce(lambda x, y: x + y, functions)


def fail_gracefully(func):
    def wrapper(data):
        if data:
            return func(data)
        else:
            return False
    return wrapper
    
### Helper functions.

def root_dependencies(dg): 
    return [dg.get_by_address(node) for node in dg.root["deps"]]

def dependencies(graph, node): 
    return [graph.get_by_address(dep) for dep in graph.get_by_address(node["address"])["deps"]]

def find_subject(dg):
    return [node for node in root_dependencies(dg) if node["rel"] == "SBJ"]

def plaintext(dg):
    return " ".join([node["word"] for node in dg.nodelist[1:]])

def retag(sentence_dg):
    raw = plaintext(sentence_dg)
    tokens = nltk.word_tokenize(raw)
    return TAGGER.tag(tokens)

def preverb_filter_factory(token, attribute):
    """ Creates a filter. """
    
    @composable
    def filter_(data):
        article, sentence_dg = data
        verb_address = sentence_dg.root["address"]
    
        preverb = [sentence_dg.get_by_address(address)[attribute] for address in xrange(verb_address)]
    
        if token not in preverb:
            return article, sentence_dg
    
    return filter_
    
### Filters

@composable
def remove_long_sentences(data):
    article, sentence_dg = data
    if len(sentence_dg.nodelist) <= MAX_LENGTH:
        return article, sentence_dg

@composable
def select_verbs(data):
    article, sentence_dg = data
    if sentence_dg.root["word"] in VERBS:
        return article, sentence_dg

@composable
def find_disagreement(data):
    article, sentence_dg = data
    subject = find_subject(sentence_dg)
    subject_tag = subject[0]["tag"]
    verb = sentence_dg.root
    verb_tag = sentence_dg.root["tag"]
    
    if subject_tag in NUMBER and verb_tag in NUMBER:
        if NUMBER[subject_tag] != NUMBER[verb_tag]:
            return article, sentence_dg

@composable
def wordnet_filter(data):
    article, sentence_dg = data
    """returns only sentence with subjects that are in wordnet."""    
    subject = find_subject(sentence_dg)[0]["word"]
    if wn.synsets(subject):
        return article, sentence_dg

@composable
def stopword_filter(data):
    article, sentence_dg = data
    verb_address = sentence_dg.root["address"]

    preverb = [sentence_dg.get_by_address(address)[attribute] for address in xrange(verb_address)]
    
    
    stop_nouns = ["number", "majority", "minority", "variety", "percent", 
                    "total", "none", "pair", "part", "km", "mm"
                    "species", "series", "variety", "rest", "percentage"
                    "fish", "deer", "cattle", "sheep" "proginy",
                    "first", "second", "third", "fourth", "fifth", "sixth", 
                    "seventh", "eighth", "ninth", "tenth",
                    "politics", "acoustics", "data", "media", "headquarters",
                    "range", "group", "kind", "half"
                  ]
    subject = find_subject(sentence_dg)
    
    if not intersection(set(preverb), set(stop_nouns)):
        return article, sentence_dg

@composable
def root_is_verb_filter(data):
    """Makes sure that the root is a verb."""
    article, sentence_dg = data
    if sentence_dg.root['tag'][0] == 'V':
        return article, sentence_dg

@composable
def preposition_filter(data):
    article, sentence_dg = data
    subject = find_subject(sentence_dg)
    subject_deps = subject[0]['deps']
    if any([sentence_dg.get_by_address(dep)['tag'] == 'IN' for dep in subject_deps]):
        return article, sentence_dg
        
@composable    
def modify_tags(data):
    article, sentence_dg = data
    
    retagged_sentence = retag(sentence_dg)
    subject = find_subject(sentence_dg)
    
    subject_address = subject[0]['address']
    verb_address    = sentence_dg.root['address']
    
    try:
        verb_word, new_verb_tag = retagged_sentence[verb_address]
        subject_word, new_subject_tag = retagged_sentence[subject_address]
    except IndexError:
        return False
    
    sentence_dg.get_by_address(verb_address)['tag'] = new_verb_tag
    sentence_dg.get_by_address(subject_address)['tag'] = new_subject_tag
    
    return article, sentence_dg

@composable
def modify_subject_tags(data):
    article, sentence_dg = data
    
    retagged_sentence = retag(sentence_dg)
    subject = find_subject(sentence_dg)
    
    subject_address = subject[0]['address']
    
    try:
        subject_word, new_subject_tag = retagged_sentence[subject_address]
    except IndexError:
        return False
    
    sentence_dg.get_by_address(subject_address)['tag'] = new_subject_tag
    
    return article, sentence_dg

@composable
def post_verb_plural_filter(data):
    article, sentence_dg = data
    
    verb_address = sentence_dg.root["address"]
    post_verb_address = verb_address + 1
    
    if post_verb_address == len(sentence_dg.nodelist):
        return False
    
    post_verb_word = sentence_dg.get_by_address(post_verb_address)
    if post_verb_word['tag'] != 'NNS':
        return article, sentence_dg

coordination_filter = preverb_filter_factory('CC', 'tag')
you_filter          = preverb_filter_factory('you', 'word')
comma_filter        = preverb_filter_factory(',', 'word')

# Output converters

@composable
def convert_to_plaintext(data):
    article, sentence_dg = data
    return article, plaintext(sentence_dg)

# Composed pipeline

def pipeline(article, sentence_dg):
    data = (article, sentence_dg)
    
    pipeline_steps = [remove_long_sentences,
                      select_verbs,
                      stopword_filter,
                      root_is_verb_filter,
                      coordination_filter,
                      you_filter,
                      comma_filter,
                      post_verb_plural_filter,
                      find_disagreement,
                      wordnet_filter,
                      preposition_filter,
                      convert_to_plaintext]
    
    composed_pipeline = compose(pipeline_steps)
    
    result = composed_pipeline(data)
    
    if result:
        yield result

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(pipeline, identityreducer)
    job.run()
    
