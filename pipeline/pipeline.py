"""
Pedro Alcocer
May 9, 2011
Finding agreement errors in Wikipedia using Hadoop
Call with:
dumbo start pipeline.py \
   -input /user/pealco/wikipedia_split_parsed_deduped_dgs \
   -output /user/pealco/disagreement_subj_int_pairs \
   -overwrite yes -hadoop h -memlimit 4294967296 
"""

import os, sys
from glob import glob

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

from functools import partial

import nltk
from nltk.parse import DependencyGraph
from nltk.corpus import wordnet as wn

from dumbo.lib import *
from sentence import Sentence

### Path updates.

nltk.data.path += ["/fs/clip-software/nltk-2.0b9-data"]

## Constants.

NUMBER = {  "VBZ" : "SG",
            "VBP" : "PL",
            "VB"  : "PL",
            "NN"  : "SG",
            "NNS" : "PL"
}

MAX_LENGTH = 20
VERBS = ["is", "are", "was", "were"]

STOPWORDS = ["number", "majority", "minority", "variety", "percent", 
                "total", "none", "pair", "part", "km", "mm",
                "species", "series", "variety", "rest", "percentage",
                "fish", "deer", "cattle", "sheep", "proginy",
                "first", "second", "third", "fourth", "fifth", "sixth", 
                "seventh", "eighth", "ninth", "tenth", "quarter",
                "politics", "acoustics", "data", "media", "headquarters",
                "range", "group", "kind", "half", "portion", "economics",
                "lot", "lots", "remainder", "amount", "host", "set", "list",
                "minimum", "maximum", "family", "handful", "bulk", "class",
                "couple", "type", "another", "average",
                ',', ':', '$', '?', '"', '%',
]

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

def find_intervenor(sentence_dg):
    subject = find_subject(sentence_dg)
    subject_deps = subject[0]['deps']
    prepositions = [sentence_dg.get_by_address(dep) for dep in subject_deps if sentence_dg.get_by_address(dep)['tag'] == 'IN']
    first_prep = prepositions[0]
    intervenor = sentence_dg.get_by_address(first_prep['deps'][0])
    return intervenor

def content_filter(string, attribute='word', scope='sentence'):
    
    @composable
    def filter_(data):
        article, sentence_dg = data
    
        if scope == 'sentence':
            matches = [node for node in sentence_dg.nodelist[1:] if node[attribute].lower() == string.lower()]
            if not matches:
                return article, sentence_dg
        elif scope == 'preverb':
            verb_address = sentence_dg.root["address"]
            preverb = [sentence_dg.get_by_address(address)[attribute] for address in xrange(1, verb_address)]
            if string not in preverb:
                return article, sentence_dg
        else:
            raise ValueError, "The scope '%s' is not defined. Defined scopes are 'sentence' and 'preverb'." % scope
    
    return filter_

### Content filters

def stopword_filter(data):
    filters = [content_filter(word) for word in STOPWORDS]
        
    composed_stopword_filter = compose(filters)
    return composed_stopword_filter(data)

### Structure filters

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
def correct_tags_filter(data):
    article, sentence_dg = data
    subject = find_subject(sentence_dg)
    subject_tag = subject[0]["tag"]
    try:
        intervenor = find_intervenor(sentence_dg)
    except IndexError:
        return False
    intervenor_tag = intervenor["tag"]
    verb = sentence_dg.root
    verb_tag = sentence_dg.root["tag"]

    if subject_tag in NUMBER and verb_tag in NUMBER and intervenor_tag in NUMBER:
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
def keep_singular_subjects(data):
    article, sentence_dg = data
    
    subject = find_subject(sentence_dg)[0]
    
    if NUMBER[subject['tag']] == 'SG':
        return article, sentence_dg
    
@composable
def keep_plural_intervenors(data):
    article, sentence_dg = data
    
    try:
        intervenor = find_intervenor(sentence_dg)
    except IndexError:
        return False
        
    if NUMBER[intervenor['tag']] == "PL":
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
    if post_verb_word['word'] != '.':
        return article, sentence_dg

content_filters = [
    content_filter('you', 'word', scope='preverb'),
    content_filter('CC',  attribute='tag',  scope='sentence'), 
]

composed_content_filters = compose(content_filters)

# Output converters

@composable
def find_agreement(data):
    article, sentence_dg = data
    subject = find_subject(sentence_dg)
    subject_tag = subject[0]["tag"]
    verb = sentence_dg.root
    verb_tag = sentence_dg.root["tag"]
    
    if subject_tag in NUMBER and verb_tag in NUMBER:
        if NUMBER[subject_tag] == NUMBER[verb_tag]:
            if NUMBER[verb_tag] == 'SG':
                return "GRAM", (article, sentence_dg)
        elif NUMBER[subject_tag] != NUMBER[verb_tag]:
            if NUMBER[verb_tag] == 'PL':
                return  "UNGRAM", sentence_dg
            
            

@composable
def convert_to_plaintext(data):
    key, sentence_dg = data
    return key, plaintext(sentence_dg)

@composable
def subject_intervenor_pairs(data):
    key, sentence_dg = data
    
    subject = find_subject(sentence_dg)[0]['word']
    
    try:
        intervenor = find_intervenor(sentence_dg)['word']
    except IndexError:
        return False
    
    sentence = plaintext(sentence_dg)
    
    #return (subject, intervenor), sentence
    return key, (subject, intervenor, plaintext(sentence_dg))

@composable
def compute_similarity(data):
    #brown_ic = wordnet_ic.ic('ic-brown.dat')
    grammaticality, triplet = data
    subject, intervenor, sentence = triplet
    
    try:
        subject_synset = wn.synsets(subject)[0]
        intervenor_synset = wn.synsets(intervenor)[0]
        similarity = subject_synset.wup_similarity(intervenor_synset)
        return sentence, (grammaticality, similarity, subject, intervenor) 
    except:
        return False
        

# Composed pipeline

def pipeline(article, sentence_dg):
    data = (article, sentence_dg)
    
    pipeline_steps = [remove_long_sentences,    # Filter out sentences whose length is greater than MAX_LENGTH.
                      select_verbs,             # Filter out sentences without approved verbs.
                      correct_tags_filter,
                      stopword_filter,          # Filter out sentences that contain words in the stopword list.
                      composed_content_filters,
                      root_is_verb_filter,      # Filter out sentences whose root node is a not a verb.
                      post_verb_plural_filter,
                      wordnet_filter,
                      preposition_filter,
                      keep_singular_subjects,
                      keep_plural_intervenors,
                      find_agreement,
                      subject_intervenor_pairs,
                      compute_similarity]
    
    composed_pipeline = compose(pipeline_steps)
    
    result = composed_pipeline(data)
    
    if result:
        yield result

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(pipeline, identityreducer)
    job.run()
    
