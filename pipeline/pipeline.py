"""
Pedro Alcocer
May 9, 2011
Finding agreement errors in Wikipedia using Hadoop
Call with:
dumbo start pipeline.py \
   -input /user/pealco/disagreement_sentence_objects \
   -output /user/pealco/disagreement_target_sentences \
   -file sentence.py \
   -file funccomp.py \
   -file constants.py \
   -overwrite yes -hadoop h -memlimit 4294967296 
"""

import os, sys
from glob import glob

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import nltk
from nltk.parse import DependencyGraph
from nltk.corpus import wordnet as wn

from dumbo.lib import *
from sentence import *
from funccomp import *
from constants import *

### Path updates.

nltk.data.path += ["/fs/clip-software/nltk-2.0b9-data"]

### Helper functions.

def content_filter(string, attribute='word', scope='sentence'):
    
    @composable
    def filter_(data):
        s_id, sentence = data
    
        if scope == 'sentence':
            matches = [node for node in sentence.dg.nodelist[1:] if node[attribute].lower() == string.lower()]
            if not matches:
                return s_id, sentence
        elif scope == 'preverb':
            verb_address = sentence.dg.root["address"]
            preverb = [sentence.dg.get_by_address(address)[attribute] for address in xrange(1, verb_address)]
            if string not in preverb:
                return s_id, sentence
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
def all_present_filter(data):
    s_id, sentence = data
    if sentence.subject and sentence.intervenor:
        return s_id, sentence

@composable
def remove_long_sentences(data):
    s_id, sentence = data
    if len(sentence.dg.nodelist) <= MAX_LENGTH:
        return s_id, sentence

@composable
def select_verbs(data):
    s_id, sentence = data
    if sentence.dg.root["word"] in VERBS:
        return s_id, sentence

@composable
def correct_tags_filter(data):
    s_id, sentence = data
    subject_tag = sentence.subject[0]["tag"]
    if sentence.intervenor:
        intervenor_tag = sentence.intervenor["tag"]
    else:
        return False
    verb = sentence.dg.root
    verb_tag = verb["tag"]

    if subject_tag in NUMBER and verb_tag in NUMBER and intervenor_tag in NUMBER:
        return s_id, sentence

@composable
def wordnet_filter(data):
    """Returns only sentence with critical words that are in wordnet."""    
    
    s_id, sentence = data
    
    if wn.synsets(sentence.subject[0]['word']) and wn.synsets(sentence.intervenor['word']):
        return s_id, sentence

@composable
def root_is_verb_filter(data):
    """Makes sure that the root is a verb."""
    s_id, sentence = data
    if sentence.dg.root['tag'][0] == 'V':
        return s_id, sentence

@composable
def preposition_filter(data):
    s_id, sentence = data
    subject_deps = sentence.subject[0]['deps']
    if any([sentence.dg.get_by_address(dep)['tag'] == 'IN' for dep in subject_deps]):
        return s_id, sentence

@composable
def keep_singular_subjects(data):
    s_id, sentence = data
    
    if NUMBER[sentence.subject['tag']] == 'SG':
        return s_id, sentence
    
@composable
def keep_plural_intervenors(data):
    s_id, sentence = data
    
    if sentence.intervenor:
        if NUMBER[sentence.intervenor['tag']] == "PL":
            return s_id, sentence

@composable
def post_verb_plural_filter(data):
    s_id, sentence = data
    
    verb_address = sentence.dg.root["address"]
    post_verb_address = verb_address + 1
    
    if post_verb_address == len(sentence.dg.nodelist):
        return False
    
    post_verb_word = sentence.dg.get_by_address(post_verb_address)
    if post_verb_word['tag'] != 'NNS' and post_verb_word['word'] != '.':
        return s_id, sentence

content_filters = [
    content_filter('you', 'word', scope='preverb'),
    content_filter('CC',  attribute='tag',  scope='sentence'), 
]

composed_content_filters = compose(content_filters)

# Output converters

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

@composable
def print_sentence(data):
    s_id, s = data
    
    return s_id, s.sentence
    
        

# Composed pipeline

def pipeline(article, sentence_dg):
    data = (article, sentence_dg)
    
    pipeline_steps = [
                      all_present_filter,
                      remove_long_sentences,    # Filter out sentences whose length is greater than MAX_LENGTH.
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
                      print_sentence,
                      ]
    
    composed_pipeline = compose(pipeline_steps)
    
    result = composed_pipeline(data)
    
    if result:
        yield result

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(pipeline, identityreducer)
    job.run()
    
