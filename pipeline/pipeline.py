"""
## Finding agreement errors in Wikipedia using Hadoop

### Pedro Alcocer

Call with:

    dumbo start pipeline.py \
        -input /user/pealco/disagreement_sentence_objects \
        -output /user/pealco/disagreement_target_sentences \
        -file sentence2.py \
        -file funccomp.py \
        -file constants.py \
        -file wordcounts.pkl \
        -overwrite yes -hadoop h -memlimit 4294967296

Every filter present in the pipeline is decorated by the `composable` decorator.

Every composable function will return `False` if it receives `False` as its
argument, bypassing all computation in the function and in the rest of the
pipeline. The composed mapper function will only emit non-`False` values.
"""

import sys
from glob import glob

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import nltk
from nltk.corpus import wordnet as wn

from dumbo.lib import *
from sentence2 import *
from funccomp import *
from constants import *

### Path updates.

nltk.data.path += ["/fs/clip-software/nltk-2.0b9-data"]

### Helper functions.


def content_filter(string, attribute='word', scope='sentence'):
    """
    This is a function factory that creates map-reduce compatible filter
    functions. The function it returns looks at the attribute `attribute` for
    every node in the `scope`, checking whether the value of that attribute
    matches `string`.

    The only valid values for the `scope` argument are "sentence" or "preverb".
    The former includes all the nodes in the sentence, while the latter includes
    only those that precede the root node (which is assumed to be the verb).
    It defaults to "sentence".
    """

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
            raise ValueError("The scope '%s' is not defined. Defined scopes are 'sentence' and 'preverb'." % scope)

    return filter_

### Content filters


def stopword_filter(data):
    """
    Only sentences that do not contain words in `STOPWORDS` should be emitted.
    """

    filters = [content_filter(word) for word in STOPWORDS]

    composed_stopword_filter = compose(filters)
    return composed_stopword_filter(data)


@composable
def all_present_filter(data):
    """
    Only sentences for which a subject and an intervenor were detected should be
    emitted.
    """
    s_id, sentence = data
    if sentence.subject and sentence.intervenor:
        return s_id, sentence


@composable
def remove_long_sentences(data):
    """
    Only sentences with a maximum length of `MAX_LENGTH` should be emitted.
    """

    s_id, sentence = data
    if len(sentence.dg.nodelist) <= MAX_LENGTH:
        return s_id, sentence


@composable
def select_verbs(data):
    """
    Only verbs that are in `VERBS` should be emitted.
    """

    s_id, sentence = data
    if sentence.dg.root["word"] in VERBS:
        return s_id, sentence


@composable
def correct_tags_filter(data):
    """
    Only sentences whose subject, verb, and intervenor have part of speech tags
    that are in `NUMBER` should be emitted.
    """

    s_id, sentence = data
    subject_tag = sentence.subject["tag"]
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
    """
    Only sentences whose subjects and intervenors have synsets in
    [Wordnet](http://wordnet.princeton.edu/) should be emitted.
    """

    s_id, sentence = data

    if wn.synsets(sentence.subject['word']) and wn.synsets(sentence.intervenor['word']):
        return s_id, sentence


@composable
def root_is_verb_filter(data):
    """
    Only sentences who root node in their dependency graph representation is
    tagged as a verb should be emitted.
    """

    s_id, sentence = data
    if sentence.dg.root['tag'][0] == 'V':
        return s_id, sentence


@composable
def preposition_filter(data):
    """
    Only sentences in which the head of subject has dependencies that are
    tagged as prepositions will be emitted.
    """
    s_id, sentence = data
    subject_deps = sentence.subject['deps']
    if any([sentence.dg.get_by_address(dep)['tag'] == 'IN' for dep in subject_deps]):
        return s_id, sentence


@composable
def keep_singular_subjects(data):
    """
    Only sentences with singular number on the subject are emitted.
    """

    s_id, sentence = data

    if NUMBER[sentence.subject['tag']] == 'SG':
        return s_id, sentence


@composable
def keep_plural_intervenors(data):
    """
    Only sentences with plural number on the intervenor noun are emitted.
    """

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
def print_sentence(data):
    s_id, s = data

    return s_id, s.sentence


# Composed pipeline

def pipeline(article, sentence_dg):
    data = (article, sentence_dg)

    pipeline_steps = [
                      all_present_filter,
                      remove_long_sentences,
                      select_verbs,
                      correct_tags_filter,
                      stopword_filter,
                      composed_content_filters,
                      root_is_verb_filter,
                      post_verb_plural_filter,
                      wordnet_filter,
                      preposition_filter,
                      keep_singular_subjects,
                      keep_plural_intervenors,
                      ]

    composed_pipeline = compose(pipeline_steps)

    result = composed_pipeline(data)

    if result:
        yield result

### Running in dumbo.

"""
This runs as a single map-reduce job.
"""

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(pipeline, identityreducer)
    job.run()
