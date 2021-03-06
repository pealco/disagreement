import re
from nltk.corpus import wordnet as wn
from constants import NUMBER, WORDCOUNTS
from math import log


class Sentence(object):

    def __init__(self, article, dg):

        self.punct_re = re.compile(r'\s([,\.;\?])')

        self.dg = dg
        self.sentence = self._plaintext()
        self.subject = self._find_subject()
        self.intervenor = self._find_intervenor()
        self.verb = self.dg.root


    def _plaintext(self):
        s = " ".join([node["word"] for node in self.dg.nodelist[1:]])
        return re.sub(self.punct_re, r'\g<1>', s)

    def similarity(self):
        """Compute Wu-Palmer similarity."""
        try:
            subject_synset = wn.synsets(self.subject['word'])[0]
            intervenor_synset = wn.synsets(self.intervenor['word'])[0]
            similarity = subject_synset.wup_similarity(intervenor_synset)
            return similarity
        except:
            return -1

    def rel_freq(self):

        subject = self.subject['word'].lower()
        intervenor = self.intervenor['word'].lower()

        subject_log_freq = log(WORDCOUNTS[subject])
        intervenor_log_freq = log(WORDCOUNTS[intervenor])
        rel_freq = subject_log_freq / intervenor_log_freq

        return rel_freq

    def _find_intervenor(self):

        subject_deps = self.subject['deps']
        prepositions = [self.dg.get_by_address(dep) for dep in subject_deps if self.dg.get_by_address(dep)['tag'] == 'IN']
        try:
            first_prep = prepositions[0]
            intervenor = self.dg.get_by_address(first_prep['deps'][0])
        except IndexError:
            return None

        return intervenor

    def _root_dependencies(self, dg):
        return [dg.get_by_address(node) for node in dg.root["deps"]]

    def _dependencies(dg, node):
        return [dg.get_by_address(dep) for dep in dg.get_by_address(node["address"])["deps"]]

    def _find_subject(self):
        return [node for node in self._root_dependencies(self.dg) if node["rel"] == "SBJ"][0]

    def is_grammatical(self):
        subject_tag = self.subject["tag"]
        verb_tag = self.verb["tag"]

        if subject_tag in NUMBER and verb_tag in NUMBER:
            if NUMBER[subject_tag] == NUMBER[verb_tag]:
                return True
            elif NUMBER[subject_tag] != NUMBER[verb_tag]:
                return False

    def __str__(self):
        return self.sentence
