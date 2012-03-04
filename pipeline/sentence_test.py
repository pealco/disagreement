import typedbytes
from nltk.corpus import wordnet as wn

from sentence2 import Sentence

sentences = typedbytes.PairedInput(open("../data/target_sentences.tb", "rb"))

for id, s in sentences:
    subject_synset    = wn.synsets(s.subject['word'])[0]
    intervenor_synset = wn.synsets(s.intervenor['word'])[0]
    similarity = subject_synset.wup_similarity(intervenor_synset)
    print subject_synset, intervenor_synset, similarity, s.similarity()