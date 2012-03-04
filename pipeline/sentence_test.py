import typedbytes

from sentence2 import Sentence

sentences = typedbytes.PairedInput(open("../data/target_sentences.tb", "rb"))

for id, s in sentences:
	print s.rel_freq