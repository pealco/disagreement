import cPickle as pickle
import typedbytes

wordcounts_file = open("../data/wordcounts.tb", "rb")
wordcounts = typedbytes.PairedInput(wordcounts_file)

wordcounts_dict = dict((word, count) for word, count in wordcounts)

output = open("../data/wordcounts.pkl", "wb")

pickle.dump(wordcounts_dict, output, protocol=1)
