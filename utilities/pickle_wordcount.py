import cPickle as pickle
import typedbytes

wordcount_file = open("../data/wordcount.tb", "rb")
wordcount = typedbytes.PairedInput(wordcount_file)

wordcount_dict = dict((word, count) for word, count in wordcount)

output = open("../data/wordcounts.pkl", "wb")

pickle.dump(wordcount_dict, output, protocol=1)
