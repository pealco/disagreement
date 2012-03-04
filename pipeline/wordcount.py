"""

# Pedro Alcocer
#
# Wikipedia wordcount
# Call with:

dumbo start wordcount.py \
    -input /user/pealco/wikipedia_split_parsed_deduped_dgs\
    -output /user/pealco/disagreement_wordcount \
    -overwrite yes -hadoop h -memlimit 4294967296

"""

import sys
from glob import glob

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import nltk

from dumbo.lib import *
from collections import defaultdict

### Path updates.

nltk.data.path += ["/fs/clip-software/nltk-2.0b9-data"]
from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer

wnl = WordNetLemmatizer()


def mapper(data):
    # initialize
    counts = defaultdict(int)

    # map
    for article, dg in data:
        for node in dg.nodelist[1:]:
            word = node["word"].lower()
            word = wnl.lemmatize(word)
            if wn.synsets(word):
                counts[word] += 1

    # close
    for word in counts:
        yield word, counts[word]

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, sumreducer)
    job.run()
