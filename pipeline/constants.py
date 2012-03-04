## Constants.

import cPickle as pickle

NUMBER = {"VBZ":  "SG",
           "VBP": "PL",
           "VB":  "PL",
           "NN":  "SG",
           "NNS": "PL"
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

wordcounts_file = open("../data/wordcounts.pkl")
WORDCOUNTS = pickle.load(wordcounts_file)
wordcounts_file.close()
