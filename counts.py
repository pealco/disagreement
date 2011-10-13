# Pedro Alcocer
# May 9, 2010
# Finding agreement errors in Wikipedia using Hadoop

import sys
from glob import glob
sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import re
from nltk.parse import DependencyGraph

from dumbo.lib import *

class Mapper():
    def __init__(self):
        
        self.sentence = []
        
    def __call__(self, key, value):
        if value == "<s>" or value == "</s>":
            if value == "<s>":
                self.sentence = []
            if value == "</s>":
                yield "words", len(self.sentence)
                yield "sentences", 1
        elif re.match(r'<text id="wikipedia:(.*)">', value):
            yield "articles", 1
        elif value == "</text>":
            pass
        else:
            self.sentence += [value]
    
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(Mapper, sumreducer)
    job.run()
