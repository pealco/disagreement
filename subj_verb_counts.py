# Pedro Alcocer
#
# Count subject-verb co-occurences.

import sys
from glob import glob
sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import re
from nltk.parse import DependencyGraph

from dumbo.lib import *

class Mapper():
    
    def root_dependencies(self, dg): 
        return [dg.get_by_address(node) for node in dg.root["deps"]]
        
    def subject(self, dg):
        return [node for node in self.root_dependencies(dg) if node["rel"] == "SBJ"]

    def __call__(self, article, sentence):
        
        new_sentence = []
        sentence = sentence.split("\n")
        for word in sentence[1:-1]:
            columns = word.split("\t")
            try:
                new_columns = [columns[0], columns[2], columns[4], columns[5]]
            except IndexError:
                return                
            new_sentence += ["\t".join(new_columns)]
        
        s = "\n".join(new_sentence)
                
        dg = DependencyGraph(s)
        
        subject = self.subject(dg)[0]["word"]
        verb    = dg.root["word"]
        
        yield (subject, verb), 1
        
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(Mapper, sumreducer)
    job.run()
