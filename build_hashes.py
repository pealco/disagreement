# Pedro Alcocer
#
# Finding grammatical agreement in Wikipedia using Hadoop

import sys
from glob import glob
sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import hashlib
import re
from nltk.parse import DependencyGraph

from dumbo.lib import *

class Mapper():
    def __init__(self):
        
        self.verbs = ["is", "are", "was", "were"]
        self.expected_number = {"was": "NN",
                                "is" : "NN",
                                "were": "NNS",
                                "are" : "NNS"
                                }
        self.stop_nouns = ["number", "majority", "percent", "total", "none", "pair", "part", "km", "mm"
                            "species", "series",
                            "fish", "deer", "cattle"]
            
    def plaintext(self, dg):
        return " ".join([node["word"] for node in dg.nodelist[1:]])
        
    def root_dependencies(self, dg): return [dg.get_by_address(node) for node in dg.root["deps"]]
    
    def dependencies(self, graph, node): return [graph.get_by_address(dep) for dep in graph.get_by_address(node["address"])["deps"]]
    
    def subject(self, dg):
        return [node for node in self.root_dependencies(dg) if node["rel"] == "SBJ"]

    def __call__(self, article, sentence):
        
        #print >> sys.stderr, sentence
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
        
        if dg.root["word"] in self.verbs and len(dg.nodelist) <= 20:
            verb = dg.root
            deps = self.root_dependencies(dg)
            subject = self.subject(dg)
            
            if len(subject) == 1:
                if not re.match(r".*[^a-z].*", subject[0]["word"].lower()):
                    subject_deps = self.dependencies(dg, subject[0])
                    if not any([node["tag"] == "CC" for node in dg.nodelist]) and not any([node["word"] == "percent" for node in dg.nodelist]):
                        if subject[0]["tag"] in ("NN", "NNS"): 
                            if (subject[0]["tag"] == self.expected_number[verb["word"]]):
                                subject_verb = subject[0]["word"] + "_" + verb["word"]
                                hash = hashlib.md5(subject_verb).hexdigest()
                                yield hash, "gram"
                            elif (subject[0]["tag"] != self.expected_number[verb["word"]]):
                                subject_verb = subject[0]["word"] + "_" + verb["word"]
                                hash = hashlib.md5(subject_verb).hexdigest()
                                yield hash, "ungram"
        
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(Mapper, identityreducer)
    job.run()
