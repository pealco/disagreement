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
        self.article = ""
        self.verbs = ["is", "are", "was", "were"]
        self.expected_number = {"was": "NN",
                                "is" : "NN",
                                "were": "NNS",
                                "are" : "NNS"
                                }
            
    def plaintext(self, dg):
        return " ".join([node["word"] for node in dg.nodelist[1:]])
        
    def root_dependencies(self, dg): return [dg.get_by_address(node) for node in dg.root["deps"]]
    
    def dependencies(self, graph, node): return [graph.get_by_address(dep) for dep in graph.get_by_address(node["address"])["deps"]]
    
    def subject(self, dg):
        return [node for node in self.root_dependencies(dg) if node["rel"] == "SBJ"]

    def __call__(self, article, sentence):
        self.article = article
        
        if article[0] == "A":
            yield article, sentence[1:-1]
        
        #s = "\n".join(["\t".join([c[0], c[2], c[4], c[5]]) for c in [word.split("\t") for word in sentence[1:-1]]])
        #dg = DependencyGraph(s)
        #if dg.root["word"] in self.verbs and len(dg.nodelist) <= 15:
        #    verb = dg.root
        #    deps = self.root_dependencies(dg)
        #    subject = self.subject(dg)
        #    
        #    if len(subject) == 1:
        #        if not re.match(r".*[^a-z].*", subject[0]["word"].lower()):
        #            subject_deps = self.dependencies(dg, subject[0])
        #            if not any([node["tag"] == "CC" for node in dg.nodelist]) and not any([node["word"] == "percent" for node in dg.nodelist]):
        #                if subject[0]["tag"] in ("NN", "NNS"): 
        #                    if (subject[0]["tag"] != self.expected_number[verb["word"]]):
        #                        yield self.article, self.plaintext(dg)
        
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(Mapper, identityreducer)
    job.run()
