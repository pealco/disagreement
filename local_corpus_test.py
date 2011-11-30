# Pedro Alcocer
# May 9, 2010
# Finding agreement errors in Wikipedia using Hadoop

import re
from nltk.parse import DependencyGraph



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
                            "fish", "deer", "cattle", "sheep" "proginy"]
                                        
    def plaintext(self, dg):
        return " ".join([node["word"] for node in dg.nodelist[1:]])
        
    def root_dependencies(self, dg): return [dg.get_by_address(node) for node in dg.root["deps"]]
    
    def dependencies(self, graph, node): return [graph.get_by_address(dep) for dep in graph.get_by_address(node["address"])["deps"]]
    
    def subject(self, dg):
        return [node for node in self.root_dependencies(dg) if node["rel"] == "SBJ"]

    def __call__(self, dg):
        
        verb = dg.root
        deps = self.root_dependencies(dg)
        subject = self.subject(dg)
        
        print dg
        
                
        #        
        #        
                #if not re.match(r".*[^a-z].*", subject[0]["word"].lower()):
                #    subject_deps = self.dependencies(dg, subject[0])
                #    if not any([node["tag"] == "CC" for node in dg.nodelist]) and not any([node["word"] == "percent" for node in dg.nodelist]):
                #        if subject[0]["tag"] in ("NN", "NNS"): 
                #            if (subject[0]["tag"] != self.expected_number[verb["word"]]):
                #                return self.plaintext(dg)
        
if __name__ == '__main__':
    from nltk.corpus import dependency_treebank
    
    dgs = dependency_treebank.parsed_sents()
    
    for dg in dgs:
        m = Mapper()
        print m(dg)
