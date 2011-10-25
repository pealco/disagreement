# Pedro Alcocer
#
# Compute subject-verb co-occurence relative frequencies.

import sys
from glob import glob

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

from collections import defaultdict
from nltk.parse import DependencyGraph
from dumbo.lib import *

def root_dependencies(dg): 
    return [dg.get_by_address(node) for node in dg.root["deps"]]

def subject_node(dg):
    return [node for node in root_dependencies(dg) if node["rel"] == "SBJ"]

def mapper(data):
    H = defaultdict(int)

    for article, dg in data:
        subject = subject_node(dg)[0]["word"]
        verb    = dg.root["word"]
        
        H[verb][subject] += 1
        
    for verb in H:
        yield verb, H[verb]
        

def reducer(verb, stripes):
    H = defaultdict(int)

    for stripe in stripes:
        for subject, count in stripe.items():
            H[subject] += count
    
    total = sum(count for count in H.values())
    
    for subject, count in H.items():
        yield (verb, subject), count/float(total)
    
    
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, reducer)
    job.run()
