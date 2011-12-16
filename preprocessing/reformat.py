# Pedro Alcocer
# May 9, 2010
# Finding agreement errors in Wikipedia using Hadoop

import sys
from glob import glob
sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

import re
from dumbo.lib import *

class Mapper():
    def __init__(self):
        
        self.title = ""
        self.article = []
        
    def __call__(self, key, value):
        m = re.match(r'<text id="wikipedia:(.*)">', value)
        if m:
            self.article = []
            self.title = m.group(1)
        elif value == "</text>":
            self.article = "\\n".join(self.article)
            yield self.title, self.article
        else:
            self.article += [value]
    
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(Mapper, identityreducer)
    job.run()
