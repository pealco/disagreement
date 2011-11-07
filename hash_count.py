# Pedro Alcocer
#
# Convert subject-verb pairs into hashes.

import hashlib
from dumbo.lib import *

def mapper(pair, freq):
    
    subject_verb = pair[0] + "_" + pair[1]
    
    hash = hashlib.md5(subject_verb).hexdigest()
    
    yield hash, freq
    
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, identityreducer)
    job.run()
