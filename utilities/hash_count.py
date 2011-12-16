# Pedro Alcocer
#
# Convert subject-verb pairs into hashes.
# dumbo start hash_count.py -input /user/pealco/subj_verb_rel_freq  -output /user/pealco/subj_verb_rel_freq_hashes -overwrite yes -hadoop h -memlimit 4294967296 -numreducetasks 30

import hashlib
from dumbo.lib import *

def mapper(pair, freq):
    
    subject_verb = pair[1] + "_" + pair[0]
    yield subject_verb, freq
    
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, identityreducer)
    job.run()
