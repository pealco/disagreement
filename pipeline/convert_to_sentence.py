"""
Pedro Alcocer

12/11/2011
Convert DirectedGraphs to Sentence objects
Call with:
dumbo start convert_to_sentence.py \
    -input /user/pealco/wikipedia_split_parsed_deduped_dgs \
    -output /user/pealco/disagreement_sentence_objects \
    -file sentence.py \
    -file constants.py \
    -overwrite yes -hadoop h -memlimit 4294967296 \
    -numreducetasks 100

    After the job completes, you should run

        hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u1.jar dumptb /user/pealco/disagreement_target_sentences/part-00000 > data.tb

    to convert the output into a typedbytes file.
"""

import sys
from glob import glob

sys.path += glob("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages/*.egg")
sys.path.append("/fs/clip-software/python-contrib-2.7.1.0/lib/python2.7/site-packages")

from dumbo.lib import identityreducer

import hashlib
from sentence import *


def mapper(article, dg):
    s = Sentence(article, dg)
    h = hashlib.sha1(s.sentence).hexdigest()
    yield h, s


if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, identityreducer)
    job.run()
