from dumbo import sumreducer, identityreducer
import hashlib

def mapper(article, sentence):
    sentence = "\n".join(sentence)
    yield (article, sentence), 1

def filter_mapper(as_pair, count):
    article, sentence = as_pair
    yield article, sentence

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, sumreducer)
    job.additer(filter_mapper, identityreducer)
    job.run()




