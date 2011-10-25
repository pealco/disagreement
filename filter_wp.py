from dumbo import sumreducer, identityreducer
import hashlib

def mapper(article, sentence):
    sentence = "\n".join(sentence)
    
    hash = hashlib.md5(sentence).hexdigest()    
    
    yield (article, hash), 1

def filter_mapper(as_pair, count):
    article, sentence = as_pair
    yield article, sentence

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, sumreducer)
    job.run(filtermapper, identityreducer)




