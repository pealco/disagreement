from dumbo import sumreducer
import hashlib

def mapper(article, sentence):
    sentence = "\n".join(sentence)
    
    hash = hashlib.md5(sentence).hexdigest()    
    
    yield (article, hash), 1

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, sumreducer)
    job.run()




