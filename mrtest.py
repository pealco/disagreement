from dumbo import identityreducer


def mapper(k, v):
    yield "key: (%s)" % k, v 

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, identityreducer)
    job.run()




