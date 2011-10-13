from dumbo import identityreducer


def mapper(k, v):
    yield v, k

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, identityreducer)
    job.run()




