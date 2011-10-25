from dumbo import sumreducer


def mapper(k, v):
    yield (k, v), 1

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, sumreducer)
    job.run()




