# Counts lines

from collections import defaultdict
from dumbo import sumreducer

def mapper(data):
    H = defaultdict(int)
    
    for k, v in data:
        H["*"] += 1
        
    for count in H:
        yield count, H[count]
        
        

if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(mapper, sumreducer)
    job.run()




