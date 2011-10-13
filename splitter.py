import re
from dumbo.lib import *

class Mapper:
    def __init__(self):
        self.sentence = []
        
    def __call__(self, key, value):
        if value[0] == "<":
            m = re.match(r'<text id="wikipedia:(.*)">', value)
            if m:
                self.article = m.group(1)
            elif value == "<s>":
                self.sentence = [value]
            elif value == "</s>":
                self.sentence += [value]
                yield self.article, self.sentence
        else:
            self.sentence += [value]
        
if __name__ == '__main__':
    import dumbo
    job = dumbo.Job()
    job.additer(Mapper, identityreducer)
    job.run()