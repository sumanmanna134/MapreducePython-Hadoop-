#plain word count program with additional output about word lengths
#we try to compute statistics about word lengths - mean, min, max

from mrjob.step import MRStep
from mrjob.job import MRJob
from statistics import mean

    

class MyMRWC4(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]
    def mapper1(self, key, line):
        words = line.split(' ')
        for word in words:
            yield len(word),1
            
    def reducer1(self, word, count_one):
        yield word, sum(count_one)
        
    def mapper2(self, key, line):
        yield 'Size', key
            
    def reducer2(self, label, size):
        l = list(size)
        yield label,l
        yield 'average', mean(l)
        yield 'min', min(l)
        yield 'max', max(l)
        
        
        

if __name__ == '__main__':
    MyMRWC4.run()