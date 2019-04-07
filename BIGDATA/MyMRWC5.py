#plain word count program with additional output about word lengths
#we try to compute statistics about word lengths - mean, min, max

from mrjob.step import MRStep
from mrjob.job import MRJob
from statistics import mean

    

class MyMRWC5(MRJob):

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
        yield 'Size', (key,line)
            
    def reducer2(self, label, size):
        l = list(size)
        l1=[]
        l2=[]
        for item in l:
            length,count=item
            l1.append(length)
            l2.append(count)
        yield label,l
        yield 'min', min(l1)
        yield 'max', max(l2)
        numerator=0
        denominator=0
        for i in range(len(l1)):
            numerator+=l1[i]*l2[i]
            denominator+=l2[i]
        average = numerator / denominator
        yield 'average',average
        
if __name__ == '__main__':
    MyMRWC5.run()