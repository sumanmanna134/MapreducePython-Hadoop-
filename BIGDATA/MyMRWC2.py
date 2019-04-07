#plain word count program with additional output about word lengths
#try to comment out the reducer to see the mapper output

from mrjob.job import MRJob

class MyMRWC2(MRJob):
    def mapper(self, key, line):
        words = line.split(' ')
        for word in words:
            yield word, 1
            yield len(word),1
            
    def reducer(self, word, count_one):
        yield word, sum(count_one)
        

if __name__ == '__main__':
    MyMRWC2.run()