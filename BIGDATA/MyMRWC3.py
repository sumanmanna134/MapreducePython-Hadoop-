#plain word count program with additional constraint of filtering out smaller words
#try to comment out the reducer to see the mapper output

from mrjob.job import MRJob

class MyMRWC3(MRJob):
    def mapper(self, key, line):
        words = line.split(' ')
        for word in words:
            if len(word)>=4:
                yield word, 1
            
    def reducer(self, word, count_one):
        yield word, sum(count_one)
        

if __name__ == '__main__':
    MyMRWC3.run()