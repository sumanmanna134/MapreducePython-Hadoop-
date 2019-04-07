from mrjob.job import MRJob

class MyMRWC(MRJob):
    def mapper(self, key, line):
        words = line.split(' ')
        for word in words:
            yield word, 1
            self.increment_counter('word','no of words',1)	

    def reducer(self, word, count_one):
        yield word, sum(count_one)
        self.increment_counter('word','no of unique words',1)	

if __name__ == '__main__':
    MyMRWC.run()