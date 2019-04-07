#counting the lines which have the relevant word
#if the line has multiple relevant words count only once

from mrjob.job import MRJob

class MyMRWC(MRJob):
    def mapper(self, key, line):
        words = line.split(' ')
        relevant_line = 0
        for word in words:
            if word.lower() == 'line':
                yield word, 1
                if relevant_line == 0:
                    yield "relevant no. of line", 1
                    relevant_line += 1;
            else:	
                yield "others", 1    

    def reducer(self, word, count_one):
        yield word, sum(count_one)

if __name__ == '__main__':
    MyMRWC.run()