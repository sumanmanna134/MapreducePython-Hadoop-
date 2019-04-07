#count the number of 'a' and 'e' in each word and list the set of unique words and their
#length, display sorted in ascending order of number of relevant characters in the word
#example for input--->

#this is my first line rkmveri
#rkmveri this makes it
#hardly any relevant here
#we have it here rkmveri again rkmveri very relevant
#this line is full of aeae
#this line has another aeae
#aeae is everywhere

#output --->

#null	[[0, ["first", 1]], [0, ["full", 1]], [0, ["is", 3]], [0, ["it", 2]], 
#[0, ["my", 1]], [0, ["of", 1]], [0, ["this", 4]], [1, ["any", 1]], [1, ["hardly", 1]], 
#[1, ["has", 1]], [1, ["line", 3]], [1, ["rkmveri", 4]], [1, ["very", 1]], [1, ["we", 1]], 
#[2, ["again", 1]], [2, ["another", 1]], [2, ["have", 1]], [2, ["here", 2]], 
#[2, ["makes", 1]], [3, ["relevant", 2]], [4, ["aeae", 3]], [4, ["everywhere", 1]]]


from mrjob.job import MRJob
from mrjob.step import MRStep

class MyMRWC(MRJob):

   
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(mapper=self.mapper2,
                    reducer=self.reducer2)
            
        ]
    def mapper1(self, key, line):
        words = line.split(' ')
        for word in words:
            yield word, 1

    def reducer1(self, word, count_one):
        yield word, sum(count_one)
        
    def mapper2(self, word, count):
        ra = 0
        for i in range(len(word)):
            if word[i]== 'a' or word[i]== 'e':
                ra += 1
        yield None, (ra, (word,count))
        
    def reducer2(self,_, nwc):
        l =[]
        for elem in nwc:
            l.append(elem)
        l.sort()
        yield None,l
                
if __name__ == '__main__':
    MyMRWC.run()