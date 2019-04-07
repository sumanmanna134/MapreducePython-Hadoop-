#same as word_count_ver_3 but a clearer formatting of result as
#output --->
#2	[["have", 1], ["here", 2], ["makes", 1], ["again", 1], ["another", 1]]
#3	[["relevant", 2]]
#4	[["everywhere", 1], ["aeae", 3]]
#1	[["very", 1], ["has", 1], ["line", 3], ["rkmveri", 4], ["any", 1], ["hardly", 1], ["we", 1]]
#0	[["this", 4], ["is", 3], ["it", 2], ["my", 1], ["of", 1], ["first", 1], ["full", 1]]

#try sorting the list on each output line in alphabetical order


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
        yield ra, (word,count)
        
    def reducer2(self,ra, nwc):
        l =[]
        for elem in nwc:
            w,c=elem
            l.append((w,c))
        #l.sort()
        yield ra,l
        
if __name__ == '__main__':
    MyMRWC.run()
    
            
    