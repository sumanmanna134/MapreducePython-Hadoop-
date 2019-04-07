# find the custID who has purchased max number of products - version 2
# step 1: yields the shopping basket size per user
# step 2: create list of users having same basket size
# step 3: find the list of user(s) having the max size of shopping basket

from mrjob.job import MRJob
from mrjob.step import MRStep

class ShoppingBasket(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse,
                   reducer=self.reducer_groupby_userID),
            MRStep(mapper=self.mapper_invert1,
                    reducer=self.reducer_groupby_size),
            MRStep(mapper=self.mapper_invert2,
                    reducer=self.reducer_max)
        ]
    def mapper_parse(self, key, line):
        (userID, productID) = line.split(',')
        yield userID, 1

    def reducer_groupby_userID(self, userID, occurances):
        yield int(userID), sum(occurances)
     
    def mapper_invert1(self,userID,size):
        yield size,userID
           
    def reducer_groupby_size(self, size, userIDs):
        userID_list=[]
        for userID in userIDs:
            userID_list.append(userID)
        yield (size, userID_list)
        
    def mapper_invert2(self, size, userID_list):
        yield 1,(size,userID_list)
        
    def reducer_max(self, key,user_count):
        yield max(user_count)
        
if __name__ == '__main__':
    ShoppingBasket.run()