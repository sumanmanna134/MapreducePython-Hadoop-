# find the custID who has purchased max number of products - version 1

from mrjob.job import MRJob
from mrjob.step import MRStep

class ShoppingBasket(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse,
                   reducer=self.reducer_groupby_userID),
            MRStep(mapper=self.mapper_invert,
            reducer=self.reducer_max)
        ]
    def mapper_parse(self, key, line):
        (userID, productID) = line.split(',')
        yield userID, 1

    def reducer_groupby_userID(self, userID, occurances):
        yield int(userID), sum(occurances)
        
    def mapper_invert(self, userID, total):
        yield 1,(total,userID)
        
    def reducer_max(self, key,user_count):
        yield max(user_count)
        
if __name__ == '__main__':
    ShoppingBasket.run()