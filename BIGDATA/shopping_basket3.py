#create shopping basket for each custID and sort the output
# result using chain of MR

from mrjob.step import MRStep
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import RawValueProtocol

class ShoppingBasket(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse,
                   reducer=self.reducer_groupby_userID),
            MRStep(mapper=self.mapper_sort)
        ]
    OUTPUT_PROTOCOL = RawValueProtocol
    #OUTPUT_PROTOCOL = JSONValueProtocol
    
    def mapper_parse(self, key, line):
        (userID, productID) = line.split(',')
        yield userID, productID

    def reducer_groupby_userID(self, userID, productIDs):
        shopping_basket=[]
        for productID in productIDs:
            shopping_basket.append(int(productID))            
        yield int(userID), shopping_basket
        
    
    def mapper_sort(self, userID, shopping_basket):
        shopping_basket.sort()
        out_line=[]
        out_line.append(userID)
        for item in shopping_basket:
            out_line.append(item)
        yield "",str(out_line)[1:-1]
        
if __name__ == '__main__':
    ShoppingBasket	.run()