#create shopping basket for each custID and sort,invert and format the output
# result using chain of MR and also map name to custID using helper data file
# python shopping_basket4.py --names=Cust_names.txt sc.txt

from mrjob.step import MRStep
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import RawValueProtocol

class ShoppingBasket(MRJob):

    def configure_options(self):
        super(ShoppingBasket, self).configure_options()
        self.add_file_option('--names', help='Path to Cust_Names.txt')
        
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse,
                   reducer=self.reducer_groupby_userID,
                   reducer_init=self.get_names),
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
        userName = self.custNames[int(userID)]          
        yield userName, shopping_basket
        
    
    def mapper_sort(self, userID, shopping_basket):
        shopping_basket.sort()
        out_line=[]
        out_line.append(userID)
        for item in shopping_basket:
            out_line.append(item)
        yield "",str(out_line)[1:-1]
        
    def get_names(self):
        self.custNames = {}

        with open("Cust_Names.txt", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split(',')
                custID = int(fields[0])
                self.custNames[custID] = fields[1].strip()
        
if __name__ == '__main__':
    ShoppingBasket.run()