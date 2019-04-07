#create shopping basket for each custID

from mrjob.job import MRJob

class ShoppingBasket(MRJob):
    def mapper(self, key, line):
        (userID, productID) = line.split(',')
        yield userID, productID

    def reducer(self, userID, productIDs):
        shopping_basket=[]
        for productID in productIDs:
            shopping_basket.append(int(productID))
                
        yield int(userID), shopping_basket
        
       # yield userID, ','.join(shopping_basket)
        
        #shopping_basket.sort()
        #yield int(userID),shopping_basket
        
if __name__ == '__main__':
    ShoppingBasket.run()
