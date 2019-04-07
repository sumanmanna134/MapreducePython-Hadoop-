#count number of products group by custID
#!/usr/bin/env python

from mrjob.job import MRJob

class ShoppingBasket(MRJob):
    def mapper(self, key, line):
        (userID, productID) = line.split(',')
        yield userID, 1

    def reducer(self, userID, occurances):
        yield int(userID), sum(occurances)

if __name__ == '__main__':
    ShoppingBasket.run()
