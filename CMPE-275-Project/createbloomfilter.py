from bloomfilter import BloomFilter
from random import shuffle
from connection import ConnectToCassandra
import datetime
import struct

def to_integer(dt_time):
        #dt_time = datetime.datetime.strptime(dt, "%Y-%m-%d")
        return 10000*dt_time.year + 100*dt_time.month + dt_time.day

class CreateBloomFilter():
    def __init__(self):
        self.cc = ConnectToCassandra()
        self.n, self.word_present = self.cc.get_id() #no of items to add
        self.p = 0.05 #false positive probability
        self.bloomf = BloomFilter(self.n,self.p)
        for item in self.word_present:
            self.bloomf.add(bytes(to_integer(item.date())))
    
    def createfilter(self):
        for item in self.word_present:
            self.bloomf.add(bytes(to_integer(item)))
    
    def testdate(self, todate):
        todate = to_integer(todate)
        if self.bloomf.check(bytes(todate)):
            return 1
        else:
            return 0
    
    


        
