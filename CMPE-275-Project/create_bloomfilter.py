from bloomfilter import BloomFilter
from random import shuffle
from businesslogic import cassandra_connect
n = 20 #no of items to add
p = 0.05 #false positive probability
 
bloomf = BloomFilter(n,p)
cc = cassandra_connect()

print("Size of bit array:{}".format(bloomf.size))
print("False positive Probability:{}".format(bloomf.fp_prob))
print("Number of hash functions:{}".format(bloomf.hash_count))
 
# words to be added
count, word_present = cc.get_id()
print(count)
# word not added
word_absent = [100005, 100006, 100007]
 
for item in word_present:
    bloomf.add(bytes(item))
 
shuffle(word_present)
shuffle(word_absent)
 
test_words = word_present[:10] + word_absent
shuffle(test_words)
for word in test_words:
    if bloomf.check(bytes(word)):
        if word in word_absent:
            print("'{}' is a false positive!".format(word))
        else:
            print("'{}' is probably present!".format(word))
    else:
        print("'{}' is definitely not present!".format(word))