#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob # import the mrjob library
from mrjob.step import MRStep # import the mrStep library

class MRtarget(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # the map step: each line in the csv file is read as a key, value pair
    # in this case, each line in the csv file only contains a value but no key
    # _ means that in this case, there is no key for each line
    def mapper(self, _, target):
        yield target, 1
        
        
    # the reduce step: combine all tuples with the same key
    # in this case, the key is the target
    # then sum all the values of the tuple, which will give the total value
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRtarget.run()

