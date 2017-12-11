from heapq import heappush,heappop,heapreplace,heappushpop

import heapq
class Solution:
    """
    @param nums: A list of integers.
    @return: The median of numbers
    """
    
    minHeap, maxHeap = [], []
    numbers = 0
    def medianII(self, nums):
        ans = []
        for item in nums:
            self.add(item)
            ans.append(self.getMedian())
        return ans
        
    
    def getMedian(self):
        return -self.maxHeap[0]

    def add(self, value):
        if self.numbers % 2 == 0:
            heapq.heappush(self.maxHeap, -value)
        else:
            heapq.heappush(self.minHeap, value)
        self.numbers += 1
        if len(self.minHeap)==0 or len(self.maxHeap)==0:
            return

        if -self.maxHeap[0] > self.minHeap[0]:
            maxroot = -self.maxHeap[0]
            minroot = self.minHeap[0]
            heapq.heapreplace(self.maxHeap, -minroot)
            heapq.heapreplace(self.minHeap, maxroot)
            #print minroot, maxroot
        