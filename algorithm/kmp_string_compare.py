#!/usr/bin/python
 
def getNext(pattern):
    next=[]
    j = 0
    plen = len(pattern)
    next.append(0)
    for i in range(1, plen):
        print pattern,pattern[i],pattern[j],i,j
        while ((j > 0) and (pattern[j] != pattern[i])):
            j = next[j-1]
        if (pattern[i] == pattern[j]):
            j = j + 1
        next.append(j)
    return next
     
 
 
def kmp_search(text, pattern):
    next=getNext(pattern)
    print next
    j = 0;
    plen = len(pattern)
    tlen = len(text)

    for i in range(0, tlen):
        while j > 0 and text[i] != pattern[j]:
            j = next[j-1]

        if text[i] == pattern[j] :
            j = j + 1;

        if j == plen :
            print "from ", i-j+1, "to ", i
            j = next[j-1]


text = ("aaaaaaaabaaaaaaaaaaaabc")
pattern = ("ababababababc")

 
kmp_search(text, pattern)
