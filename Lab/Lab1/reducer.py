#!/usr/bin/env python

import sys

a = [] #contains list of existing keys
dict = {} #output dictionary

for line in sys.stdin:#for each line in the input
    key,value = line.rstrip().split("\t")#split the key value pairs seperated by \t
    key.split() #convert to key list
    value.split()# convert to value list
    if key in a:# if key value exists in a
        dict[key] = list(set(dict[key]) & set(value))#value is intersection of both the value pairs
        dict[key].remove(' ')#remove spaces
    else:
        a.append(key) #append the key value to a
        dict[key] = list(value)#value is just the value
        dict[key].remove(' ')#remove the spaces

for k,v in dict.items(): #printing the output dictionary
    print("(%s) -> (%s)"%(k,' '.join(map(str,v))))