#!/usr/bin/env python

import sys


for line in sys.stdin:
	user,friends = line.rstrip().split("->") #Split each line in the file separated by --> which are user and friends list
	friends_list  = friends.split() # convert the friends to list of friends
	for i in friends_list: #iterate through the friends list
		key = [] # generate the key in the sorted order of user and friend
		if user > i: #If the user name starts with the letter after friend
			key = i + ' ' + user #then key will be friend ,user
		else:
			key = user + ' ' + i#else key will user, friend
		print("%s\t%s"%(key,friends))#printing the key and friends list separted by a tab

