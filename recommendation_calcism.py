import cPickle
import numpy as np
import time, datetime
import os,gc
from scipy.sparse import *

from multiprocessing import Pipe,Process,Queue
import operator
from heapq import nlargest

assert "users_to_recommend" in globals(), "train.py has not run yet"

user_visit_count = map(len, txn_matrix_by_user)

txn_matrix = csr_matrix(([1]*len(txn_sparse_rows), (txn_sparse_rows, txn_sparse_cols)), shape=(user_count, item_count), dtype="uint32")

txn_sparse_rows = None
del txn_sparse_rows
txn_sparse_cols = None
del txn_sparse_cols
gc.collect()

# Parallel similarity calcuation
def calc_sim(i,user_indexes,txn_matrix,user_visit_count,user_count,SIM_THRESHOLD):
	print "Thread %d started" % i
	toret = []
	start_time=time.time()
	txn_matrix_t = txn_matrix.transpose()
	count = 0
	total = len(user_indexes)
	cache = []
	end = max(user_indexes)+1
	cache_size = 0
	cache_starting = 0
	range_user_count = range(user_count)
	f=open("tmp__" + similarity_f+"."+str(i),'w')
	for u1 in user_indexes:
		similarities = []
		if u1 - cache_starting >= cache_size:
			print "Thread %d: %d/%d, %.2f sec" % (i,count,total,time.time()-start_time)
			start_time=time.time()
			print "Thread %d: Filling Cache (%d-%d)" % (i, u1, min(end,u1+100))
			cache = (txn_matrix[u1:min(end,u1+100)]*txn_matrix_t).todense()
			cache_top_users = np.asarray(np.argsort(cache)[:,-1401:])
			# cache_top_users = np.asarray(np.argsort(cache)[:,-201:])
			# cache_size = len(cache)
			cache_size = cache.shape[0]
			cache_starting = u1
			print "Thread %d: Done Filling Cache (%d-%d), %.2f sec" % (i,u1, min(end,u1+100), time.time()-start_time)
			start_time=time.time()
		for u2 in cache_top_users[u1 - cache_starting,:]:
			if u1 == u2: continue
			numer = cache[u1 - cache_starting,u2]
			if numer > 0:
				if user_visit_count[u2] == numer: continue
				similarity = numer * 1.0 / (user_visit_count[u1]+user_visit_count[u2]-numer)
				if similarity >= SIM_THRESHOLD:
					similarities.append((similarity, u2))
		similarities = nlargest(200, similarities, key=lambda x:x[0])
		count += 1
		f.write(",".join(map(lambda x:str(x[0])+"_"+str(x[1]), similarities))+"\n")
	print "Thread %d: Finished" % i
	f.close()

my_processes = []
chunk_size = len(useful_users)/NUM_CORES

starts = []
ends = []

for i in range(NUM_CORES):
	starts.append(i*chunk_size)
	ends.append((i+1)*chunk_size)
	if i == NUM_CORES - 1:
		ends[-1] =  len(useful_users)


for i in range(NUM_CORES):
	my_processes.append(Process(target=calc_sim, args=(i,map(lambda x:users[x],useful_users[starts[i]:ends[i]]),txn_matrix,user_visit_count,user_count,SIM_THRESHOLD)))
	my_processes[i].start()


for i in range(NUM_CORES):
	print "Receiving from thread %d" % i
	my_processes[i].join()

with open(similarity_f, 'w') as f:
	for i in range(NUM_CORES):
		print "Loading %d/%d" % (i,NUM_CORES)
		for line in open("tmp__" + similarity_f+"."+str(i)):
			f.write(line)


