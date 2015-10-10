import cPickle
import numpy as np
import time, datetime
import os, sys, getopt,gc
from scipy.sparse import *

from multiprocessing import Pipe,Process
import operator

import json

settings = json.load(open("../dataset.json"))

NUM_RECOMMENDATIONS = 30
NUM_NEIGHBORS = 130 # Cannot excced 200
SIM_THRESHOLD = 0.0 # default is 0
NUM_CORES = 4
NUM_JOBS = 100

data_period = None
data_paths = None

try:
	opts, args=getopt.getopt(sys.argv[1:], "k:s:c:d:")
except:
	print "train.py -k <NUM_NEIGHBORS> -s <SIM_THRESHOLD> -c <NUM_CORES> -d <DATA_PERIOD>"
	exit(2)
for opt, arg in opts:
	if opt == '-k':
		NUM_NEIGHBORS = int(arg)
	elif opt == "-s":
		SIM_THRESHOLD = float(arg)
	elif opt == "-c":
		NUM_CORES = int(arg)
	elif opt == "-d":
		data_period = arg
		data_paths = settings[data_period]

if data_paths == None:
	print "train.py -k <NUM_NEIGHBORS> -s <SIM_THRESHOLD> -c <NUM_CORES> -d <DATA_PERIOD>"
	exit(2)

similarity_f = "similarities__d_%s__s_%s.csv" % (data_period, str(SIM_THRESHOLD))

print NUM_NEIGHBORS, "neighbors"
print "similarity threshold:", SIM_THRESHOLD
print NUM_CORES, "cores"
print "Data period =", data_period
print "Similarity file:", similarity_f

execfile("../utilities/read_txn.py")

users_to_recommend = []
for line in open(data_paths["users"]):
	users_to_recommend.append(int(line.split(",")[0]))

users_to_recommend_set = set(users_to_recommend)

useful_users = filter(lambda x:x in users_to_recommend_set, user_ids)

# similarities = []
if os.path.isfile(similarity_f):
	print "Delay Loading similarities, but checking length"
	count = 0
	for line in open(similarity_f): count += 1
	assert len(useful_users) == count
	txn_sparse_rows = None
	del txn_sparse_rows
	txn_sparse_cols = None
	del txn_sparse_cols
	gc.collect()
else:
	execfile("recommendation_calcsim.py")

class SimFinder(object):
	def __init__(self, filename, useful_users):
		self.f = open(filename, "r")
		self.pointer = 0
		self.useful_users = useful_users
		self.bound = len(self.useful_users)
	def get(self, contest_id):
		assert useful_users[self.pointer] <= contest_id
		line = None
		while self.pointer < self.bound and useful_users[self.pointer] <= contest_id:
			line = self.f.readline()
			self.pointer += 1
		return self.extract(line)
	def extract(self, line):
		line=line.strip().split(",")
		tmp=[]
		if line[0] != "":
			for l in line:
				l=l.split("_")
				tmp.append((float(l[0]),int(l[1])))
		return tmp
	def close(self):
		self.f.close()


print "Calculating most popular items"
most_visited = sorted([(i, visit_count[i]*1.0/user_count) for i in range(item_count)], key=lambda x:x[1], reverse=True)


txn_matrix_by_item = None
del txn_matrix_by_item
gc.collect()


# Parallel recommendation
def recommend(i,contest_users): # users are contest ids
	global txn_matrix_by_user, NUM_NEIGHBORS, user_ids, item_ids, users, label
	print "Thread %d started" % i
	toret = []
	start_time=time.time()
	total = len(contest_users)
	count = 0
	sim_finder = SimFinder(similarity_f, useful_users)
	f=open("tmp__" + label+"."+str(i),'w')
	for user_id in contest_users:
		if user_id in users:
			user = users[user_id]
			already_visited=txn_matrix_by_user[user]
			scores = dict()
			# neighbors = similarities[user_id][:NUM_NEIGHBORS] # similarities use contest user id as key
			neighbors = sim_finder.get(user_id)[:NUM_NEIGHBORS]
			for (similarity, u2) in neighbors:
				for m2 in txn_matrix_by_user[u2]:
					if m2 in already_visited: continue
					if m2 not in scores: scores[m2] = 0.0
					scores[m2] += similarity
			n_neighbors = len(neighbors)
			recommendations = map(lambda x: (x[0], x[1]/n_neighbors), sorted(scores.items(), key=operator.itemgetter(1), reverse=True)[:NUM_RECOMMENDATIONS])
			if len(recommendations) < NUM_RECOMMENDATIONS:
				recommendations_items = set(map(lambda x:x[0], recommendations))
				for (m2,score) in most_visited:
					if m2 not in recommendations_items and m2 not in already_visited:
						recommendations.append((m2,0)) # Depress scores from popularity based recommendations
						recommendations_items.add(m2)
						if len(recommendations_items) >= NUM_RECOMMENDATIONS: break
			f.write("%d\t[%s]\n" % (user_id, ",".join(map(lambda x:str(item_ids[x[0]])+":"+str(x[1]), recommendations))))
		else:
			f.write("%d\t[%s]\n" % (user_id, ",".join(map(lambda x:str(item_ids[x[0]])+":0",most_visited[:NUM_RECOMMENDATIONS]))))
		count += 1
		if count % 1000 == 0:
			print "Thread %d: %d/%d, %.2f sec" % (i,count,total,time.time()-start_time)
			start_time=time.time()
	print "Thread %d: Finished" % i
	f.close()

current_time=datetime.datetime.now().strftime("%m%d%H%M%S")
label="UserBased__d_%s__k_%d__s_%s__t_%s" % (data_period, NUM_NEIGHBORS, str(SIM_THRESHOLD), current_time)
# Recommendation script
my_processes = []
chunk_size = len(users_to_recommend)/NUM_CORES # Notice: user_count instead of item_count
starts = []
ends = []
for i in range(NUM_CORES):
	starts.append(i*chunk_size)
	ends.append((i+1)*chunk_size)
	if i == NUM_CORES - 1:
		ends[-1] = len(users_to_recommend)
for i in range(NUM_CORES):
	my_processes.append(Process(target=recommend, args=(i,users_to_recommend[starts[i]:ends[i]])))
	my_processes[i].start()
for i in range(NUM_CORES):
	print "Receiving from thread %d" % i
	my_processes[i].join()
with open(label, 'w') as f:
	for i in range(NUM_CORES):
		for line in open("tmp__" + label+"."+str(i)):
			f.write(line)

