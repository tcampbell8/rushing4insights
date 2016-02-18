__author__ = 'tatePro'
#football_ml.py

from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes 
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt

def dictionarize(line, columns):
	'''
	converts a list into a dict using the list columns as keys
	'''
	N = len(columns)
	line_dict = dict()
	for i in xrange(N):
		line_dict[columns[i].encode('utf-8')] = line[i].encode('utf-8')
	return line_dict

def label_description(line, labels):
	'''
	replaces a description with a label in labels if that label is contained in the description 
	'''
	for l in labels:
		if l in line['description']:
			line['description'] = l
			break
	return line

def conv_rush_labs(line, labels):
	for l in labels:
		if l in line['description']:
			line['description'] = 'rush'
			break
	return line

def encode_pass_rush(line):
	'''
	encodes description labels pass -> 1.0, rush -> 0
	'''
	if line['description'] == 'pass':
		line['description'] = 1.0
	elif line['description'] == 'rush':
		line['description'] = 0
	return line 

def prep_svm_data(line, variables):
	values = list()
	for v in variables:
		try:
			values.append(float(line[v]))
		except Exception as e:
			values.append(float(0))
	return LabeledPoint(values[0], values[1:])


d2008 = sc.textFile('nfl_pbp_data/2008_nfl_pbp_data.csv')
d2009 = sc.textFile('nfl_pbp_data/2009_nfl_pbp_data.csv')
d2010 = sc.textFile('nfl_pbp_data/2010_nfl_pbp_data.csv')
d2011 = sc.textFile('nfl_pbp_data/2011_nfl_pbp_data.csv')
d2012 = sc.textFile('nfl_pbp_data/2012_nfl_pbp_data.csv')

counts = d2008.count(),d2009.count(),d2010.count(),d2011.count(),d2012.count()

d12_8 = d2012.union(d2011).union(d2010).union(d2009).union(d2008)
nrows = d12_8.count() - 5
data = d12_8.map(lambda l: l.split(','))
names = data.take(1)[0]

data_dicts = data.map(lambda l: dictionarize(l, names))

event_types = ['kicks', 'pass', 'punts', 'sacked', 'extra point', 'field goal', 
'left end', 'left tackle', 'left guard', 'right end', 'right tackle',
'right guard', 'up the middle', 'kneels', 'PENALTY', 'FUMBLES', 'scrambles', 
'spiked', 'BLOCKED', 'RECOVERED', 'rushed', 'punted', 'TOUCHDOWN', 'Touchback',
'kick', 'kicked', 'SAFETY', 'TWO POINT CONVERSION', 'intercepted'
'TWO POINT CONVERSION ATTEMPT']

# all of the following event_types correspond to rushing plays: left end, left tackle,
# left guard, right end, right tackle, right guard, up the middle, rushed. 

# in order to perform classification we'll convert all the rushing play labels into 'rush'
# and then keep only data points with description equal to 'rush' or 'pass'

rushing_types = ['left end', 'left tackle','left guard', 'right end', 'right tackle',
 'right guard', 'up the middle', 'rushed']

data_lab_desc = data_dicts.map(lambda line: label_description(line, event_types))
just_labels = data_lab_desc.map(lambda line: line['description'])
label_distribution = Counter(just_labels.collect())
label_distribution

n_labeled = sum([label_distribution[e] for e in event_types])
print '%d out of %d (%2.2f percent) descriptions have been successfully labeled' % (n_labeled, nrows, 100*float(n_labeled)/nrows) 

data_converted_rush_labs = data_lab_desc.map(lambda line: conv_rush_labs(line, rushing_types))
just_converted_labels = data_converted_rush_labs.map(lambda line: line['description'])
conv_lab_dist = Counter(just_converted_labels.collect())
conv_lab_dist

##############################################################

# we'll encode 'pass' = 1.0 and 'rush' = 0

new_data = data_converted_rush_labs.filter(lambda line: line['description'] in ['rush', 'pass'])

nfl_data = new_data.map(lambda line: encode_pass_rush(line))

teamplays = nfl_data.map(lambda line: (line['off'], line['description']))
teamplays_red = teamplays.reduceByKey(lambda a,b: str(a) + ',' + str(b))
teamsplits = teamplays_red.map(lambda line: (line[0], Counter(line[1].split(','))))
splitdist = teamsplits.collect()
passrate = list()
#team_keys = [l[0] for l in splitdist]
for l in splitdist:
	passrate.append((l[0], float(l[1]['1.0'])/int(l[1]['1.0'] + l[1]['0'])))

passrate = sorted(passrate, key = lambda tup: tup[1])
passrate.pop()

teams = [k[0] for k in passrate]
rates = [100*k[1] for k in passrate]

plot_domain = [2*x for x in range(32)]
fig, ax = plt.subplots() 
plot = ax.bar(plot_domain, rates, width = 1.3, color='r') 
plt.xticks(rotation=90)
ax.set_xticks([n+0.6 for n in plot_domain])
ax.set_xticklabels(teams)
ax.set_ylim([45,65])
ax.set_xlim([-1,65])
ax.set_ylabel('Pass rate (%)')
ax.set_title('Empirical Passing Rates for NFL Teams \n (over 2002 - 2015 regular seasons)')
plt.savefig('pass_rate_by_team_final.jpeg')
plt.show()





VARS = ['description', 'qtr', 'min', 'down', 'togo', 'ydline', 'defscore', 'offscore']

data1 = nfl_data.map(lambda line: prep_svm_data(line, VARS))

svm_model = SVMWithSGD.train(data1, iterations=2000)
#logreg_model = LogisticRegressionWithLBFGS.train(data1)

# Evaluating the model on training data
labelsAndPreds = data1.map(lambda p: (p.label, svm_model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(data1.count())
print("SVM Training Error = " + str(trainErr))
