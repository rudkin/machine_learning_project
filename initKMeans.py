from __future__ import with_statement

'''
Created on Mar 1, 2012

@author: Nabin
         Kristine
         Sanjay
'''

import simplejson as json
import os
from random import sample

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))


#
# get all documents
#
corpus = []
fullPath = os.path.join(PROJECT_ROOT, 'enron_corpus.txt')
filein = open(fullPath, "r")
for line in iter(filein):
   jsonDocument = line.rstrip()
   document = json.loads( jsonDocument )
   corpus.append( document )
filein.close();

#
# randomly grab 50 points to be our kmeans centers
centroids = []
samples = sample( range(len(corpus)), 50 )
for idx in samples:
    centroids.append( corpus[idx] )

#
# output starting centers
centersOut = json.dumps( centroids )
fullPath = os.path.join(PROJECT_ROOT, 'intermediateResults.txt')
fileOut = open(fullPath,'w')
fileOut.write(centersOut)
fileOut.close()


