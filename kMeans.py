from __future__ import with_statement

'''
Created on Mar 1, 2012

@author: Nabin
         Kristine
         Sanjay
'''

from mr_kMeansIterate import MRkMeansIter
import simplejson as json
from math import sqrt
import os
import nltk

'''
This is a calling program to run several mr jobs
1. first it calls an mrjob to run through the data set and pick k random points as starting points for iteration
2. control iterative process by stepping through iterative improvements in centroid calculations until convergence
Improvements -
a. more orderly handling of path to intermediate results
b. really random selection of starting inputs
c. calculation of SSE
d. spread reducer calc over multiple reducers.

'''

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))

#
# calculate the jaccard distance between two document word vectors
#
def jaccard_dist(doc1, doc2):
    # input doc format: [docID, [wordId1, wordId2, etc]]
    s1 = set(doc1[1])
    s2 = set(doc2[1])
    return nltk.metrics.distance.jaccard_distance(s1, s2)


def main():
    #
    # initialize start by getting existing canopy centers and 
    # setting it to the intermediate results file
    #
    resultsPath = os.path.join(PROJECT_ROOT, 'intermediateResults.txt')
    fileIn = open(resultsPath)
    centroidsJson = fileIn.read()
    fileIn.close()

    filePath = os.path.join(PROJECT_ROOT, 'enron_corpus.txt')

    #
    # setup the delta that we consider threshhold for "close-enough"
    #
    # delta = 10
    delta = 59 

    #
    # Begin iteration on change in centroids
    #
    #while delta > 0.01:
    while delta > 27.25:
        # parse old centroid values
        # centersJson format: [ document1, document2,..., documentN ]
        oldCentroids = json.loads(centroidsJson)

        # run one iteration
        mrJob2 = MRkMeansIter(args=[filePath])
        with mrJob2.make_runner() as runner:
            runner.run()
            
        # compare new centroids to old ones
        fileIn = open(resultsPath)
        centroidsJson = fileIn.read()
        fileIn.close()
        newCentroids = json.loads(centroidsJson)
        
        kMeans = len(newCentroids)
        
        delta = 0.0
        for i in range(kMeans):
            new_center_doc = newCentroids[i]
            old_center_doc = oldCentroids[i]
            delta += jaccard_dist(new_center_doc, old_center_doc)
        
        print delta

if __name__ == '__main__':
    main()
