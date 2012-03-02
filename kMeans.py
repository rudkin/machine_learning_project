from __future__ import with_statement

'''
Created on Mar 1, 2012

@author: Nabin Acharya
         Kristine Rudkin
         Sanjay Tibrewal
'''

from mr_kMeansIterate import MRkMeansIter
import simplejson as json
from math import sqrt
import os
import nltk

'''
This is a calling program to run an mr job that iterates over a set of
Eron documents and performs kMeans clustering on the documents
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

    # input file
    filePath = os.path.join(PROJECT_ROOT, 'enron_corpus.txt')

    #
    # setup the delta that we consider threshhold for "close-enough"
    #
    delta = 10

    #
    # Begin iteration on change in centroids
    #
    while delta > 0.01:
        # parse old centroid values
        # centersJson format: [ document1, document2,..., documentN ]
        oldCentroids = json.loads(centroidsJson)

        # run one iteration
        mrJob2 = MRkMeansIter( args=[filePath] )
        with mrJob2.make_runner() as runner:
            runner.run()
            
        # compare new centroids to old ones
        # determine if we are close enough to a final clustering solution 
        fileIn = open(resultsPath)
        centroidsJson = fileIn.read()
        fileIn.close()
        newCentroids = json.loads(centroidsJson)
       
        delta = 0.0
        for i in range(len(newCentroids)):
            new_center_doc = newCentroids[i]
            old_center_doc = oldCentroids[i]
            dist = jaccard_dist(new_center_doc, old_center_doc)
            delta += jaccard_dist(new_center_doc, old_center_doc)
       
        delta = delta / len(newCentroids) 
        print delta
        exit()

if __name__ == '__main__':
    main()
