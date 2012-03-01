'''
Created on Mar 1, 2012

@author: Nabin
         Kristine
         SanJay
'''

import os
from mrjob.job import MRJob
from math import sqrt
#from numpy import mat, zeros, shape, random, array, zeros_like
import numpy as np
from random import sample
import simplejson as json
import nltk

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))


#
# calculate the jaccard distance between two document word vectors
#
def jaccard_dist(doc1, doc2):
    # input document format: [docID, [wordId1, wordId2, etc]]
    s1 = set(doc1[1])
    s2 = set(doc2[1])
    return nltk.metrics.distance.jaccard_distance(s1, s2)


#
# given a target and list of [document,distance] pairs, find which pair comes
# closest to the target distance
#
def closest_document(target, collection):
    smallest_dist = 9999
    closest_idx = 0
    idx = 0
    for docitem in collection:
        document = docitem[0]
        jac_distance = docitem[1]
        if abs(target-jac_distance) < smallest_dist:
           smallest_dist = abs(target-jac_distance)
           closest_idx = idx
        idx += 1 
    return closest_idx

#
# start iterating through to find kMeans clusters
#
class MRkMeansIter(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(MRkMeansIter, self).__init__(*args, **kwargs)

        # 
        # get current set of centroids: every mapper needs a set of centroids to work off of
        # in format [ document1, document2, ..., documentN ]
        # where each document is of format: [ docId, [wordIds] ]
        #
        self.centroids = [] 
        fullPath = os.path.join(PROJECT_ROOT, 'intermediateResults.txt')
        fileIn = open(fullPath)
        centroidsJson = fileIn.read()
        fileIn.close()
        self.centroids = json.loads(centroidsJson)

        self.kMeans = len(self.centroids)
        self.numMappers = 1 # number of mappers

        # log file for debugging purposes
        self.logOut = None
        logPath = os.path.join(PROJECT_ROOT, 'kmeans_progress.log')
        if not self.logOut:
            self.logOut = open(logPath,'w')
            self.logOut.write("Inside mr_kMeansIterate.py init \n")
        self.logOut.write( "end of init, num of entroids = "+repr(len(self.centroids))+"\n")

                                                 
    def configure_options(self):
        super(MRkMeansIter, self).configure_options()
      
 
    def mapper(self, key, val):
        self.logOut.write("in kmeans Mapper...\n")

        #
        # map each val (document) onto closest centroid point
        # document = [docId, [wordId1, wordId2, ..., wordIdN]]
        #
        document = json.loads(val)
        self.logOut.write("In mapper, looking at document: "+repr(document)+"\n")
        
        #
        # now get the shortest distance between all other centroid clusters
        # and our document; the centroid set with shortest distance is where we
        # assign our document
        #
        final_distance = jaccard_dist( document, self.centroids[0] )
        found_cluster_idx = 0
        i = 0
        for centroid_document in self.centroids:
            tmp_distance = jaccard_dist( document, centroid_document )
            if( tmp_distance < final_distance ):
                found_cluster_idx = i
                final_distance = tmp_distance
            i += 1

        #
        # for this mapper, we just output closest centroid cluster this document values into, and the 
        # input document with it's distance 
        #
        out = [found_cluster_idx, final_distance, document]
        jOut = json.dumps(out)
        yield 1,jOut

    
    def reducer(self, key, xs):
        self.logOut.write("In kMeans reducer...\n")
        self.logOut.write("xs  "+repr(xs)+"\n")

        #
        # slightly different input: key is still arbitrary, but xs is list of [centroid_index, distance, document]
        # first, we need to do the cluster centroid summation initialization
        #
        cluster_doc_list = {}
        cluster_sums = {}

        # 
        # find new centroid distance of collected clusters
        #
        for val in xs:
            #
            # basically, here, add up all the sums of all centroid clusters;
            #
            temp = json.loads(val)
            cluster_idx  = temp[0]
            jac_distance = temp[1]
            document     = temp[2]
           
            # now add this document distance into the overall cluster summation
            if cluster_sums.has_key(cluster_idx):
                cluster_sums[cluster_idx] += jac_distance
            else:
                cluster_sums[cluster_idx] = jac_distance

            if not cluster_doc_list.has_key(cluster_idx):
                cluster_doc_list[cluster_idx] = []
            cluster_doc_list[cluster_idx].append( [document, jac_distance] )

        #
        # output current set of documents assigned to clusters
        # a list of clusters without the cluster key (key was only necessary for combining)
        #
        clusters = []
        for ckey in cluster_doc_list:
            docitem = cluster_doc_list[ckey]
            clusters.append( docitem[0] )
        clustersOut = json.dumps(clusters)
        fullPath = os.path.join(PROJECT_ROOT, 'canopy_clusters.txt')
        fileOut = open(fullPath,'w')
        fileOut.write(clustersOut)
        fileOut.close()

        #
        # now find new set of centroids in all the clusters
        #
        newCentroids = []
        for cidx in cluster_doc_list:
            self.logOut.write("looking at sums for cluster : "+repr(cidx)+"\n")
            sum = cluster_sums[cidx]
            num_docs = len(cluster_doc_list[cidx])
            new_avg_distance = sum / num_docs
            self.logOut.write("sum = "+repr(sum)+", num_docs = "+repr(num_docs)+", avg-distance = "+repr(new_avg_distance)+"\n")
            self.logOut.write( "cluster "+repr(cidx)+": new avg distance = "+repr(new_avg_distance) )
            self.logOut.write( "\tchecking against centroid doc list: "+repr(cluster_doc_list[cidx]) )

            # which document in the cluster now represents the new avg distance? 
            doc_idx = closest_document( new_avg_distance, cluster_doc_list[cidx] )
            self.logOut.write("closest document to new avg: "+repr(doc_idx)+"\n")
            newCentroids.append( cluster_doc_list[cidx][doc_idx][0] )
 
        # 
        # write new centroids to file
        #
        centOut = json.dumps(newCentroids)
        fullPath = os.path.join(PROJECT_ROOT, 'intermediateResults.txt')
        fileOut = open(fullPath,'w')
        fileOut.write(centOut)
        fileOut.close()

        if False: yield 1,2
        
    #def steps(self):
    #    return ([self.mr(mapper=self.mapper,reducer=self.mapper,mapper_final=None)])
            

if __name__ == '__main__':
    MRkMeansIter.run()
