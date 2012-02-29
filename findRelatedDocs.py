from __future__ import with_statement

'''
findRelatedDocs.py
Given a set of enron email documents and their assignment to different clusters: find all documents that
are similar to a sample document; needs input files: enron_corpus.txt, canopy_clusters.txt, and vocab.enron.txt

Created on Feb 29, 2012
@author: Nabin
         Kristine
         Sanjay
'''

import os
import simplejson as json
from random import sample
import nltk
from operator import itemgetter


PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))

#
# calculate the jaccard distance between two document word vectors
#
def jaccard_dist(doc1, doc2):
    # input doc format: [docID, [wordId1, wordId2, etc]]
    s1 = set(doc1[1])
    s2 = set(doc2[1])
    return nltk.metrics.distance.jaccard_distance(s1, s2)


def remove_dupes(seq):
    # not order preserving
    set = {}
    map(set.__setitem__, seq, [])
    return set.keys()


#
# get the enron corpus of documents; read in line at a time so we don't
# overrun the input buffer
#
def get_corpus_documents(sfile):
    filePath = os.path.join(PROJECT_ROOT, sfile)
    fileIn = open( filePath, "r" )
    corpus = {} 
    for line in iter( fileIn ):
        line =  line.rstrip()
        line = json.loads(line)
        docId = line[0]
        wordVector = line[1]

        # create a document "object"
        document = []
        document.append(docId) 
        document.append(wordVector) 
        corpus[docId] = document

    fileIn.close()
    return corpus


#
# get canopy clusters; contains all the enron documents assigned to one or 
# more clusters
#
def get_canopy_clusters(sfile):
    canopies = []
    filePath = os.path.join(PROJECT_ROOT, sfile)
    fileIn = open( filePath, "r" )
    canopiesJson = fileIn.read()
    fileIn.close()
    canopies = json.loads( canopiesJson )
    return canopies


#
# return a dictionary with all vocabulary words so it is easier to
# simply index into the word; unfortunately, indexing starts
# at 1 and not 0, so account for this
#
def get_corpus_vocabulary(sfile):
    filePath = os.path.join(PROJECT_ROOT, sfile)
    fileIn = open(sfile, "r")
    words = {}
    idx = 1
    for line in iter(fileIn):
        line = line.rstrip()
        words[idx] = line
        idx += 1
    fileIn.close();
    return words


#
# given a sample document and the canopies of all documents, find similar docs
#
def find_similar_documents( targetDocument, canopies ):

    targetDocId = targetDocument[0]
    print "find_similar_documents, looking for target doc id: "+repr(targetDocId)

    # find all canopies that contain sampleDocId
    matchingCanopies = []
    for canopy in canopies:
        if targetDocId in canopy: 
            matchingCanopies.append( canopy )

    print "Found matching canopies list: "+repr(len(matchingCanopies))

    # randomly select two document ids from each matching canopy
    similarDocumentIds = []
    for canopy in matchingCanopies:
        sampleIndexes = sample( range(len(canopy)), 2 )
        for idx in sampleIndexes:
            similarDocumentIds.append( canopy[idx] )

    # make sure list of matching document ids is unique
    similarUniqueDocumentIds = remove_dupes( similarDocumentIds )

    # now sort the list by distance from target
    similarDocuments = sort_by_distance( targetDocument, similarUniqueDocumentIds ) 

    # return a list of documents
    return similarDocuments


#
# sort all the similar documents by their respective distances to the target document
#
def sort_by_distance( targetDocument, similarDocumentIds ):

    # get all the distances for each similar document
    rankings = {}
    for docId in similarDocumentIds:
        document = corpus[docId] 
        dist = jaccard_dist( targetDocument, document )
        rankings[docId] = dist

    # now sort by distance value; returns list of (key,value) pairs
    distanceSort = sorted(rankings.items(), key=itemgetter(1), reverse=True)

    # create final list of similar documents by distance
    finalSorted = []
    for tuple in distanceSort:
        docId = tuple[0]
        document = corpus[docId]
        finalSorted.append( document )

    return finalSorted
        
#
# given the document dictionary and the
# in-memory vocabulary , return a list of words
# instead of a vector of numbers.
#
def docprint( document, vocabulary ):
    docId = document[0]
    docWordList = document[1]
    rwords = []
    for word_idx in docWordList:
        rwords.append( vocabulary[word_idx] )
    print "Document ID: "+repr(docId)+"\n"
    print rwords
    print "\n\n"


#
# main:
# read in all the data needed to do similarity comparisons
#
corpus = get_corpus_documents( 'enron_corpus.txt' )
vocabulary = get_corpus_vocabulary( "vocab.enron.txt" )
canopies = get_canopy_clusters( 'canopy_clusters.txt' )

#
# choose a random sample(s) from enron corpus
#
randomSamples = sample( corpus.keys(), 1 )

#
# find the documents closest to each sample selected
#
for random_idx in randomSamples:
    sampleDocument = corpus[random_idx]
    similarDocuments = find_similar_documents( sampleDocument, canopies )

    # print out sample document
    print "Sample Document: \n"
    docprint( sampleDocument, vocabulary )

    # ok, found closest documents; print them out
    print "Similar Documents: \n"
    count = 0
    for document in similarDocuments:
        count += 1
        #similar_document = corpus[similar_docId]
        docprint( document, vocabulary )
        if count >= 2:
            break

