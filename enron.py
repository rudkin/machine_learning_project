import os
from random import sample
import simplejson as json
import nltk

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))


enron_file = "docword.enron.txt"
vocab_file = "vocab.enron.txt"

numDocumentsInCorpus = 0
numVocabularyWords = 0
totalCorpusWords = 0

# given the enron document build a dictionary with 
# doc id as the key and a vector of word ids as the value
# returns the dictionary (in-memory)
def all_documents(sfile):
    filein = open(sfile, "r")

    # first three lines are, respectively, 
    # 1. number of documents in corpus, 
    # 2. number of unique vocabulary words, 
    # 3. total words overall in corpus
    numDocumentsInCorpus = filein.readline().rstrip()
    numVocabularyWords = filein.readline().rstrip()
    totalCorpusWords = filein.readline().rstrip()

    # all remaining file input lines are <docId wordId count> format
    documents = {}
    for line in iter(filein):
        line =  line.rstrip()
        vec = line.split(" ")
        docId = int(vec[0])
        wordId = int(vec[1])
        if documents.has_key(docId):
            words = documents[docId]
            words.append(wordId)
        else: 
            words = []
            words.append(wordId)
            documents[docId] = words
    filein.close()
    return documents

#
# use the vocabulary file and read it in memory 
# return a vector with all the words so it is easier to 
# simply index into the word
#
def docwords(sfile):
    filein = open(sfile, "r")
    words = []
    for line in iter(filein):
        line = line.rstrip()
        words.append(line)
    filein.close();
    return words

#
# given the document dictionary and the 
# in-memory vocabulary , return a list of words 
# instead of a vector of numbers. 
#
def docprint(document, words):
    rwords = []
    for idx in iter(document):
        i = idx
        rwords.append(words[i])    
    print rwords

#
# calculate the jaccard distance between two document word vectors
#
def doc_jaccard(doc1, doc2):
    s1 = set(doc1)
    s2 = set(doc2)
    return nltk.metrics.distance.jaccard_distance(s1, s2)

#
# main:
# parse the document into a dictionary
#
docs = all_documents(enron_file)

# now print out the jaccards
print "Jaccard between docId 1 and 4: ", doc_jaccard(docs[1], docs[4])
print "Jaccard between docId 2 and 4:", doc_jaccard(docs[2], docs[4])
print "Jaccard between docId 3 and 4:", doc_jaccard(docs[3], docs[4])
print "Jaccard between docId 4 and 4:", doc_jaccard(docs[4], docs[4])

# parse the vocabulary into memory
#words = docwords(vocab_file)
#docprint(docs['118'], words)

#
# write out docs with associated word/term vectors to file; even though
# ordering is same in corpus, technically docId values do not have to be
# sequential
#
fullPath = os.path.join(PROJECT_ROOT, 'enron_corpus.txt')
fileOut = open(fullPath,'w')
last_index = len(docs) + 1
#for i in range(1, last_index):
for i in range(1, 501):
    outString = json.dumps([i,docs[i]]) + "\n"
    fileOut.write(outString)
fileOut.close()

#
# now from our corpus, take random samples and find the distances
# 
#samples_1 = sample( range(len(corpus)), 20 )
#print samples_1
#samples_2 = sample( range(len(corpus)), 20 )
#print samples_2
#for i in range(len(samples_1)):
#    doc1_idx = samples_1[i]
#    doc2_idx = samples_2[i]
##    doc1 = corpus[doc1_idx]
#    doc2 = corpus[doc2_idx]
#    print doc1[0], doc2[0], doc_jaccard(doc1[1], doc2[1])
 

