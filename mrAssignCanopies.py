import os
from mrjob.job import MRJob
import nltk
import simplejson as json
from math import sqrt

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))


#
# calculate the jaccard distance between two document word vectors
#
def jaccard_dist(doc1, doc2):
    # input doc format: [docID, [wordId1, wordId2, etc]]
    s1 = set(doc1[1])
    s2 = set(doc2[1])
    return nltk.metrics.distance.jaccard_distance(s1, s2)

#
# assign each document in the enron corpus to a canopy cluster;
# canopy cluster center points have been discovered via step2,
# mrChooseCanopies.py 
# 
class mrAssignCanopies(MRJob):

    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(mrAssignCanopies, self).__init__(*args, **kwargs)
        self.T2 = 0.94                  # T2 threshhold, where T2 < T1
        self.T1 = 0.99                  # T1 threshhold
        self.logOut = None              # fd for logging output file

        # each mapper needs a copy of the canopy centers list
        self.canopyCenters = [] 
        filePath = os.path.join(PROJECT_ROOT, 'canopy_centers.txt')
        fileIn = open( filePath )
        centersJson = fileIn.read()
        fileIn.close()
        self.canopyCenters = json.loads(centersJson)

        # log file for debugging purposes
        logPath = os.path.join(PROJECT_ROOT, 'assign_progress.log')
        if not self.logOut:
            self.logOut = open(logPath,'w')
            self.logOut.write("Inside mrAssignCanopies init \n")


    def configure_options(self):
        super(mrAssignCanopies, self).configure_options()


    # 
    # assign all corpus documents to a canopy cluster; all documents
    # are fed into mapper via stdin
    # 
    def mapper(self, key, line):

        document = json.loads(line)

        # find all canopy clusters this document may fall into
        assigned_canopies = [];
        for cidx in range(len(self.canopyCenters)):
            clusterCenterPoint  = self.canopyCenters[cidx]
            if jaccard_dist(document, clusterCenterPoint) < self.T1:
                assigned_canopies.append( cidx )   

        docId = document[0]
        yield 1, [docId, assigned_canopies]


    #
    # combine all emitted assigned documents into clusters into a final set of canopy clusters
    #
    def reducer(self, n, document_assignments):

        self.logOut.write("\nIn reducer...\n")
        finalCanopyClusters = {}

        for assignment in document_assignments:
            docId = assignment[0]
            canopyList = assignment[1]
            for canopyId in canopyList:
                if finalCanopyClusters.has_key(canopyId):
                    finalCanopyClusters[canopyId].append(docId)
                else:
                    doc_list = []
                    doc_list.append(docId)
                    finalCanopyClusters[canopyId] = doc_list

        # some debugging info on our clusters
        self.logOut.write("\nAfter assignment, "+repr(len(finalCanopyClusters))+" canopy clusters\n\n")
        N = sum = sumsq = 0.0
        for cidx in range(len(finalCanopyClusters)):
            cluster_len = len(finalCanopyClusters[cidx])
            N += 1
            sum += cluster_len
            sumsq += (cluster_len * cluster_len)
        mean = sum/N
        sd = sqrt(sumsq/N - mean*mean)
        self.logOut.write("Avg docs per cluster: "+repr(mean)+"; std dev: "+repr(sd)+"\n\n");

        # now we have the final set of canopy clusters; output them to file
        canopiesOut = json.dumps(finalCanopyClusters)
        fullPath = os.path.join(PROJECT_ROOT, 'canopy_clusters.txt')
        fileOut = open(fullPath,'w')
        fileOut.write(canopiesOut)
        fileOut.close()

        if self.logOut:
            self.logOut.close()


if __name__ == '__main__':
    mrAssignCanopies.run()
