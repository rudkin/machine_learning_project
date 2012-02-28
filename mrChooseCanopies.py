import os
from mrjob.job import MRJob
import nltk
import simplejson as json

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))


#
# calculate the jaccard distance between two document word vectors
#
def jaccard_dist(doc1, doc2):
    # input doc format: [docID, [wordId1, wordId2, etc]]
    s1 = set(doc1[1])
    s2 = set(doc2[1])
    return nltk.metrics.distance.jaccard_distance(s1, s2)


class mrChooseCanopies(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(mrChooseCanopies, self).__init__(*args, **kwargs)
        self.canopyCenters = []         # intermediate canopy center points as we find them
        self.T1 = 1.00                  # T1 threshhold
        self.T2 = 0.94                  # T2 threshhold, where T2 < T1

        self.numMappers = 1             # number of mappers
        self.mapperPasses = 0           # number of passes through mapper

        logPath = os.path.join(PROJECT_ROOT, 'choose_progress.log')
        self.logOut = open(logPath,'w')
        self.logOut.write("Inside mrChooseCanopies init \n")
   
 
    def configure_options(self):
        super(mrChooseCanopies, self).configure_options()

 
    #
    # find a set of canopy centers from the input data
    #
    def mapper(self, key, line):

        document = json.loads(line)

        if self.mapperPasses == 0:
            # initialize this mapper's copy of self.canopyCenters to zero
            self.mapperPasses = 1
            self.canopyCenters = []

        emitPoint = True
        if self.canopyCenters:
            for centerPoint in self.canopyCenters:
                dist = jaccard_dist(document, centerPoint)
                if jaccard_dist(document, centerPoint) < self.T2:
                    emitPoint = False

        if emitPoint:
            self.canopyCenters.append(document)
            yield 1,document


    #
    # combine all emitted center points into a final set of center points
    #
    def reducer(self, n, centers):

        self.logOut.write("\nIn reducer...\n")
        finalCanopyCenters = []

        for document in centers:
            addPoint = True
            if finalCanopyCenters:
                # make sure this center point is not too close to existing center point
                for finalCenterPoint in finalCanopyCenters:
                    if jaccard_dist(document, finalCenterPoint) < 0.5*self.T2:
                        addPoint = False

            if addPoint:
                finalCanopyCenters.append( document )

        self.logOut.write("\nAfter finalizing, "+repr(len(finalCanopyCenters))+" canopies in cluster\n\n")

        # now we have the final set of canopy centers; output them to file
        canopiesOut = json.dumps(finalCanopyCenters)
        fullPath = os.path.join(PROJECT_ROOT, 'canopy_centers.txt')
        fileOut = open(fullPath,'w')
        fileOut.write(canopiesOut)
        fileOut.close()

        self.logOut.close()


if __name__ == '__main__':
    mrChooseCanopies.run()
