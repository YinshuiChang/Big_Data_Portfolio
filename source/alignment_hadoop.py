# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
from json import loads
from functools import reduce
import numpy as np

class Align_Hadoop(MRJob):
    # Include additional files needed by the job
    FILES = ['seq_utils.py']
    
    def mapper_read(self, _, line):
        """
        Reads each line of input and parses it as JSON.
        """
        yield _, loads(line)
        
    def reducer_catesian(self, _, jsons):
        """
        Separates the sequences (with barcodes) from the cDNAs and yields all possible pairs.
        """
        sequences = []
        cdnas = []
        for j in jsons:
            if 'barcode' in j:
                sequences.append(j)
            else:
                cdnas.append(j)
        # Yield all possible pairs of sequences and cDNAs
        for cdna in cdnas:
            for seq in sequences:
                yield seq, cdna

    def mapper_swa(self, seq, cdna):
        """
        Applies the quality-adjusted Smith-Waterman algorithm to each sequence and cDNA pair.
        """
        from seq_utils import quality_adjusted_smith_waterman, substitution_matrix, gap_penalty
        yield seq['barcode'], (cdna['transcript_id'], *quality_adjusted_smith_waterman(seq['sequence'], seq['quality'], cdna['sequence'], substitution_matrix, gap_penalty))

    def mergeValue(self, c,v):
        """
        Merges the current best alignments with a new alignment.
        """
        if not c: # If no current best alignment, use the new value
            return [v]
        if c[0][1] > v[1]: # If the current best score is higher, keep the current best
            return c
        elif c[0][1] == v[1]: # If the scores are equal, append the new alignment
            c.append(v)
            return c
        else: # If the new score is higher, use the new value
            return [v]

    def reducer_alignments(self, barcode, alignments):
        """
        Reduces the alignments to keep only the best ones for each barcode.
        """
        for alignment in reduce(self.mergeValue, alignments, []):
            yield alignment[0], barcode

    def reducer_featurecount(self, feature, barcodes):
        """
        Counts the number of unique barcodes associated with each feature.
        """
        yield feature, len(list(barcodes))
    
    def steps(self):
        """
        Defines the steps of the MapReduce job.
        """
        return [
            MRStep(mapper=self.mapper_read,
                   reducer=self.reducer_catesian),
            MRStep(mapper=self.mapper_swa,
                   reducer=self.reducer_alignments),
            MRStep(reducer=self.reducer_featurecount)
        ]



if __name__ == '__main__':
    # Run the MRJob
    Align_Hadoop.run()