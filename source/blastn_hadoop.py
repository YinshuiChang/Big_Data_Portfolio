# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
from json import loads, dumps
from functools import reduce
import numpy as np
from mrjob.protocol import JSONValueProtocol


class TupleProtocol(object):
    def write(self, key, value):
        return dumps(value).encode('utf_8')

class Blastn_Hadoop(MRJob):
    # Include additional files needed by the job
    FILES = ['seq_utils.py']
    
    OUTPUT_PROTOCOL = TupleProtocol

    def configure_args(self):
        super(Blastn_Hadoop, self).configure_args()
        self.add_passthru_arg(
            '--query_sequence', type=str, default="AGTACGCATACGGCATA", help='please input a sequence to query')

    def mapper(self, _, line):
        """
        The mapper function processes each line of the input file.
        It loads the sequence data from JSON, applies the Smith-Waterman algorithm,
        and yields the transcript ID along with the alignment score and aligned sequences.
        """
        from seq_utils import smith_waterman, substitution_matrix, gap_penalty
        seq = loads(line) # Parse the JSON line into a dictionary
        yield _, (seq['transcript_id'], *smith_waterman(self.options.query_sequence, seq['sequence'], substitution_matrix, gap_penalty))


    def mergeValue(self, c,v):
        """
        This function merges values for the combiner and reducer.
        It keeps the alignments with the highest scores.
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

    
    def combiner(self, _, blast):
        """
        The combiner function aggregates results locally before sending to the reducer.
        It applies the mergeValue function to keep only the best alignments.
        """
        for i in reduce(self.mergeValue, blast, []):
            yield _, i

    def reducer(self, _, blast):
        """
        The reducer function aggregates results from all mappers and combiners.
        It applies the mergeValue function to keep only the best alignments.
        """
        for i in reduce(self.mergeValue, blast, []):
            yield _,i


if __name__ == '__main__':
    # Run the MRJob
    Blastn_Hadoop.run()