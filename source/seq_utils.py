import re
import numpy as np
from functools import wraps
from time import time


def parse_fasta_header(header):
    """
    Parses the header line of a FASTA file and returns a dictionary of attributes.
    """
    
    # Use regex to split the header into parts. Each part is either a key-value pair or a standalone attribute.
    # https://regex101.com/r/lUySu2/1
    parts = re.findall( r'\S+:[^:]+\[.*\][^:]*$|\S+:[^:]+$|\S+:.+?(?=\S+:)|\S+',header)
    attributes = {}

    # The two parts are the transcript ID and seqtype
    attributes['transcript_id'] = parts[0][1:]  # Remove the leading '>'
    attributes['seqtype'] = parts[1]
    
    # Parse the remaining attributes
    for part in parts[2:]:
        try:
            key, value = part.split(':', 1)
            attributes[key] = value
        except Exception as e:
            print("Error in " + header, e)
    
    return attributes
    
def read_fasta(filepath):
    """
    Reads a FASTA file and returns a dictionary of sequences with their attributes.
    """
    with open(filepath, 'r') as file:
        sequences = []
        seq_id = ""
        seq = ""
        for line in file:
            if line.startswith(">"):
                if seq_id:
                    temp = parse_fasta_header(seq_id)
                    sequences.append(
                        {"transcript_id":temp['transcript_id'],
                        "attributes": temp,
                        "sequence": seq
                        })
                seq_id = line.strip()
                seq = ""
            else:
                seq += line.strip()
        if seq_id:
            temp = parse_fasta_header(seq_id)
            sequences.append(
                {"transcript_id":temp['transcript_id'],
                "attributes": temp,
                "sequence": seq
                })
    return sequences


def parse_fastq_header(header):
    """
    Parses the header line of a FASTQ file and returns a dictionary of attributes.
    """
    # Split the header into parts by colons, spaces, and plus signs
    parts = re.split(r'[: +]', header)
    # Define the attribute names expected in the FASTQ header
    names = ['instrument', 'run_number', 'flowcell_ID', 'lane', 'tile', 'x_pos', 'y_pos', 'read', 'is_filtered', 'control number', 'i7_index', 'i5_index']
    attributes = {name:part for name,part in zip(names, parts)}
    attributes['instrument'] = attributes['instrument'][1:] # Remove the leading '@'
    return attributes


def read_fastq(filepath):
    """
    Reads a FASTQ file and returns a list of dictionaries with sequence information.
    Each dictionary contains 'seq_id', 'attributes', 'sequence', and 'quality' keys.
    """
    sequences = []
    with open(filepath, 'r') as file:
        while True:
            seq_id = file.readline().strip()
            if not seq_id:
                break  # End of file
            sequence = file.readline().strip()
            file.readline()  # Plus sign line (ignored)
            quality = file.readline().strip()
            temp = parse_fastq_header(seq_id)
            sequences.append({
                "seq_id": temp["tile"] + ':' + temp["x_pos"] + ':' + temp["y_pos"],
                "attributes": temp,
                "sequence": sequence,
                "quality": quality
            })
    return sequences

def read_fastq_barcode(sequences_path, barcodes_path):
    """
    Reads a FASTQ file and the corresponding barcode FASTQ file.
    Returns a list of dictionaries with sequence information.
    Each dictionary contains 'barcode', 'attributes', 'sequence', 'quality' and 'bc_quality' keys.
    """
    sequences = []
    with open(sequences_path, 'r') as seq_file, open(barcodes_path, 'r') as code_file:
        while True:
            seq_id = seq_file.readline().strip()
            code_id = code_file.readline().strip()
            if not seq_id:
                break  # End of file
            sequence = seq_file.readline().strip()
            barcode = code_file.readline().strip()
            seq_file.readline()  # Plus sign line (ignored)
            code_file.readline()  # Plus sign line (ignored)
            quality = seq_file.readline().strip()
            barcode_quality = code_file.readline().strip()
            temp = parse_fastq_header(seq_id)
            sequences.append({
                "barcode": barcode,
                "attributes": temp,
                "sequence": sequence,
                "quality": quality,
                "bc_quality": barcode_quality
            })
    return sequences


# Define the substitution matrix and gap penalty
substitution_matrix = {
    ('A', 'A'): 2, ('A', 'C'): -1, ('A', 'G'): -1, ('A', 'T'): -1, ('A', 'N'): 0,
    ('C', 'A'): -1, ('C', 'C'): 2, ('C', 'G'): -1, ('C', 'T'): -1, ('C', 'N'): 0,
    ('G', 'A'): -1, ('G', 'C'): -1, ('G', 'G'): 2, ('G', 'T'): -1, ('G', 'N'): 0,
    ('T', 'A'): -1, ('T', 'C'): -1, ('T', 'G'): -1, ('T', 'T'): 2, ('T', 'N'): 0,
    ('N', 'A'): 0, ('N', 'C'): 0, ('N', 'G'): 0, ('N', 'T'): 0, ('N', 'N'): 0,
}
gap_penalty = -2

def smith_waterman(seq1, seq2, substitution_matrix, gap_penalty):
    """
    Smith-Waterman algorithm for local sequence alignment.
    """
    m, n = len(seq1), len(seq2)
    score_matrix = np.zeros((m + 1, n + 1))
    traceback_matrix = np.zeros((m + 1, n + 1), dtype=int)

    max_score = 0
    max_pos = (0, 0)

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            match = score_matrix[i - 1][j - 1] + substitution_matrix[(seq1[i - 1], seq2[j - 1])]
            delete = score_matrix[i - 1][j] + gap_penalty
            insert = score_matrix[i][j - 1] + gap_penalty
            score = max(0, match, delete, insert)
            score_matrix[i][j] = score

            if score == match:
                traceback_matrix[i][j] = 1  # diagonal
            elif score == delete:
                traceback_matrix[i][j] = 2  # up
            elif score == insert:
                traceback_matrix[i][j] = 3  # left

            if score > max_score:
                max_score = score
                max_pos = (i, j)

    align1, align2 = "", ""
    i, j = max_pos

    while score_matrix[i][j] != 0:
        if traceback_matrix[i][j] == 1:
            align1 = seq1[i - 1] + align1
            align2 = seq2[j - 1] + align2
            i -= 1
            j -= 1
        elif traceback_matrix[i][j] == 2:
            align1 = seq1[i - 1] + align1
            align2 = "-" + align2
            i -= 1
        elif traceback_matrix[i][j] == 3:
            align1 = "-" + align1
            align2 = seq2[j - 1] + align2
            j -= 1

    return max_score, align1, align2



def quality_adjusted_smith_waterman(seq1, qual1, seq2, substitution_matrix, gap_penalty):
    """
    Smith-Waterman algorithm with quality score adjustment.
    """
    m, n = len(seq1), len(seq2)
    score_matrix = np.zeros((m + 1, n + 1))
    traceback_matrix = np.zeros((m + 1, n + 1), dtype=int)

    max_score = 0
    max_pos = (0, 0)

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            base1 = seq1[i - 1]
            base2 = seq2[j - 1]
            quality1 = ord(qual1[i - 1]) - 33  # Convert ASCII quality score to Phred score

            match_score = substitution_matrix[(base1, base2)] * (quality1 / 40.0)
            match = score_matrix[i - 1][j - 1] + match_score
            delete = score_matrix[i - 1][j] + gap_penalty
            insert = score_matrix[i][j - 1] + gap_penalty
            score = max(0, match, delete, insert)
            score_matrix[i][j] = score

            if score == match:
                traceback_matrix[i][j] = 1  # diagonal
            elif score == delete:
                traceback_matrix[i][j] = 2  # up
            elif score == insert:
                traceback_matrix[i][j] = 3  # left

            if score > max_score:
                max_score = score
                max_pos = (i, j)

    align1, align2 = "", ""
    i, j = max_pos

    while score_matrix[i][j] != 0:
        if traceback_matrix[i][j] == 1:
            align1 = seq1[i - 1] + align1
            align2 = seq2[j - 1] + align2
            i -= 1
            j -= 1
        elif traceback_matrix[i][j] == 2:
            align1 = seq1[i - 1] + align1
            align2 = "-" + align2
            i -= 1
        elif traceback_matrix[i][j] == 3:
            align1 = "-" + align1
            align2 = seq2[j - 1] + align2
            j -= 1

    return max_score, align1, align2
