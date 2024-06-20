
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r took: %2.4f sec' % \
          (f.__name__, te-ts))
        return result
    return wrap

import random

def generate_random_dna_sequence(length, include_n=False, weights=None):
    """
    Generates a random DNA sequence of a given length with optional nucleotide weights.

    Args:
    - length (int): The length of the DNA sequence.
    - include_n (bool): If True, the sequence may contain 'N'.
    - weights (dict): A dictionary with nucleotides as keys and their corresponding weights (int) as values. default: 1

    Returns:
    - str: A random DNA sequence.
    """
    # Define the possible nucleotides
    nucleotides = ['A', 'C', 'G', 'T']
    if include_n:
        nucleotides.append('N')
    
    # Create a list of nucleotides according to their weights
    weighted_nucleotides = []
    for nucleotide in nucleotides:
        weighted_nucleotides.extend([nucleotide] * weights.get(nucleotide,1))
    

    # Generate the random sequence
    sequence = ''.join(random.choice(weighted_nucleotides) for _ in range(length))
    return sequence

## Example usage
# length = 50
# include_n = True
# weights = {'A': 6, 'C': 4, 'G': 4, 'T': 6, 'N': 1}  # Example weights
# random_dna_sequence = generate_random_dna_sequence(length, include_n, weights)
# print(f"Random DNA Sequence (length {length}, include 'N': {include_n}): {random_dna_sequence}")


from json import dumps, loads
def writeToHadoop(client, filepath, cdnas_ds):
    """
    Writes a dataset to Hadoop file system (HDFS).

    Parameters:
    client (hdfs.InsecureClient): The HDFS client.
    filepath (str): The path in HDFS where the file will be written.
    cdnas_ds (list): The dataset to be written, where each element is a dictionary representing a cDNA.

    Returns:
    None
    """
    try:
        # Open the HDFS file for writing
        with client.write(filepath) as writer:
            # Write the first cDNA as JSON string
            writer.write(dumps(cdnas_ds[0]))
            # Write the remaining cDNAs, each on a new line as JSON string
            for cdna in cdnas_ds[1:]:
                writer.write('\n' + dumps(cdna))
    except Exception as e:
        print("Fehler beim Kopieren der Datei in HDFS: ", e)

def deleteTestSet(client, filepath):
    """
    Deletes a file from the Hadoop file system (HDFS).

    Parameters:
    client (hdfs.InsecureClient): The HDFS client.
    filepath (str): The path in HDFS of the file to be deleted.

    Returns:
    None
    """
    # Delete the specified file from HDFS
    client.delete(filepath)


from time import time
from subprocess import run
from seq_utils import smith_waterman, substitution_matrix, gap_penalty
from pyspark.sql import SparkSession

# see blastn_pyspark for more info
def mergeValue(c,v):
    if not c:  # If no current best alignment, use the new value
        return [v]
    if c[0][1] > v[1]:  # If the current best score is higher, keep the current best
        return c
    elif c[0][1] == v[1]:  # If the scores are equal, append the new alignment
        c.append(v)
        return c
    else:  # If the new score is higher, use the new value
        return [v]

def mergeCombiners(a,b):
    if not (a and b):  # If one list is empty, return the other
        return a + b
    if a[0][1] > b[0][1]:  # If the best score in 'a' is higher, keep 'a'
        return a
    elif a[0][1] < b[0][1]:  # If the best score in 'b' is higher, keep 'b'
        return b
    else:  # If the best scores are equal, combine the lists
        return a + b


def blastn_pyspark(query_sequence, filepath):
    """
    Executes the BLASTN algorithm using PySpark.

    Parameters:
    query_sequence (str): The DNA sequence to be used as the query for the alignment.
    filepath (str): The HDFS path where the input sequences are stored.

    Returns:
    tuple: Execution time and the alignment output.
    """
    ts = time() # Start timing the operation
    # Initialize a Spark session and Spark context
    with SparkSession.builder.master("local[1]").appName("SparkBlast.com").getOrCreate() as spark:
        sc = spark.sparkContext
        # Read sequences from HDFS and deserialize JSON strings
        sequences_rdd = sc.textFile("hdfs://localhost:9000" + filepath).map(lambda x: loads(x
        # Perform the Smith-Waterman alignment and aggregate results
        out = sequences_rdd.map(lambda seq: (seq['transcript_id'], *smith_waterman(query_sequence, seq['sequence'], substitution_matrix, gap_penalty))) \
            .aggregate([], mergeValue, mergeCombiners)
    te = time() # End timing the operation
    return te-ts, out # Return the execution time and results


def blastn_hadoop(query_sequence, filepath):
    #"""
    #Executes the BLASTN algorithm using Hadoop MapReduce.
    #
    #Parameters:
    #query_sequence (str): The DNA sequence to be used as the query for the alignment.
    #filepath (str): The HDFS path where the input sequences are stored.
    #
    #Returns:
    #tuple: Execution time and the alignment output.
    #"""
    ts = time() # Start timing the operation
    # Run the Hadoop job using the specified script and capture the output
    result = run(["python3", "blastn_hadoop.py", "-r", "hadoop", "--conf-path","mrjob.conf", "--query_sequence", query_sequence, "hdfs://localhost:9000" + filepath], capture_output=True)
    te = time() # End timing the operation
    # Process the output from the Hadoop job and deserialize JSON strings
    return te-ts, list(map(lambda x: tuple(loads(x)), result.stdout.decode('utf-8').split('\t\n')[:-1]))



from operator import add
from seq_utils import quality_adjusted_smith_waterman

# Function to create the initial combiner list
def createCombiner(v):
    return [v]



def align_pyspark(filepath_cdna, filepath_seq):
    """
    Executes the sequence alignment using PySpark.

    Parameters:
    filepath_cdna (str): The HDFS path where the cDNA sequences are stored.
    filepath_seq (str): The HDFS path where the single-cell RNA sequences are stored.

    Returns:
    tuple: Execution time and the alignment output.
    """
    ts = time() # Start timing the operation
    # Initialize a Spark session and Spark context
    with SparkSession.builder.master("local[1]").appName("SparkBlast.com").getOrCreate() as spark:
        sc = spark.sparkContext
        # Read cDNA and sequence data from HDFS and deserialize JSON strings
        cdnas_rdd = sc.textFile("hdfs://localhost:9000" + filepath_cdna).map(lambda x: loads(x))
        sequences_rdd = sc.textFile("hdfs://localhost:9000" + filepath_seq).map(lambda x: loads(x))
        # see alignment spark for more details
        out = cdnas_rdd.cartesian(sequences_rdd) \
            .map(lambda seq: (seq[1]['barcode'], (seq[0]['transcript_id'], *quality_adjusted_smith_waterman(seq[1]['sequence'], seq[1]['quality'], seq[0]['sequence'], substitution_matrix, gap_penalty)))) \
            .combineByKey(createCombiner, mergeValue, mergeCombiners) \
            .flatMap(lambda x: [(y[0],1) for y in x[1]]) \
            .reduceByKey(add) \
            .collect()
    te = time() # End timing the operation
    return te-ts, out # Return the execution time and results


def align_hadoop(filepath_cdna, filepath_seq):
    """
    Executes the sequence alignment using Hadoop MapReduce.

    Parameters:
    filepath_cdna (str): The HDFS path where the cDNA sequences are stored.
    filepath_seq (str): The HDFS path where the single-cell RNA sequences are stored.

    Returns:
    tuple: Execution time and the alignment output.
    """
    ts = time() # Start timing the operation
    # Run the Hadoop job using the specified script and capture the output
    result = run(["python3", "alignment_hadoop.py", "-r", "hadoop", "--conf-path","mrjob.conf", "hdfs://localhost:9000" + filepath_cdna, "hdfs://localhost:9000" + filepath_seq], capture_output=True)
    te = time() # End timing the operation
    # Process the output from the Hadoop job
    out = list(map(lambda x: (x[0][1:-1], int(x[1])), 
                   map(lambda x: x.split('\t'),
                       result.stdout.decode('utf-8').split('\n')[:-1])))
    return te-ts, out # Return the execution time and results