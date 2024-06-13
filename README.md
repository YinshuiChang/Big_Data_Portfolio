# Big Data Analysis Portfolio
Author: Yinshui Chang  
Work in Progress
## Project Title: Building a Sequence Alignment Tool Using Big Data Technologies with Minimal Dependencies

### Overview
This project aims to build an efficient sequence alignment tool utilizing advanced big data technologies with minimal dependencies. The project involves creating a comprehensive environment to handle sequence alignment tasks and perform comparative analyses using Hadoop MapReduce and Apache Spark. The focus will be on implementing the Smith-Waterman algorithm for local alignment and developing a BLASTN-like program and a sequence alignment program to compare sequence alignment performance on both Hadoop and Spark platforms.

### Datasets

1. **Fasta File: Human cDNAs**
   - **Source**: Ensembl (www.ensembl.org)
   - **File**: Homo_sapiens.GRCh38.cdna.all.fa.gz
   - **Description**: Contains cDNA sequences corresponding to Ensembl genes, excluding ncRNA genes. Includes transcript sequences for actual and possible genes, such as pseudogenes and nonsense-mediated decay (NMD) transcripts.

2. **Fastq File: Single-Cell RNA Sequencing**
   - **Source**: 10x Genomics
   - **Dataset**: 500 Human PBMCs, 3' LT v3.1, Chromium X
   - **Description**: Single-cell RNA sequencing data from human peripheral blood mononuclear cells (PBMCs) of a healthy female donor. Approximately 500 cells were recovered, sequenced on an Illumina NovaSeq 6000 to a read depth of 50k mean reads per cell. The sequencing includes paired-end reads with dual indexing.

### Software and Tools
- **Docker**: To create a consistent and portable development environment.
- **Hadoop**: For distributed storage and MapReduce processing.
- **Apache Spark**: For in-memory data processing and analysis.
- **Jupyter Lab**: For interactive development and visualization.

### Project Steps

1. **Create Docker Environment**
   - Set up a Docker environment containing Hadoop, Spark, and Jupyter Lab.

2. **Read and Upload Data**
   - Read the human cDNA FASTA file and the single-cell RNA sequencing FASTQ file.
   - Upload the datasets to the local Hadoop cluster for distributed processing.

3. **Smith-Waterman Algorithm Implementation**
   - Implement the Smith-Waterman algorithm for local sequence alignment.

4. **BLASTN Program Development**
   - Develop a BLASTN-like program using Hadoop MapReduce and Spark Directed Acyclic Graph (DAG) for a single-stage MapReduce task.
   - The program should perform nucleotide sequence alignment and report significant matches.

5. **Performance Comparison (BLASTN)**
   - Compare the runtime and efficiency of the BLASTN program on Hadoop versus Spark.
   - Analyze and document the performance differences between the two platforms.

6. **Sequence Alignment Program Development**
   - Create a more complex sequence alignment program using Hadoop MapReduce and Spark DAG for multi-stage MapReduce tasks.
   - The program should handle quality scores and perform high-accuracy alignments.

7. **Performance Comparison (Sequence Alignment)**
   - Compare the runtime and efficiency of the sequence alignment program on Hadoop versus Spark.
   - Analyze and document the performance differences, focusing on scalability and resource utilization.

### Expected Outcomes
- A Docker environment with integrated Hadoop, Spark, and Jupyter Lab for sequence data analysis.
- Efficient implementation of the Smith-Waterman algorithm for local sequence alignment.
- Development of a BLASTN-like program and a sequence alignment program using both Hadoop MapReduce and Spark.
- Comparative analysis of the performance and efficiency of sequence alignment tasks on Hadoop and Spark platforms.
- Documentation of findings and recommendations for using Hadoop or Spark for specific sequence alignment and analysis tasks.
