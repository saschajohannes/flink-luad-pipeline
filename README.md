# Flink - LUAD pipeline

This project conatains a scala/ Apache Flink based pipline for prediction of Lung Adenocarcinoma tumour (LUAD). The pipeline is trained using HiSeq RNASeq and HiSeq miRNASeq data which were retrieved from The Cancer Genome atlas. The pipeline uses a SVM based approach for the prediction, and a Alternating Least Square matrix completion and co-expression based filtering approach as preprocessing step.

# Content
 * _input/input.txt_ - the used definition file wh
 * _input/miRNASeq_ and _input/RNASeq_ - contains the data files from The Cancer Genome Atlas
 * _pipeline/_ - contains a IntelliJ IDEA Scala project
 * Report.pdf - Contains backgroud information of the pipeline

## Run it

The pipeline is run using a single file argument (called definition file). This file contains all required definitions, the file has a table like layout with three columns (tab separated). The general structure is shown below:

```
#define a sample name
def	sample	<sample-name>

#define a sample to predict
def	predictive	<sample-name>

#define sample as tumorous, default: non-tumorous
diagnosis	<sample-name>	TN

#define a sample type
def sample-type <sample-type>

#add a file for a sample-type to a sample
<sample-type>	<sample-name>	<file-path>

#define output
def	output	<output-file>

#define threshold for correlation
def pc-threshold <threshold|none>
```
### Output 
The resulting prediction is either printed to the console (if no output file is defined in the input file) or written to the specidifed file. The File contains the defined sample ids which should be prediced and either a 1.0 (indicating a tumorous prediction) or a -1.0 (indicating a non-umorous prediction)


### Example
A small example with four samples for training and one example for prediction:
```
def sample  samp1
def sample  samp2
def sample  samp3
def sample  samp4

diagnosis   samp1   TN
diagnosis   samp2   TN

def predictive samp5

def sample-type miRNA
def sample-type methylation

miRNA   samp1   miRNAFile1.txt
miRNA   samp2   miRNAFile2.txt
miRNA   samp3   miRNAFile3.txt
miRNA   samp4   miRNAFile4.txt
miRNA   samp5   miRNAFile5.txt

methylation   samp1   methylationFile1.txt
methylation   samp2   methylationFile2.txt
methylation   samp3   methylationFile3.txt
methylation   samp4   methylationFile4.txt
methylation   samp5   methylationFile5.txt

#disable correlation based filtering
def pc-threshold    none
```

A possible result:
```
samp5   -1.0
```