import subprocess
import argparse
from subprocess import check_output, CalledProcessError

parser = argparse.ArgumentParser()
parser.add_argument("--app_name", type=str, default="NWeight", help="app name")
parser.add_argument("--exp_type", type=str, default="Spark", help="system to run")

args = parser.parse_args()

name = args.app_name
exp_type = args.exp_type


if 'PR' in name:
    class_name = 'com.ibm.crail.benchmarks.Main'
    class_args='-t pagerank -gi 10 -i /pr-input-small' 
if 'SVDPP' in name:
    class_name = 'org.apache.spark.examples.graphx.SVDPlusPlusExample'
    class_args = '/svdpp-input'
elif 'NWeight' in name:
    class_name = 'com.intel.hibench.sparkbench.graph.nweight.NWeight'
    class_args = '/HiBench/NWeight/Input /HiBench/NWeight/Output 3 30 8 3 false graphx'
elif 'Bayes' in name:
    class_name = 'org.apache.spark.examples.mllib.SparseNaiveBayes'
    class_args = '/HiBench/Bayes/Input'
elif 'KMeans' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.DenseKMeans'
    class_args = '-k 10 --numIterations 5 --storageLevel MEMORY_ONLY --initMode Random /HiBench/KMeans/Input/samples'
elif 'SVM' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.SVMWithSGDExample'
    class_args = '--numIterations 100 --storageLevel MEMORY_ONLY --stepSize 1.0 --regParam 0.01 /HiBench/SVM/Input'
elif 'LR' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.LogisticRegression'
    class_args = '/HiBench/LR/Input' 
elif 'GBT' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.GradientBoostedTree'
    class_args = '--numClasses 2 --maxDepth 30 --maxBins 32 --numIterations 20 --learningRate 0.1 /HiBench/GBT/Input'
elif 'GMM' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.GaussianMixtureModel'
    class_args = '-k 10 --numIterations 5 --storageLevel MEMORY_ONLY /HiBench/GMM/Input/samples'
elif 'LDA' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.LDAExample'
    class_args = '--numTopics 20 --maxIterations 10 --optimizer online --maxResultSize 1g /HiBench/LDA/Input /HiBench/LDA/Output'
elif 'PCA' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.PCAExample'
    class_args = '--k 10 --maxResultSize 1g /HiBench/PCA/Input'
elif 'RF' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.RandomForestClassification'
    class_args = '--numTrees 100 --numClasses 2 --featureSubsetStrategy auto --impurity gini --maxDepth 4 --maxBins 32 /HiBench/RF/Input'
elif 'XGBOOST' in name:
    class_name = 'com.intel.hibench.sparkbench.ml.XGBoost'
    class_args = '--numClasses 2 --maxDepth 30 --maxBins 32 --numIterations 20 --learningRate 0.1 /HiBench/XGBoost/Input'


if 'PR' in name:
    jar = '/home/wonook/sql-benchmarks/target/sql-benchmarks-1.0.jar'
elif 'CC' or 'SVDPP' in name:
    jar = '/home/wonook/blaze/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar'
else:
    jar = '/home/wonook/HiBench/sparkbench/assembly/target/sparkbench-assembly-8.0-SNAPSHOT-dist.jar'


print(class_args)

num_executors = 2
executor_mem = '30g'
dag_path = "None"
promote = 0.0
profile_timeout = 60
disk = '0g'

#hdfs dfs -rm -R /HiBench/$TEST_NAME/Output


script = f'/home/wonook/blaze/run_atc21.sh {name} {exp_type} {executor_mem} {num_executors} {disk} {dag_path} {class_name} {jar} {promote} {profile_timeout} {class_args}'

print(script)

proc = subprocess.run(script, shell=True, check=True)
try:
    outs, errs = proc.communicate(timeout=10)
except CalledProcessError:
    proc.kill()
    outs, errs = proc.communicate()
