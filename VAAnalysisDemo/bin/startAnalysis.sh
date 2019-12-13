#!/usr/bin/env bash

# this sets the Java environment heap size and starts the Instrument Analysis Process


echo "Starting time:  $(date -u)"
#export CLASSPATH=/home/kip.twitchell/cdscVADataDemoSys/target/scala-2.11/classes
export CLASSPATH=/Users/ktwitchell001/workspace/cdscVADataDemoSys/target/scala-2.11/classes
echo "Classpath: "$CLASSPATH
echo "Process ID: "$$
SECONDS=0
echo "Start Seconds: "$SECONDS

inputPath="/Users/ktwitchell001/workspace/VADataDemo/VADataParallelSept8/"
outputPath="/Users/ktwitchell001/workspace/VADataDemo/VADataParallelSept8/"  #never tested difference betwen input and output paths
#inputPath="/home/kip.twitchell/finData2/"
#outputPath="/home/kip.twitchell/finData2/"  #never tested difference betwen input and output paths
maxIDFileName="VATestMaxID.txt"  #contains the max identifiers for instruments, balances, etc.  Read and written by
 # both programs, and passed as input to the next program processing the transaction files.
instIDFileName="VATestInstID.txt" # Keyed by Vendor Name, used to ID new vendors and assign Vendor (Instrument) IDs to them
instTBLFileName="VATestInstTBL.txt" # Key by Instrument ID, the only meaningful attribute is the Vendor Name
balTBLFileName="VATestBalTBL.txt"  # After processing the Rev file, the balance file is renamed to B1xxx at bottom of script
tranTBLFileName="VATestTranTBL.txt" # this file is a temporary file.  The perm file is it the T1xxx file named below
instUPDFileName="VATestInstUPD.txt" # this is a temporary file.  the permanent file is the U1xxx file named below
sumTBLFileName="VATestSumTBL.txt"  # for future use; no use in current instance, except program writes it
aggTBLFileName="VATestAggTBL.txt"  # for future use; no use in current instance, except program writes it
#agencyFileName="aRefAgencyCSV.txt"  #used on 2014 - 16 revenue data to put the agencyTab name as the instrument ID field
#reference table names hardcoded in the program, not passed as parameters.

clear

scala -J-Xmx6g /Users/ktwitchell001/workspace/cdscVADataDemoSys/src/main/scala/instAnalysis.scala \
     $inputPath $outputPath $maxIDFileName \
    $instTBLFileName $balTBLFileName $tranTBLFileName "updtablenotused" $sumTBLFileName $aggTBLFileName
