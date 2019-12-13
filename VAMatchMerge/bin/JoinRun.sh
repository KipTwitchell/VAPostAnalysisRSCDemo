#!/usr/bin/env bash
###########################################################################
#
#  Consolidated Data Supply Chain (CDSC) Demo System
#
# Copyright 2017, IBM Corporation.  All rights reserved.
#     Kip Twitchell <finsysvlogger@gmail.com>.
#                   kip.twitchell@us.ibm.com
#  Craeted Jan 2018
#
# Shell script to run the small join test for the stage 1
#
###########################################################################


echo "*********************************************************************"
echo "*********************************************************************"
echo "         START OF JOIN PROCESSING"
echo "*********************************************************************"
echo "*********************************************************************"

echo "Starting time:  $(date -u)"
#export CLASSPATH=C/'Program Files'/Java/jdk1.8.0_131;C/Users/IBM_ADMIN/.m2/repository/org/scala-lang;
#echo "Classpath: "$CLASSPATH
echo "Process ID: "$$
SECONDS=0
echo "Start Seconds: "$SECONDS

#libPath='/C/Users/IBM_ADMIN/.ivy2/cache/org.scala-lang/scala-library/jars'
#libPath='C/Users/IBM_ADMIN/.m2/repository/org/scala-lang/*'
inputPath="/Users/ktwitchell001/workspace/VAPostAnalysisRSCDemo/VAMatchMerge/data/"
outputPath="/Users/ktwitchell001/workspace/VAPostAnalysisRSCDemo/VAMatchMerge/data"  #never tested difference betwen input and output paths
maxIDFileName="VATestMaxID.txt"  #contains the max identifiers for instruments, balances, etc.  Read and written by
 # both programs, and passed as input to the next program processing the transaction files.
instIDFileName="VATestInstID.txt" # Keyed by Vendor Name, used to ID new vendors and assign Vendor (Instrument) IDs to them
instTBLFileName="VATestInstTBL." # Key by Instrument ID, the only meaningful attribute is the Vendor Name
balTBLFileName="VATestBalTBL.txt"  # After processing the Rev file, the balance file is renamed to B1xxx at bottom of script
instUPDFileName="VATestInstUPD.txt" # this is a temporary file.  the permanent file is the U1xxx file named below
sumTBLFileName="VATestSumTBL.txt"  # for future use; no use in current instance, except program writes it
agencyTabFileName="aRefAgencyTab.txt"  #used on revenue data to put the agencyTab name as the instrument ID field
agencyFileName="aRefAgencyCSV.txt"  #used on 2014 - 16 revenue data to put the agencyTab name as the instrument ID field

echo "Input Path: "$inputPath

# file name syntax is the file name less "xp.txt" for expenses and "ev.txt" for revenue files, where the name is
# "FY" + nn for fiscal year + "q" + n for the quarter on expense files, nothing on rev files + "e" for expense, "r" revenue
fileList=$inputPath"T1*"
fileCount=0

#for dirFile in $fileList
#do
#
#
#
#    file=$(basename $dirFile)
#    # Set and manipulate varFables
#
#    # these file names are not passed to the Scala Program
#    # input file to the awk statements
#    if [ ${#file} -ge 12 ] && [ ${file:7:1}="e" ]
#     then
#        filetype="E"
#        inputFile=$file
#        outputTRN="T1"${file:2:5}"xp.txt"
#        outputUPDi="U1"${file:2:5}"xp.txt"
#        outputRAW="Raw"${file:2:5}"xp.txt"
#        fiscalMM="0"${file:5:1} # assumed first month of quarter
#
#     else
#        if [ ${file:5:1}="r" ]
#            then
#                filetype="R"
#                inputFile=$file
#                outputTRN="T1"${file:2:3}"ev.txt"
#                outputUPDi="U1"${file:2:3}"ev.txt"
#                outputRAW="Raw"${file:2:3}"ev.txt"
#                outputBAL="B1"${file:2:2}"bal.txt"   # rename of file only occcurs on Revenue Files
#                fiscalMM="01" # assumed first month of year
#            else
#                filetype=" "
#                echo "ERROR IN FILETYPE; NEITHER EXPENSE OR REVENUE"
#                exit
#         fi
#    fi
#    fiscalCC="20"
#    fiscalYY=${file:2:2}
#    slash="/"
#    fiscalPeriod=$fiscalCC$fiscalYY$slash$fiscalMM
#    acctgPeriod=$fiscalCC$fiscalYY$slash$fiscalMM$slash'01'
#    echo "Fiscal Date: "$fiscalPeriod
#    echo "Accounting Date: "$acctgPeriod

    thrd="txtInstID9.txt"
    year1="03"
    year2="04"
    year3="05"
    year4="06"
    year5="07"
    year6="08"
    year7="09"
    year8="10"
    year9="11"
    yearA="12"
    yearB="13"
    yearC="14"
    yearD="15"
    yearE="16"
    tranTBLFileName="C1Data."
    year="All Years"


#    echo "---------------------------------------------------------------------"
#    echo "---------------------------------------------------------------------"
#    echo "         BEGINNING OF NON-THREAD PROCESSING FOR FILE YEAR: " + $year" THREAD: "$thrd
##    echo "File Type: "$filetype" Input File: "$inputFile" Output File: "$outputTRN
#    echo "---------------------------------------------------------------------"
#    echo "---------------------------------------------------------------------"
#
##      sort -k 1 -m   \
#      sort -k 1    \
#       $inputPath"T1"$year1"q1exp."$thrd  \
#       $inputPath"T1"$year1"q2exp."$thrd  \
#       $inputPath"T1"$year1"q3exp."$thrd  \
#       $inputPath"T1"$year1"q4exp."$thrd  \
#       $inputPath"T1"$year1"rev."$thrd  \
#       $inputPath"T1"$year2"q1exp."$thrd  \
#       $inputPath"T1"$year2"q2exp."$thrd  \
#       $inputPath"T1"$year2"q3exp."$thrd  \
#       $inputPath"T1"$year2"q4exp."$thrd  \
#       $inputPath"T1"$year2"rev."$thrd  \
#       $inputPath"T1"$year3"q1exp."$thrd  \
#       $inputPath"T1"$year3"q2exp."$thrd  \
#       $inputPath"T1"$year3"q3exp."$thrd  \
#       $inputPath"T1"$year3"q4exp."$thrd  \
#       $inputPath"T1"$year3"rev."$thrd  \
#       $inputPath"T1"$year4"q1exp."$thrd  \
#       $inputPath"T1"$year4"q2exp."$thrd  \
#       $inputPath"T1"$year4"q3exp."$thrd  \
#       $inputPath"T1"$year4"q4exp."$thrd  \
#       $inputPath"T1"$year4"rev."$thrd  \
#       $inputPath"T1"$year5"q1exp."$thrd  \
#       $inputPath"T1"$year5"q2exp."$thrd  \
#       $inputPath"T1"$year5"q3exp."$thrd  \
#       $inputPath"T1"$year5"q4exp."$thrd  \
#       $inputPath"T1"$year5"rev."$thrd  \
#       $inputPath"T1"$year6"q1exp."$thrd  \
#       $inputPath"T1"$year6"q2exp."$thrd  \
#       $inputPath"T1"$year6"q3exp."$thrd  \
#       $inputPath"T1"$year6"q4exp."$thrd  \
#       $inputPath"T1"$year6"rev."$thrd  \
#       $inputPath"T1"$year7"q1exp."$thrd  \
#       $inputPath"T1"$year7"q2exp."$thrd  \
#       $inputPath"T1"$year7"q3exp."$thrd  \
#       $inputPath"T1"$year7"q4exp."$thrd  \
#       $inputPath"T1"$year7"rev."$thrd  \
#       $inputPath"T1"$year8"q1exp."$thrd  \
#       $inputPath"T1"$year8"q2exp."$thrd  \
#       $inputPath"T1"$year8"q3exp."$thrd  \
#       $inputPath"T1"$year8"q4exp."$thrd  \
#       $inputPath"T1"$year8"rev."$thrd  \
#       $inputPath"T1"$year9"q1exp."$thrd  \
#       $inputPath"T1"$year9"q2exp."$thrd  \
#       $inputPath"T1"$year9"q3exp."$thrd  \
#       $inputPath"T1"$year9"q4exp."$thrd  \
#       $inputPath"T1"$year9"rev."$thrd  \
#       $inputPath"T1"$yearA"q1exp."$thrd  \
#       $inputPath"T1"$yearA"q2exp."$thrd  \
#       $inputPath"T1"$yearA"q3exp."$thrd  \
#       $inputPath"T1"$yearA"q4exp."$thrd  \
#       $inputPath"T1"$yearA"rev."$thrd  \
#       $inputPath"T1"$yearB"q1exp."$thrd  \
#       $inputPath"T1"$yearB"q2exp."$thrd  \
#       $inputPath"T1"$yearB"q3exp."$thrd  \
#       $inputPath"T1"$yearB"q4exp."$thrd  \
#       $inputPath"T1"$yearB"rev."$thrd  \
#       $inputPath"T1"$yearC"q1exp."$thrd  \
#       $inputPath"T1"$yearC"q2exp."$thrd  \
#       $inputPath"T1"$yearC"q3exp."$thrd  \
#       $inputPath"T1"$yearC"q4exp."$thrd  \
#       $inputPath"T1"$yearC"rev."$thrd  \
#       $inputPath"T1"$yearD"q1exp."$thrd  \
#       $inputPath"T1"$yearD"q2exp."$thrd  \
#       $inputPath"T1"$yearD"q3exp."$thrd  \
#       $inputPath"T1"$yearD"q4exp."$thrd  \
#       $inputPath"T1"$yearD"rev."$thrd  \
#       $inputPath"T1"$yearE"q1exp."$thrd  \
#       $inputPath"T1"$yearE"q2exp."$thrd  \
#       $inputPath"T1"$yearE"q3exp."$thrd  \
#       $inputPath"T1"$yearE"q4exp."$thrd  \
#       $inputPath"T1"$yearE"rev."$thrd > \
#        $outputPath$tranTBLFileName$thrd
#
#    echo "Phase Sort End Seconds: "$SECONDS" YEAR: " + $year" THREAD: "$thrd



    echo "Phase 0 End Seconds: "$SECONDS" YEAR: " + $year" THREAD: "$thrd
    echo "---------------------------------------------------------------------"
    echo "---------------------------------------------------------------------"
    echo "         BEGINNING OF THREAD PROCESSING FOR FILE YEAR: " + $year" THREAD: "$thrd
    echo "---------------------------------------------------------------------"
    echo "---------------------------------------------------------------------"



    thrdprocess () {

        echo "Process ID: "$$" Thread: "$thrd
        thisPID=$$
        SECONDS=0
        echo "Start Seconds: "$SECONDS" Thread: "$thrd
        echo "Starting the java run #####################"
#        java -cp $libPath/*;target/VAMatchMerge-0.1-SNAPSHOT-jar-with-dependencies.jar
#         java -jar '/ngsafr/VAMatchMerge/target/VAMatchMerge-0.1-SNAPSHOT-jar-with-dependencies.jar'
         java -jar '/Users/ktwitchell001/workspace/VAPostAnalysisRSCDemo/VAMatchMerge/target/scala-2.12/vamatchmerge_2.12-0.1.0-SNAPSHOT.jar'
        echo "Finishing the java run #####################"
#        scala /Users/ktwitchell001/workspace/cdscVADataDemoSys/src/main/scala/JoinTest.scala \
#             $inputPath $outputPath $maxIDFileName$thrd \
#            $instTBLFileName$thrd $balTBLFileName$thrd $tranTBLFileName$thrd "dummy"$thrd $sumTBLFileName$thrd

        echo "End Seconds: "$SECONDS$thrd" Thread: "$thrd
        sample=1
#        \top "-l " $sample " > "$outputPath"endtimethrd"$thrd"PID"$thisPID".txt"
        \ps -ef > $outputPath"endtimethrd"$thrd"PID"$thisPID".txt"

    }
#
    (thrd="txtInstID0.txt"; thrdprocess) #&
#    (thrd="txtInstID1.txt"; thrdprocess) &
#    (thrd="txtInstID2.txt"; thrdprocess) &
#    (thrd="txtInstID3.txt"; thrdprocess) &
#    (thrd="txtInstID4.txt"; thrdprocess) &
#    (thrd="txtInstID5.txt"; thrdprocess) &
#    (thrd="txtInstID6.txt"; thrdprocess) &
#    (thrd="txtInstID7.txt"; thrdprocess) &
#    (thrd="txtInstID8.txt"; thrdprocess) &
#    (thrd="txtInstID9.txt"; thrdprocess) &
#    wait
#
#
#    fileCount=$[fileCount+1]
#    echo "Input File count processed: "$fileCount" "$inputFile
#    echo "File: "$inputFile" end process time:  $(date -u)"
#
#
#
#    ###########################################################################
#done  # end of loop
#    ###########################################################################

    echo "Ending time:  $(date -u)"

echo "*********************************************************************"
echo "*********************************************************************"
echo "         END OF PROCESSING EXPENSE AND REVENUE FILES"
echo "*********************************************************************"
echo "*********************************************************************"

#!/usr/bin/env bash
