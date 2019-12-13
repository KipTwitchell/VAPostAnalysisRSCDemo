#!/usr/bin/env bash

# Really Simple Commerce Demo System Start up Script
# This script creates named pipes, and then starts four other terminal windows to run each of the sessions
# It runs the Server Side Scala RSC program as well, and upon completion, deletes the associated communication files

export CLASSPATH=/Users/ktwitchell001/workspace/cdscVADataDemoSys/target/scala-2.11/classes
echo "Process ID: "$$

pathName="/tmp/"
authReadFile="authRead"
serverReadFile="serverRead"
vendReadFile="vendRead"
custReadFile="custRead"
instFile="QueryDataInst.csv"
transFile="QueryDataTrans.csv"

clientScriptFile="/Users/ktwitchell001/workspace/cdscVADataDemoSys/src/main/scala/RSCstartClient.sh"
serverScalaProg="/Users/ktwitchell001/workspace/cdscVADataDemoSys/src/main/scala/RSCserver.scala"
echo "Communication path: "$pathName" client side write: "$authReadFile" server side write: "$serverReadFile

echo "creating communication files"
rm $pathName$authReadFile
rm $pathName$serverReadFile
rm $pathName$vendReadFile
rm $pathName$custReadFile

rm $pathName$instFile
rm $pathName$transFile
cp "/Users/ktwitchell001/workspace/cdscVADataDemoSys/src/main/scala/"$instFile $pathName$instFile
cp "/Users/ktwitchell001/workspace/cdscVADataDemoSys/src/main/scala/"$transFile $pathName$transFile

mkfifo $pathName$authReadFile
mkfifo $pathName$serverReadFile
mkfifo $pathName$vendReadFile
mkfifo $pathName$custReadFile

echo "chmod against client script file"
chmod +x $clientScriptFile
#chmod +x $serverScriptFile  not needed as it is executed here in this script.  No chmod required.

#echo "Starting terminal windows"
open -a Terminal.app $clientScriptFile  # Authorizer Session
open -a Terminal.app $clientScriptFile  # Vendor Session
open -a Terminal.app $clientScriptFile  # Customer Session

echo -n -e "\033]0;RSC SERVER\007"

echo "Starting server side process"
#scala -J-Xmx1g $serverScalaProg \
scala $serverScalaProg \
     $pathName $authReadFile $serverReadFile $vendReadFile $custReadFile $instFile $transFile

Sleep 10 # let the other notes finish before deleting communication files

echo "Cleaning up communication files"
rm $pathName$authReadFile
rm $pathName$serverReadFile
rm $pathName$vendReadFile
rm $pathName$custReadFile
