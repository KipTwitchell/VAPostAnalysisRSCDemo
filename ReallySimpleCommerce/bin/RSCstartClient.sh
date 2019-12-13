#!/usr/bin/env bash

# Begins the client side for the Really Simple Commerce System
# This is called by the RSCsetup Script

export CLASSPATH=/Users/ktwitchell001/workspace/cdscVADataDemoSys/target/scala-2.11/classes
echo "Process ID: "$$
clientScalaProg="/Users/ktwitchell001/workspace/cdscVADataDemoSys/src/main/scala/RSCclient.scala"
pathName="/tmp/"
authReadFile="authRead"
serverReadFile="serverRead"
vendReadFile="vendRead"
custReadFile="custRead"
echo -n -e "\033]0;RSC Client Cell Phone\007"

clear

scala -deprecation $clientScalaProg \
     $pathName $authReadFile $serverReadFile $vendReadFile  $custReadFile -deprecation
