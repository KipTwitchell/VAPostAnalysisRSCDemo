Consolidated Data Supply Chain Demo System using data from the Commonwealth of Virginia

This system has three major parts to
1. A posting engine process
2. A simple analysis engine
3. A simple data capture engine
4. A simple join program, called Match-Merge

The code in this repository has not been built while in this repository structure, and likely needs to be updated for latest libraries in order to run.

A more detailed description of the system can be found at https://ledgerlearning.com/2019/12/13/simple-financial-system-proof-of-concept/  

Each is described below.

# Posting Engine**

The posting engine has multiple parts to it.  
1. The data files come from DataPoint from VA, the expenses and revenue data from 2003 to 2016 (excluding 2016 revenue, which was not posted correctly).
2. The files have multiple file formats, so the first stage of the cdscScript.sh uses awk to detect what kind of file, what year, and to transform it into a consistent readable format
3. The file is then sorted using linux sort to place transactions in order by vendor name
4. The file is then read by the instIDAssign scala program, which 
(a) also reads the vendor ID master file, sorted by vendor name, and having the assigned vendor ID next to it 
(b)detects if a new vendor has been detected, and assigns an ID to this vendor (it also updates a small file of vendor numbers to keep track of assigned IDs).  These updates are written to Instrument Update file.  
(c) turns the raw transactions into universal journals. 
(d) Write out an updated Vendor Master file. 
5. The Instrument Updates and Universal Journal files are sorted
6. The instPosting process reads the Vendor Master (sorted by vendor number), universal journals, instrument updates, and balances files.  It then updates the vendor master, and posts transactions to the balance files.

The process is started by using the cdscScript.sh, which after step 1 above automatically runs sub processes in parallel for the remaining steps, processing all files with a particular file name in the input directory.

# Analysis Engine

The analysis engine is an interactive program using simple terminal interface, asking at first how many years of balances should be loaded into memory.  The program also can load high value instruments as it loads the data, and instrument attributes for use in analysis process.

After loading the data, the users is presented with various types of cuts of the data that might be delivered.  They use either the summary data accumulated upon loading, the high value instrument data, or the detailed data.  Option a.2 presents data which joins from the detailed data to the instruments to show instrument types.

The process is started by running the bash script startAanalysis.sh.

# Really Simple Commerce System

The really simple commerce system (RSC) is started by using the RSCstartServer.sh which automatically kicks off three copies of the RSCstartClient.sh.  (also note the server script copies to csv files into the tmp directory, to load exisiting customer accounts and balances into the server memory.)

The three terminal screens can be used to present an interactive simulated text messaging system between the server and three other parties:
1. The Authorizer can add a new Customer (2) to the the RSC network.
2. The Customer can accept invitations to join the network, can add details to their customer records, and can pay for goods and services when invited by the (3) vendor
3. The Vendor specifies amounts for the customer to pay, and accepts payment.

The system does not use cell phone values, and there are no edits on the input data.  The state of each cell phone must be waiting for response from the server before the server sends that response or the system hangs.  The system uses named pipes to communicate to each other in their respective roles.  No two terminal can share the same role at the same time.  The data in this part of the system is not connected to the VA data used in the other two parts of the system.

These are the files used in the system:

The data for the system can be downloaded from these pages:

https://data.world/finsysvlogger/state-of-va-financial-data-readme
https://data.world/finsysvlogger/state-of-va-financial-data-part-1
https://data.world/finsysvlogger/state-of-virginia-financial-data-set-2 

The instPosting.scala program has record structures at the top, the Transaction, Instrument, and Balance classes.

File names are as follows:

aRef files are the descriptive information for the Agency, Fund, and other attributes.  There aren't any classes in the Posting program; these are only used in the Analysis Program (not sure there were classes there)



B1nnBAL.txt are the balances.  nn = the year 03.  They contain this data, and the columns are described by the BalCLS in the posting program.



the FYnnQxEXP and REV files are the original input files from VA.  nn = year, x = quarter.



the newVAT.... files are temporary processing files, left over from the last execution of the process.  Not useful for anything.

The RAWnnQxEXP files are the output from the ETL process, so not yet universal journals.  Probably not to useful for much.

the TnnQxEXP are the Universal Journals.  These can be used for our Spark work, as they have valid Inst. IDs assigned.  They are defined by the TransCLS structures in the posting program.




the UnnQxEXP files are the Updates to the Instrument (vendor) records.  They show when a new Vendor is detected.  Probably not very useful for much.



the VATestInstID files are the Vendor ID assignment files.  They have the Vendor Name, followed by the Vendor assigned ID.  Probably not real useful for our work.



the VATestInstTBL files contain the Vendor Information.  They have the Vendor ID, followed by name, and are very useful.  They are described by the InstCLS structure in the posting program.



the VATestMaxID file contains the maximum ID assigned to the journals, the balances, and to the Insturments.  They are probably not useful for a lot of things.  
  
  # Match Merge
  The Match Merge program simply joins together two of the outputs from the Posting Program, showing what might be the most efficient join process possible in terms of total compute resources used.
  
# Legal
  
  Each source file must include a license header for the Apache Software License 2.0. Using the SPDX format is the simplest approach. e.g.
  
` /*`
`  Copyright <holder> All Rights Reserved.`
` ` 
`  SPDX-License-Identifier: Apache-2.0
`
`  */`
  
  We have tried to make it as easy as possible to make contributions. This applies to how we handle the legal aspects of contribution. We use the same approach - the Developer's Certificate of Origin 1.1 (DCO) - that the Linux® Kernel community uses to manage code contributions.
  
  We simply ask that when submitting a patch for review, the developer must include a sign-off statement in the commit message.
  
  Here is an example Signed-off-by line, which indicates that the submitter accepts the DCO:
  
  Signed-off-by: John Doe <john.doe@example.com>
  You can include this automatically when you commit a change to your local git repository using the following command:
  
  `git commit -s`