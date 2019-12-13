import java.io.{File, PrintWriter}
import scala.io.Source

//package com.IBM.SAFRPOC
//###########################################################################
//  Consolidated Data Supply Chain (CDSC) Demo System
//
//  Program:  instPosting - Transaction Posting and Reporting
//  This program reads:
//    - A transaction file
//    - Instrument Update Transactions
//    - A Max Key table, containing the highest key assigned to any Instrument
//
//  It writes:
//    - A Balance Table, (The Instrument Ledger)
//    - an updated Instrument Table
//    - any new generated transactions
//    - An updated Max Key Table, with the new highest used Key
//    - Any summaries created off these tables.
//
//	(c) Copyright IBM Corporation. 2017
//  SPDX-License-Identifier: Apache-2.0
//     Kip Twitchell <finsysvlogger@gmail.com>.
//  Created July 2017
//###########################################################################

//###########################################################################
//  to dos:
//  2 - class for inst tbl
//###########################################################################


object instPosting {
  def main(args: Array[String]): Unit = {

    case class stdTransCLSPost(
                                // Element      Data Type                Default Value     KEY col Description
                                transinstID: String,                     // "0",         X  1  Instrument ID
                                transjrnlID: String,                     // "0",            2  Business Event / Journal ID
                                transjrnlLineID: String,                 // "1",            3  Jrnl Line No
                                transjrnlDescript: String,               // " ",            4  Journal / Event Description
                                transledgerID: String,                   // "ACTUALS",   X  5  Ledger
                                transjrnlType: String,                   // "FIN",       X  6  Jrnl Type
                                transbookCodeID: String,            // "SHRD-3RD-PARTY", X  7  Book-code / Basis
                                translegalEntityID: String,              // "STATE-OF-VA"X  8  Legal Entity (CO. / Owner)
                                transcenterID: String,                   // "0",         X  9  Center ID
                                transprojectID: String,                  // "0",         X  10 Project ID
                                transproductID: String,                  // "0",         X  11 Product / Material ID
                                transaccountID: String,                  // "0",         X  12 Nominal Account
                                transcurrencyCodeSourceID: String,       // "USD",       X  13 Curr. Code Source
                                transcurrencyTypeCodeSourceID: String,   // "TXN",       X  14 Currency Type Code Source
                                transcurrencyCodeTargetID: String,       // "USD",       X  15 Curr. Code Target
                                transcurrencyTypeCodeTargetID: String,   // "BASE-LE",   X  16 Currency Type Code Target
                                transtransAmount: String,                // "0",            17 Transaction Amount
                                transfiscalPeriod: String,               // "0",         X  18 Fiscal Period
                                transacctDate: String,                   // "0",               Acctg. Date
                                transtransDate: String,                  // "0",               Transaction Date
                                transdirVsOffsetFlg: String,             // "O",               Direct vs. Offset Flag
                                transreconcileFlg: String,               // "N",               Reconciliable Flag
                                transadjustFlg: String,                  // "N",               Adjustment Flag
                                transmovementFlg: String,                // "N",               Movement Flag
                                transunitOfMeasure: String,              // " ",               Unit of Measure
                                transstatisticAmount: String,            // " ",               Statistical Amount
                                transextensionIDAuditTrail: String,      // " ",               Audit Trial Extension ID
                                transextensionIDSource: String,          // " ",               Source Extension ID
                                transextensionIDClass: String,           // " ",               Classification Extension ID
                                transextensionIDDates: String,           // " ",               Date Extension ID
                                transextensionIDCustom: String)          // " "                Other Customization Ext ID


    case class stdInstCLS(
                           // Element      Data Type                Default Value       Description
                           instinstID: String,                     // "0",               Instrument ID
                           instEffectDate: String,                 // "0000-00-00",      Record Effective Start Date
                           instEffectTime: String,                 // "0",               Record Effective Start Time
                           instEffectEndDate: String,              // "9999-99-99",      Record Effective End Date
                           instEffectEndTime: String,              // "9",               Record Effective End Time
                           instHolderName: String,                 // "Vendor Name",     Name of Instrument Holder
                           instTypeID: String,                     // "EXP",             Vendor (EXP) or REV Record
                           instAuditTrail: String)                 // "0,                Timestamp Updated

    case class stdBalCLS(
                          // Element      Data Type                Default Value       Description
                          balinstID: String,                     // "0",               Instrument ID
                          balbalID: String,                      // "0",               Balance ID
                          balledgerID: String,                   // "ACTUALS",         Ledger
                          baljrnlType: String,                   // "FIN",             Jrnl Type
                          balbookCodeID: String,                 // "SHRD-3RD-PARTY",  Book-code / Basis
                          ballegalEntityID: String,              // "STATE-OF-VA",     Legal Entity (CO. / Owner)
                          balcenterID: String,                   // "0",               Center ID
                          balprojectID: String,                  // "0",               Project ID
                          balproductID: String,                  // "0",               Product / Material ID
                          balaccountID: String,                  // "0",               Nominal Account
                          balcurrencyCodeSourceID: String,       // "USD",             Curr. Code Source
                          balcurrencyTypeCodeSourceID: String,   // "TXN",             Currency Type Code Source
                          balcurrencyCodeTargetID: String,       // "USD",             Curr. Code Target
                          balcurrencyTypeCodeTargetID: String,   // "BASE-LE",         Currency Type Code Target
                          balfiscalYear: String,                 // "0",               Fiscal Year - main bucket key
                          baltransAmount: String,                // "0",               Transaction Amount
                          balmovementFlg: String,                // "N",               Movement Flag
                          balunitOfMeasure: String,              // " ",               Unit of Measure
                          balstatisticAmount: String)            // " ",               Statistical Amount




    println(" ")
    println("********************************")
    println("*** Stage 2 Posting Program! ***")
    println("********************************")
    println(" ")

    //args.foreach(println)
    // ------------------ uncomment below lines when using in PASSED ARGS parameters
        println("Using Passed Parameters")
    val filePaths = Map("inputPath" -> args(0), "outputPath" -> args(1))
    val fileNames = Map("maxIDKeyFileName" -> args(2), "instTBLFileName" -> args(3),
      "balTBLFileName" -> args(4), "tranTBLFileName" -> args(5), "instUPDFileName" -> args(6),
      "sumTBLFileName" -> args(7))
    // ------------------ uncomment above lines when using PASSED ARGS program parameters

    // ------------------ un comment below lines when using IN PROGRAM file parameters
//    var filePaths = Map("inputPath" -> "/Users/ktwitchell001/workspace/VADataDemo/VATestData/", "outputPath" ->
//      "/Users/ktwitchell001/workspace/VADataDemo/VATestData/")
//    var fileNames = Map("maxIDKeyFileName" -> "VATestMaxID.txt",
//      "instTBLFileName" -> "VATestInstTBL.txt",
//      "balTBLFileName" -> "VATestBalTBL.txt",
//      "tranTBLFileName" -> "VATestTranTBL.txt", // this file is a temp file used as input to sort for transactions
//      "instUPDFileName" -> "VATestInstUPD.txt",
//      "sumTBLFileName" -> "VATestSumTBL.txt")
    // ------------------ uncomment above lines when using IN PROGRAM file parameters

    //val debugPrint = "N"

    //*****************************************************************************************
    // Open Files
    //*****************************************************************************************
    // balance file
    val balX = Source.fromFile(filePaths("inputPath") + fileNames("balTBLFileName")).getLines()
    val balTBLLine = balX.buffered

    // transaction file
    // the Pipe Parameter passed to this program is not used:  I couldn't figure out how to put the file attributes in
    // an if statement and have them be referenceable.
    //   transaction file from STREAM
    // ------------------ transaction uncomment below lines to read PIPE, not file
    val tranStreamX = Source.fromInputStream(System.in).getLines() // Pipe version of input
        val transLine = tranStreamX.buffered
        println("Processing transactions from Pipe")
    // ------------------ transaction uncomment above lines to read PIPE, not file

    //  transaction file from FILE
    // ------------------ transaction uncomment below lines to read FILE, not pipe
//    var tranFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + "T103q1exp.txt").getLines()
////    var tranFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + fileNames("tranTBLFileName")).getLines()
//    val transLine: BufferedIterator[String] = tranFileX.buffered
//    println("Module configured for transaction File" + filePaths("inputPath") + fileNames("tranTBLFileName"))
    // ------------------ transaction uncomment above lines to read FILE, not pipe

    // instrument UPD transaction file from FILE
    //    var instUPDFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + "U103q1exp.txt").getLines()
    val instUPDFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + fileNames("instUPDFileName")).getLines()
    val instUPDLine: BufferedIterator[String] = instUPDFileX.buffered
    println("Module configured for inst UPD File")

    // instrument table file from FILE
    //        var instUPDFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + "VATestU13q1exp.txt").getLines()
    val instTBLFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + fileNames("instTBLFileName")).getLines()
    val instTBLLine: BufferedIterator[String] = instTBLFileX.buffered
    println("Module configured for inst TBL File")

    //*****************************************************************************************
    // A Balance Transaction File would be a good idea.  it simply replaces any matching balance key
    // with the record in the balance file based upon a flag (update, replace, delete, etc.)
    //*****************************************************************************************


    //*****************************************************************************************
    // Process MaxKey
    //*****************************************************************************************
    // read MaxKey ID file.  Read all records because file is small and completely rewritten.
    // hold file open with lock until process is finished to contron concurrency.
    // desired header record removed because I couldn't do the right kind of IO
    // record structure in header row is "SysID,InstID,BalID,TranID,TimeStamp,Comment"

    var maxBalID: Int = 0 //counter used to increment to new instrument IDs

    val maxKeySave = Source.fromFile(filePaths("inputPath") + fileNames("maxIDKeyFileName")).getLines.mkString
    val Array(maxKeySysID, maxKeyInstID, maxKeyBalID, maxKeyTranID, maxKeyTimeStamp, maxKeyComment) =
      maxKeySave.split(",").map(_.trim)
    if (maxKeySysID == "1") {
      maxBalID = maxKeyBalID.toInt
      // if (debugPrint == "Y") println(maxBalID)
    }
    val startMaxBalID = maxKeyBalID.toInt

    //*****************************************************************************************
    // Open output files
    //*****************************************************************************************
    // max ID record
    val newMaxIDwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("maxIDKeyFileName")))
    // instrument record
    val newInstTBLwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("instTBLFileName")))
    // tran record  --- for any new transactions created in this process
    val newTranwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("tranTBLFileName")))
    // instrument updates
    val newBalTBLwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("balTBLFileName")))
    // summary view table
    val newSumTBLwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("sumTBLFileName")))

    var balWritten = 0
    var instWritten = 0


    //*****************************************************************************************
    // Perform Initial Reads
    //*****************************************************************************************
    //-----------------------------------------------------------------------------------------
    // Instrument File
    //-----------------------------------------------------------------------------------------

    var instRec = stdInstCLS("","","","","","","","")
    var testInstFullKey: String = " "
    var testInstInstID: String = " "
    var instRead = 0

    def readInst(): Unit = {
      val e: Array[String] = instTBLLine.next.split(',')
      instRec = stdInstCLS(e(0),e(1),e(2),e(3),e(4),e(5),e(6),e(7)) //19 elements
      testInstFullKey = instRec.instinstID
      // the full key actually includes the effective date and time, but these are not on other
      // related records, and so for this program, they are not included.
      // testInstFullKey = instRec.instinstID + instRec.instEffectDate + instRec.instEffectTime

      // Although the below is equal to the above, for symmetry of coding names, it is here as well.
      testInstInstID = instRec.instinstID
      instRead += 1
      // if (debugPrint == "Y") println("inst rec full key: " + testInstFullKey)
    }

    var instEOF = "N"
    def eofInst(): Unit = {
      // if (debugPrint == "Y") println("inst high values. EOF Inst")
      testInstFullKey = "𝖛" * testInstFullKey.length //this is a hack, a higher value constant would be good
      testInstInstID = "𝖛" * testInstInstID.length
      instEOF = "Y"
    }

    if (instTBLLine.isEmpty) eofInst()
    else readInst()

    val instRecTemplate = stdInstCLS("0", "0000-00-00", "0", "9999-99-99", "9", " ", "EXP", "0")

    //-----------------------------------------------------------------------------------------
    // Balance File
    //-----------------------------------------------------------------------------------------

    var balRec = stdBalCLS("","","","","","","","","","","","","","","","0","","","")
    var testBalFullKey: String = " "
    var testBalInstID: String = " "
    var balAmtAccum: Double = 0
    var balRead = 0

    def readBal(): Unit = {
      val e: Array[String] = balTBLLine.next.split(',')
      balRec = stdBalCLS(e(0), e(1),e(2),e(3),e(4),e(5),e(6),e(7),e(8),e(9),e(10),e(11),
        e(12),e(13),e(14),e(15),e(16),e(17),e(18)) //19 elements
      testBalFullKey = balRec.balinstID + balRec.balledgerID + balRec.baljrnlType + balRec.balbookCodeID + balRec.ballegalEntityID +
        balRec.balcenterID + balRec.balprojectID + balRec.balproductID + balRec.balaccountID +
        balRec.balcurrencyCodeSourceID + balRec.balcurrencyTypeCodeSourceID +
        balRec.balcurrencyCodeTargetID + balRec.balcurrencyTypeCodeTargetID + balRec.balfiscalYear
      // if (debugPrint == "Y") println("bal rec full key: " + testBalFullKey)
      testBalInstID = balRec.balinstID
      balAmtAccum = balRec.baltransAmount.toDouble
      balRead += 1
    }

    var balEOF = "N"
    def eofBal(): Unit = {
      // if (debugPrint == "Y") println("bal high values. EOF bal")
      testBalFullKey = "𝖛" * testBalFullKey.length //this is a hack, a higher value constant would be good
      testBalInstID = "𝖛" * testBalInstID.length
      balEOF = "Y"
      // if (tranEOF == "N") lastBalWriteflg = "Y"  // added balEOF == "Y" statement to eofTran routine.  this isn't needed?
    }

    if (balTBLLine.isEmpty) eofBal()
    else readBal()

    val balRecTemplate = stdBalCLS("0", "0", "ACTUALS", "FIN", "SHRD-3RD-PARTY", "STATE-OF-VA", "0", "0",
      "0", "0", "USD", "TXN", "USD", "BASE-LE", "0", "0", "N", " ", " ")

    //-----------------------------------------------------------------------------------------
    // Trans file
    //-----------------------------------------------------------------------------------------

//    case class transCLS(tranVendorName: String, tranAgency: String, tranFund: String, tranProg: String, tranObj: String, tranAmt: String,
//                        tranFisDt: String, tranAcctDt: String, tranTranDt: String, tranType: String)

    var transRec = stdTransCLSPost("","","","","","","","","","","","","","","","","","","","",
      "","","","","","","","","","","") // 31 elements
    var testtranFullKey: String = " "
    var testTransInstID: String = " "
    var transRead = 0

    def readTran(): Unit = {
      val e: Array[String] = transLine.next.split(',')
      transRec = stdTransCLSPost(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9),
        e(10), e(11), e(12), e(13), e(14), e(15), e(16), e(17), e(18), e(19),
        e(20), e(21), e(22), e(23), e(24), e(25), e(26), e(27), e(28), e(29), e(30))
      try {
        testtranFullKey = transRec.transinstID + transRec.transledgerID + transRec.transjrnlType + transRec.transbookCodeID + transRec.translegalEntityID +
          transRec.transcenterID + transRec.transprojectID + transRec.transproductID + transRec.transaccountID +
          transRec.transcurrencyCodeSourceID + transRec.transcurrencyTypeCodeSourceID +
          transRec.transcurrencyCodeTargetID + transRec.transcurrencyTypeCodeTargetID + transRec.transfiscalPeriod.substring(0,4)
      }
      catch {
        case e: StringIndexOutOfBoundsException =>
          println("Non-numeric Amount! Trans record: " + transRead + " trans: " + transRec)
          println("Trans ID/Description: " + transRec.transjrnlID + " " + transRec.transjrnlLineID + " " + transRec.transjrnlDescript)
          throw new StringIndexOutOfBoundsException
      }
      // if (debugPrint == "Y") println("trans key: " + testtranFullKey + " trans row: " + transRec)
      testTransInstID = transRec.transinstID
      transRead += 1
    }

    var tranEOF = "N"
    def eofTran(): Unit = {
      // if (debugPrint == "Y") println("tran high values; EOF tran")
      testtranFullKey = "𝖛" * testtranFullKey.length  //this is a hack, a higher value constant would be good
      testTransInstID = "𝖛" * testTransInstID.length
      tranEOF = "Y"
    }

    if (transLine.isEmpty) eofTran()
    else readTran()

    //-----------------------------------------------------------------------------------------
    // Instrument Update file
    //-----------------------------------------------------------------------------------------
    // insturment update output record structure
    case class instUPDCLS(instUPDID: String, instUPDVendorName: String, instUPDTypeID: String, instUPDEffectDate: String)


    var instUPDRec = instUPDCLS("","","","")
    var testInstUPDFullKey: String = " "
    var testInstUPDInstKey: String = " "
    var instUPDRead = 0

    def readInstUPD(): Unit = {
      val e: Array[String] = instUPDLine.next.split(',')
      instUPDRec = instUPDCLS(e(0),e(1),e(2),e(3))
      testInstUPDFullKey = instUPDRec.instUPDID
      testInstUPDInstKey = instUPDRec.instUPDID
      // if (debugPrint == "Y") println("inst Update key: " + testInstUPDFullKey + " Inst UPD row: " + instUPDRec)
      instUPDRead += 1
    }

    var instUPDEOF = "N"
    def eofInstUPD(): Unit = {
      // if (debugPrint == "Y") println("tran high values; EOF Inst UPD")
      testInstUPDFullKey = "𝖛" * testInstUPDFullKey.length  //this is a hack, a higher value constant would be good
      testInstUPDInstKey = "𝖛" * testInstUPDInstKey.length
      instUPDEOF = "Y"
      //      if (instEOF == "Y") writeInst()  // routine only accessible if instUPD > inst record.  must write for last instUPD record.
    }

    if (instUPDLine.isEmpty) eofInstUPD()
    else readInstUPD()

    //*****************************************************************************************
    // process variables for while loop in file processing
    //*****************************************************************************************
    var swapInst = "N"
    var saveInstFullKey: String = ""
    var saveInstFullRecord = stdInstCLS("","","","","","","","")
    var saveInstID: String = ""
    var swapBal = "N"
    var saveBalFullKey: String = ""
    var saveBalFullRecord = stdBalCLS("","","","","","","","","","","","","","","","","","","")
    var saveBalInstID: String = ""
    var saveBalAmtAccum: Double = 0
    var lastBalWriteflg = "N"
    var counter: Int = 0

    //*****************************************************************************************
    // Mainline Program Structure:
    //  LOAD DATA INTO LISTS:
    //  1 - Read all Instrument for a specific Instrument ID, and load into a list
    //    - Perform Instrument List functions, if specified
    //    - Future would include change data capture on Instruments
    //  2 - Load all balances for same Insturment
    //    - Perform Balance List functions, if specified
    //    - Future would include CRUD processing on balances
    //  3 - Load all Trans
    //    - Perform Trans List functions.  This is Accounting Rules Engine (ARE) functions
    //  PERFORM INITIAL POSTING
    //  4 - Match Transactions to Balances
    //  5 - Update or create new balances
    //  PERFORM TRANSACTION GENERATION
    //  6 - Loop through balances, generating new transactions
    //  7 - This process could become recursive in the future, with levels of generation and next step
    //  PERFORM SECOND POSTING
    //  8 - Apply generated transactions to balances
    //  ALLOCATION STEPS
    //  9 - Creation of basis and allocations would happen here....later functionality
    //  REPORT and UNLOAD (FILE SAVE) LOOP
    //  10- Create various reports in the midst of the unload process
    //  11- Save report structures
    //  CLEAN-UP
    //  12- Produce control report
    //*****************************************************************************************

    while (tranEOF == "N" || balEOF == "N" || instEOF == "N" || instUPDEOF == "N") {

      if (testInstUPDFullKey == testInstFullKey) {
        // if (debugPrint == "Y") {println("==: instUPD: " + testInstUPDFullKey + " Inst: " + testInstFullKey + " remaining rec: " )}
        // {println("==: instUPD: " + testInstUPDFullKey + " Inst: " + testInstFullKey + " remaining rec: " )}
        // compare attributes of instUPD to inst; save an effective date if they are different, create new inst record
        if (instRec.instHolderName != instUPDRec.instUPDVendorName) {
          println("***************************************************************************************")
          println("WARNING! Vendor Name Changed!  New Instrument Record Required.  Additional Code Needed!")
          println("***************************************************************************************")
          //new effective dated instrument record should be created here.
        }
        // read next instUPDsaction record
        if (instUPDEOF == "N") {if (instUPDLine.hasNext) {readInstUPD()}
        else {eofInstUPD()}
        }
      }
      else {
        if (testInstUPDFullKey < testInstFullKey) {
          // if (debugPrint == "Y") {println("<<: instUPD: " + testInstUPDFullKey + " inst: " + testInstFullKey + )}
          // new inst found.  test if new inst is equal to already read and saved inst record from inst file
          if (swapInst == "Y" && testInstUPDFullKey >= saveInstFullKey) {useSaveInst()}
          else {
            if (swapInst == "N") {
              // save prior vendor
              swapInst = "Y"
              saveInstFullKey = testInstFullKey
              saveInstFullRecord = instRec
              saveInstID = testInstInstID
            }
            formatInstwUPD()  // build new inst record
          }
        }
        else {
          if (testInstUPDFullKey > testInstFullKey) {
            // finished with inst record
            // if (debugPrint == "Y") println(">>: after instUPD: " + testInstUPDFullKey + " inst: " + testInstFullKey)

            // Process Transactions and Balances for this Instrument
            while (testTransInstID <=  testInstInstID || testBalInstID <=  testInstInstID) {
              processTranBal()
            }
            // finish old inst and write to output file
            writeInst()
            // test if new vendor is equal to already read and saved inst record from inst file
            if (swapInst == "Y" && (testInstUPDFullKey >= saveInstFullKey || instUPDEOF == "Y")) {
              useSaveInst() // then return to test and write instUPDs record for this inst.
            }
            else {
              if (swapInst == "Y") {formatInstwUPD()} // build new inst record without replacing saved instrument from read ahead
              else {
                // read new instrument
                if (instEOF == "N") {if (instTBLLine.hasNext) {readInst()}
                else {eofInst()}
                }
              }
            }
          }
          else {println("an evaluation error has occurred on the Inst and InstUPD Files!  Files might be out of sort order")}
        }
      }


      def processTranBal (): Unit = {

        if (testtranFullKey == testBalFullKey) {
          // if (debugPrint == "Y") {println("==: tran: " + testtranFullKey + " Bal: " + testBalFullKey + " remaining rec: " )}
          // {println("==: tran: " + testtranFullKey + " Bal: " + testBalFullKey + " remaining rec: " )}
          // matching bal record found, accumulate amount
          // try {
          balAmtAccum = balAmtAccum + transRec.transtransAmount.toDouble
          // }
          // catch {case e: NumberFormatException =>
          //   println("Non-numeric Amount! Trans record: " + transRead + " trans: " + testtranFullKey)
          //   throw new NumberFormatException
          // }
          // read next transaction record
          if (tranEOF == "N") {
            if (transLine.hasNext) {
              readTran()
            }
            else {
              eofTran()
            }
          }
        }
        else {
          if (testtranFullKey < testBalFullKey) {
            // if (debugPrint == "Y") {println("<<: tran: " + testtranFullKey + " inst: " + testBalFullKey + " Max ID: " + maxBalID.toString)}
            // new bal found.  test if new bal is equal to already read and saved bal record from bal file
            if (swapBal == "Y" && testtranFullKey >= saveBalFullKey) {
              useSaveBal()
            }
            else {
              if (swapBal == "N") {
                // save prior vendor
                swapBal = "Y"
                saveBalFullKey = testBalFullKey
                saveBalInstID = testBalInstID
                saveBalFullRecord = balRec
                saveBalAmtAccum = balAmtAccum
                balAmtAccum = 0
              }
              formatBalwTran() // build new bal record
            }
          }
          else {
            if (testtranFullKey > testBalFullKey) {
              // finished with bal record
              // if (debugPrint == "Y") println(">>: after tran: " + testtranFullKey + " bal: " + testBalFullKey)
              // finish old bal and write to output file
              writeBal()
              // test if new balance is equal to already read and saved balance record from bal file
              if (swapBal == "Y" && (testtranFullKey >= saveBalFullKey || tranEOF == "Y")) {
                useSaveBal() // then return to test and accumulate trans record for this balance.
              }
              else {
                if (swapBal == "Y") {
                  formatBalwTran()
                } // build new bal record without replacing saved balance from read ahead
                else {
                  // read new balance
                  if (balEOF == "N") {
                    if (balTBLLine.hasNext) {
                      readBal()
                    }
                    else {
                      eofBal()
                    }
                  }
                }
              }
            }
            else {
              println("an evaluation error has occurred on the Balance and Trans Files!  Files might be out of sort order")
            }
          }
        }
      }
      counter += 1
      // if (debugPrint == "Y") println("counter val: " + counter)
      // One additional loop is required once either the Inst or the InstUPD file is finished to write the Last Balance
      if (instEOF == "Y" && instUPDEOF == "Y" && (tranEOF == "N" || balEOF == "N")) {
        // if (debugPrint == "Y") println("Finish extra instrument")

        // Any additional times through means there are trans or balances without instruments
        if (lastBalWriteflg == "Y") {
          println("***************************************************************************************")
          println("WARNING! Transactions or Balances without Instruments.  Critical Error:  Files Invalid!")
          println("***************************************************************************************")
          tranEOF = "Y"
          balEOF = "Y"
        }

        lastBalWriteflg = "Y"
      }
    }

    //*****************************************************************************************
    // Write max ID record
    //*****************************************************************************************
    // insert new updated max ID value
    newMaxIDwriter.write(maxKeySysID + "," + maxKeyInstID + ","
      + 0.toString * (10 - maxBalID.toString.length) + maxBalID.toString + ","   //left fill with zeros to 10 digits
      + maxKeyTranID + ","
      + maxKeyTimeStamp + ","
      + maxKeyComment)

    //*****************************************************************************************
    // Close files
    //*****************************************************************************************
    newMaxIDwriter.close()
    newInstTBLwriter.close()
    newTranwriter.close()
    newBalTBLwriter.close()

    //*****************************************************************************************
    // Control Report
    //*****************************************************************************************

    println("*****************************************")
    println("Balance Posting Program Control Report")
    println("While Loops:                " + counter)
    println("Instruments Read:           " + instRead)
    println("Instrument Updates Read:    " + instUPDRead)
    println("Transactions Read:          " + transRead)
    println("Balances Read:              " + balRead)
    println("Instruments Written:        " + instWritten)
    println("Balances Written:           " + balWritten)
    println("Balance IDs Assigned:       " + (maxBalID - startMaxBalID))
    println("*****************************************")


    //*****************************************************************************************
    //*****************************************************************************************
    // End of Main def drop through logic
    //*****************************************************************************************
    //*****************************************************************************************

    //*****************************************************************************************
    // Common Routines
    //*****************************************************************************************

    // Used the Saved Insturment from prior read
    def useSaveInst(): Unit = {
      swapInst = "N"
      testInstFullKey = saveInstFullKey
      testInstInstID = saveInstID
      instRec = saveInstFullRecord
    }

    def formatInstwUPD(): Unit = {
      testInstFullKey = testInstUPDFullKey
      testInstInstID = testInstUPDInstKey
      //val stdBalRecOut = balRecTemplate.copy(
      instRec = instRecTemplate.copy(
        instUPDRec.instUPDID,
        instUPDRec.instUPDEffectDate,
        "0",                                   //  Record Effective Start Time
        "9999/99/99",                          //  Record Effective End Date
        "9",                                   //  Record Effective End Time
        instUPDRec.instUPDVendorName,
        instUPDRec.instUPDTypeID,
        "0"                                     // Audit Time Stamp
      )
    }

    def writeInst(): Unit = {
      val instSeq: String =
        instRec.instinstID + "," +
          instRec.instEffectDate + "," +
          instRec.instEffectTime + "," +
          instRec.instEffectEndDate + "," +
          instRec.instEffectEndTime + "," +
          instRec.instHolderName + "," +
          instRec.instTypeID + "," +
          instRec.instAuditTrail

      newInstTBLwriter.write(instSeq + "\n")
      // if (debugPrint == "Y") println("Final output Inst: " + instRec)
      instWritten += 1
    }

    // Used the Saved vendor from prior read
    def useSaveBal(): Unit = {
      swapBal = "N"
      testBalFullKey = saveBalFullKey
      testBalInstID = saveBalInstID
      balRec = saveBalFullRecord  // not sure if I can put data back in here????????????????????????????
      balAmtAccum = saveBalAmtAccum
    }

    def formatBalwTran(): Unit = {
      testBalFullKey = testtranFullKey
      testBalInstID = testTransInstID
      maxBalID += 1 // increment max bal ID field

      //val stdBalRecOut = balRecTemplate.copy(
      balRec = balRecTemplate.copy(
        transRec.transinstID,
        0.toString * (10 - maxBalID.toString.length) + maxBalID.toString,  //left fill with zeros to 10 digits
        transRec.transledgerID,
        transRec.transjrnlType,
        transRec.transbookCodeID,
        transRec.translegalEntityID,
        transRec.transcenterID,
        transRec.transprojectID,
        transRec.transproductID,
        transRec.transaccountID,
        transRec.transcurrencyCodeSourceID,
        transRec.transcurrencyTypeCodeSourceID,
        transRec.transcurrencyCodeTargetID,
        transRec.transcurrencyTypeCodeTargetID,
        transRec.transfiscalPeriod.substring(0,4),
        balAmtAccum.toString,                                 //16th element
        transRec.transmovementFlg,
        transRec.transunitOfMeasure,
        transRec.transstatisticAmount
      )
    }

    def writeBal(): Unit = {
      val balSeq: String =
        balRec.balinstID + "," +
          balRec.balbalID + "," +
          balRec.balledgerID + "," +
          balRec.baljrnlType + "," +
          balRec.balbookCodeID + "," +
          balRec.ballegalEntityID + "," +
          balRec.balcenterID + "," +
          balRec.balprojectID + "," +
          balRec.balproductID + "," +
          balRec.balaccountID + "," +
          balRec.balcurrencyCodeSourceID + "," +
          balRec.balcurrencyTypeCodeSourceID + "," +
          balRec.balcurrencyCodeTargetID + "," +
          balRec.balcurrencyTypeCodeTargetID + "," +
          balRec.balfiscalYear + "," +
          round(balAmtAccum, 2) + "," + //16th element
          balRec.balmovementFlg + "," +
          balRec.balunitOfMeasure + "," +
          balRec.balstatisticAmount

      newBalTBLwriter.write(balSeq + "\n")
      // if (debugPrint == "Y") println("Final output bal: " + balRec)
      balAmtAccum = 0
      balWritten += 1
    }


    def round(x: Double, p: Int): String = {
      try {
        val Amt = f"$x%1.2f".split('.')
        Amt(0) + "." + Amt(1).substring(0, if (p > Amt(1).length()) Amt(1).length() else p)
      }
      catch {case e: NumberFormatException =>
        println("Non-numeric Amount! Trans record: " + transRead + " trans: " + testtranFullKey)
        println("Trans ID/Description: " + transRec.transjrnlID + " " + transRec.transjrnlLineID + " " + transRec.transjrnlDescript)
        println("Amount: " + x.toString + " Balrec ID: " + balRec.balinstID + "," + balRec.balbalID)
        println("Balance Key "  + balRec.balledgerID + " " +
          balRec.baljrnlType + " " +
          balRec.balbookCodeID + " " +
          balRec.ballegalEntityID + " " +
          balRec.balcenterID + " " +
          balRec.balprojectID + " " +
          balRec.balproductID + " " +
          balRec.balaccountID + " ")

        throw new NumberFormatException
      }

    }
  }
}
