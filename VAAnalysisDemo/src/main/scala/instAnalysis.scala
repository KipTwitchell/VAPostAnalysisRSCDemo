import java.io.{File, PrintWriter}

import scala.io.Source
import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.io.StdIn.readLine

/*
 * //	(c) Copyright IBM Corporation. 2017
//  SPDX-License-Identifier: Apache-2.0
//     Kip Twitchell <finsysvlogger@gmail.com>.
//  Created July 2017
 */


object instAnalysis {
  def main(args: Array[String]): Unit = {

    val balFileNames = Array(
      "B103bal.txtInstID0.txt",
      "B103bal.txtInstID1.txt",
      "B103bal.txtInstID2.txt",
      "B103bal.txtInstID3.txt",
      "B103bal.txtInstID4.txt",
      "B103bal.txtInstID5.txt",
      "B103bal.txtInstID6.txt",
      "B103bal.txtInstID7.txt",
      "B103bal.txtInstID8.txt",
      "B103bal.txtInstID9.txt",
      "B104bal.txtInstID0.txt",
      "B104bal.txtInstID1.txt",
      "B104bal.txtInstID2.txt",
      "B104bal.txtInstID3.txt",
      "B104bal.txtInstID4.txt",
      "B104bal.txtInstID5.txt",
      "B104bal.txtInstID6.txt",
      "B104bal.txtInstID7.txt",
      "B104bal.txtInstID8.txt",
      "B104bal.txtInstID9.txt",
      "B105bal.txtInstID0.txt",
      "B105bal.txtInstID1.txt",
      "B105bal.txtInstID2.txt",
      "B105bal.txtInstID3.txt",
      "B105bal.txtInstID4.txt",
      "B105bal.txtInstID5.txt",
      "B105bal.txtInstID6.txt",
      "B105bal.txtInstID7.txt",
      "B105bal.txtInstID8.txt",
      "B105bal.txtInstID9.txt",
      "B106bal.txtInstID0.txt",
      "B106bal.txtInstID1.txt",
      "B106bal.txtInstID2.txt",
      "B106bal.txtInstID3.txt",
      "B106bal.txtInstID4.txt",
      "B106bal.txtInstID5.txt",
      "B106bal.txtInstID6.txt",
      "B106bal.txtInstID7.txt",
      "B106bal.txtInstID8.txt",
      "B106bal.txtInstID9.txt",
      "B107bal.txtInstID0.txt",
      "B107bal.txtInstID1.txt",
      "B107bal.txtInstID2.txt",
      "B107bal.txtInstID3.txt",
      "B107bal.txtInstID4.txt",
      "B107bal.txtInstID5.txt",
      "B107bal.txtInstID6.txt",
      "B107bal.txtInstID7.txt",
      "B107bal.txtInstID8.txt",
      "B107bal.txtInstID9.txt",
      "B108bal.txtInstID0.txt",
      "B108bal.txtInstID1.txt",
      "B108bal.txtInstID2.txt",
      "B108bal.txtInstID3.txt",
      "B108bal.txtInstID4.txt",
      "B108bal.txtInstID5.txt",
      "B108bal.txtInstID6.txt",
      "B108bal.txtInstID7.txt",
      "B108bal.txtInstID8.txt",
      "B108bal.txtInstID9.txt",
      "B109bal.txtInstID0.txt",
      "B109bal.txtInstID1.txt",
      "B109bal.txtInstID2.txt",
      "B109bal.txtInstID3.txt",
      "B109bal.txtInstID4.txt",
      "B109bal.txtInstID5.txt",
      "B109bal.txtInstID6.txt",
      "B109bal.txtInstID7.txt",
      "B109bal.txtInstID8.txt",
      "B109bal.txtInstID9.txt",
      "B110bal.txtInstID0.txt",
      "B110bal.txtInstID1.txt",
      "B110bal.txtInstID2.txt",
      "B110bal.txtInstID3.txt",
      "B110bal.txtInstID4.txt",
      "B110bal.txtInstID5.txt",
      "B110bal.txtInstID6.txt",
      "B110bal.txtInstID7.txt",
      "B110bal.txtInstID8.txt",
      "B110bal.txtInstID9.txt",
      "B111bal.txtInstID0.txt",
      "B111bal.txtInstID1.txt",
      "B111bal.txtInstID2.txt",
      "B111bal.txtInstID3.txt",
      "B111bal.txtInstID4.txt",
      "B111bal.txtInstID5.txt",
      "B111bal.txtInstID6.txt",
      "B111bal.txtInstID7.txt",
      "B111bal.txtInstID8.txt",
      "B111bal.txtInstID9.txt",
      "B112bal.txtInstID0.txt",
      "B112bal.txtInstID1.txt",
      "B112bal.txtInstID2.txt",
      "B112bal.txtInstID3.txt",
      "B112bal.txtInstID4.txt",
      "B112bal.txtInstID5.txt",
      "B112bal.txtInstID6.txt",
      "B112bal.txtInstID7.txt",
      "B112bal.txtInstID8.txt",
      "B112bal.txtInstID9.txt",
      "B113bal.txtInstID0.txt",
      "B113bal.txtInstID1.txt",
      "B113bal.txtInstID2.txt",
      "B113bal.txtInstID3.txt",
      "B113bal.txtInstID4.txt",
      "B113bal.txtInstID5.txt",
      "B113bal.txtInstID6.txt",
      "B113bal.txtInstID7.txt",
      "B113bal.txtInstID8.txt",
      "B113bal.txtInstID9.txt",

      "B114bal.txtInstID0.txt",
      "B114bal.txtInstID1.txt",
      "B114bal.txtInstID2.txt",
      "B114bal.txtInstID3.txt",
      "B114bal.txtInstID4.txt",
      "B114bal.txtInstID5.txt",
      "B114bal.txtInstID6.txt",
      "B114bal.txtInstID7.txt",
      "B114bal.txtInstID8.txt",
      "B114bal.txtInstID9.txt",

      "B115bal.txtInstID0.txt",
      "B115bal.txtInstID1.txt",
      "B115bal.txtInstID2.txt",
      "B115bal.txtInstID3.txt",
      "B115bal.txtInstID4.txt",
      "B115bal.txtInstID5.txt",
      "B115bal.txtInstID6.txt",
      "B115bal.txtInstID7.txt",
      "B115bal.txtInstID8.txt",
      "B115bal.txtInstID9.txt",

      "B116bal.txtInstID0.txt",
      "B116bal.txtInstID1.txt",
      "B116bal.txtInstID2.txt",
      "B116bal.txtInstID3.txt",
      "B116bal.txtInstID4.txt",
      "B116bal.txtInstID5.txt",
      "B116bal.txtInstID6.txt",
      "B116bal.txtInstID7.txt",
      "B116bal.txtInstID8.txt",
      "B116bal.txtInstID9.txt"

    )

    val instFileNames = Array(
      "VATestInstTBL.txtInstID0.txt",
      "VATestInstTBL.txtInstID1.txt",
      "VATestInstTBL.txtInstID2.txt",
      "VATestInstTBL.txtInstID3.txt",
      "VATestInstTBL.txtInstID4.txt",
      "VATestInstTBL.txtInstID5.txt",
      "VATestInstTBL.txtInstID6.txt",
      "VATestInstTBL.txtInstID7.txt",
      "VATestInstTBL.txtInstID8.txt",
      "VATestInstTBL.txtInstID9.txt"
    )

    case class stdInstCLSAccum(
                           // Element      Data Type                Default Value       Description
                           instinstID: String,                     // "0",               Instrument ID
                           instEffectDate: String,                 // "0000-00-00",      Record Effective Start Date
                           instEffectTime: String,                 // "0",               Record Effective Start Time
                           instEffectEndDate: String,              // "9999-99-99",      Record Effective End Date
                           instEffectEndTime: String,              // "9",               Record Effective End Time
                           instHolderName: String,                 // "Vendor Name",     Name of Instrument Holder
                           instTypeID: String,                     // "EXP",             Vendor (EXP) or REV Record
                           instAuditTrail: String)                 // "0,                Timestamp Updated



    case class stdBalCLSAccum(
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
    
    val stdBalTitleCSVRow =
      "Instrument ID" + "," +
      "Balance ID" + "," +
      "Ledger" + "," +
      "Jrnl Type" + "," +
      "Book-code / Basis" + "," +
      "Legal Entity (CO. / Owner)" + "," +
      "Center ID" + "," +
      "Project ID" + "," +
      "Product / Material ID" + "," +
      "Nominal Account" + "," +
      "Curr. Code Source" + "," +
      "Currency Type Code Source" + "," +
      "Curr. Code Target" + "," +
      "Currency Type Code Target" + "," +
      "Fiscal Year - main bucket key" + "," +
      "Transaction Amount" + "," +
      "Movement Flag" + "," +
      "Unit of Measure" + "," +
      "Statistical Amount"


    case class aggBalCLS(
                          // Element      Data Type                Default Value       Description
                          aggledgerID: String,                   // "ACTUALS",         Ledger
                          aggjrnlType: String,                   // "FIN",             Jrnl Type
                          aggbookCodeID: String,                 // "SHRD-3RD-PARTY",  Book-code / Basis
                          agglegalEntityID: String,              // "STATE-OF-VA",     Legal Entity (CO. / Owner)
                          aggcenterID: String,                   // "0",               Center ID
                          aggprojectID: String,                  // "0",               Project ID
                          aggproductID: String,                  // "0",               Product / Material ID
                          aggaccountID: String,                  // "0",               Nominal Account
                          aggcurrencyCodeSourceID: String,       // "USD",             Curr. Code Source
                          aggcurrencyTypeCodeSourceID: String,   // "TXN",             Currency Type Code Source
                          aggcurrencyCodeTargetID: String,       // "USD",             Curr. Code Target
                          aggcurrencyTypeCodeTargetID: String,   // "BASE-LE",         Currency Type Code Target
                          aggfiscalYear: String)                 // "0",               Fiscal Year - main bucket key

    val aggBalTitleCSVRow =
        "Ledger" + "," +
        "Jrnl Type" + "," +
        "Book-code / Basis" + "," +
        "Legal Entity (CO. / Owner)" + "," +
        "Center ID" + "," +
        "Project ID" + "," +
        "Product / Material ID" + "," +
        "Nominal Account" + "," +
        "Curr. Code Source" + "," +
        "Currency Type Code Source" + "," +
        "Curr. Code Target" + "," +
        "Currency Type Code Target" + "," +
        "Fiscal Year - main bucket key" + "," +
        "Transaction Amount"

    case class aggAllSmallCLS(
                          // Element      Data Type                Default Value       Description
                          aggledgerID: String,                   // "ACTUALS",         Ledger
                          aggjrnlType: String,                   // "FIN",             Jrnl Type
                          aggbookCodeID: String,                 // "SHRD-3RD-PARTY",  Book-code / Basis
                          agglegalEntityID: String,              // "STATE-OF-VA",     Legal Entity (CO. / Owner)
                          aggcenterID: String,                   // "0",               Center ID
                          aggprojectID: String,                  // "0",               Project ID
                          aggproductID: String,                  // "0",               Product / Material ID
                          aggaccountID: String,                  // "0",               Nominal Account
                          aggfiscalYear: String                 // "0",               Fiscal Year - main bucket key
                           )

    val aggAllSmallTitleTabRow =
      "Ledger" + "\t" +
        "Jrnl Type" + "\t" +
//        "Book-code / Basis" + "\t" +
        "Legal Entity" + "\t" +
        "Center ID" + "\t" +
        "Project ID" + "\t" +
        "Product / Material ID" + "\t" +
        "Fiscal Year" + "\t" +
        "Nominal Account"



    println("*" * 100 )
    println("                           Consolidated Data Supply Chain")
    println("                           Data Analysis Proof of Concept")
    println("*" * 100 )

    // ------------------ uncomment below lines when using in PASSED ARGS parameters
    println("Using Passed Parameters")
    val filePaths = Map("inputPath" -> args(0), "outputPath" -> args(1))
    val fileNames = Map("maxIDKeyFileName" -> args(2), "instTBLFileName" -> args(3),
      "balTBLFileName" -> args(4), "tranTBLFileName" -> args(5), "instUPDFileName" -> args(6),
      "sumTBLFileName" -> args(7), "aggTBLFileName" -> args(8))
    // ------------------ uncomment above lines when using PASSED ARGS program parameters

    // ------------------ un comment below lines when using IN PROGRAM file parameters
//    var filePaths = Map("inputPath" -> "/Users/ktwitchell001/workspace/VADataDemo/VADataParallelSept5/", "outputPath" ->
//      "/Users/ktwitchell001/workspace/VADataDemo/VADataParallelSept5/")
//    var filePaths = Map("inputPath" -> "/Users/ktwitchell001/workspace/VADataDemo/VATestData/", "outputPath" ->
//      "/Users/ktwitchell001/workspace/VADataDemo/VATestData/")
//    var fileNames = Map("maxIDKeyFileName" -> "VATestMaxID.txt",
//      "instTBLFileName" -> "VATestInstTBL.txt",
//      "balTBLFileName" -> "VATestBalTBL.txt",
//      "tranTBLFileName" -> "VATestTranTBL.txt", // this file is a temp file used as input to sort for transactions
//      "instUPDFileName" -> "VATestInstUPD.txt",
//      "sumTBLFileName" -> "VATestSumTBL.csv",
//      "aggTBLFileName" -> "VATestAggTBL.csv")
    // ------------------ uncomment above lines when using IN PROGRAM file parameters


    //*****************************************************************************************
    // Initial Prompts
    //*****************************************************************************************

    println("Type q at any prompt to quit the program")

    println("How many years of data do you want to analyze? (01 - 13) ")
    val loadYears = scala.io.StdIn.readLine().trim.toUpperCase()
    if (loadYears.substring(0,1) == "Q") sys.exit(0)
    println("OK, we'll load " + loadYears + " year's worth of data")

    println("Starting with which year (2003 - 2016) ")
    val startYears = scala.io.StdIn.readLine().trim.toUpperCase()
    if (startYears.substring(0,1) == "Q") sys.exit(0)
    println("OK, we'll load data starting with year " + startYears)

    println("How many top valued Instruments do you want to analyze? (1 - 1000) ")
    var topInstCount = scala.io.StdIn.readLine().trim.toUpperCase()
    if (topInstCount.substring(0,1) == "Q") sys.exit(0)
    println("OK, we'll track the top " + topInstCount + " instruments")

    println("Should we load Instrument Records for Names, etc.?  (Y/N)")
    var loadInstRecs = scala.io.StdIn.readLine().trim.toUpperCase()
    if (loadInstRecs.substring(0,1) == "Q") sys.exit(0)
    if (loadInstRecs == "Y" || loadInstRecs == "y") println("OK, we'll load Instrument Records")
    else  println("OK, we'll NOT load Instrument Records")

    println("*" * 100 )
    println("Loading Tables")

    //*****************************************************************************************
    // Open Files
    //*****************************************************************************************
    // balance file
    if (startYears.toInt + loadYears.toInt > 2016) {println("Year range must be 2003 - 2016.  Start again.");sys.exit(0)}
    var currentFileNameIndex = (startYears.toInt - 2003) * 10
    var filePartionCount = 10 * loadYears.toInt
    var balX = Source.fromFile(filePaths("inputPath") + balFileNames(currentFileNameIndex)).getLines()
    var balTBLLine = balX.buffered
    filePartionCount -= 1 // we have just processed the first file here.


    // inst file
    var fileInstPartionCount = 9
    var instX = Source.fromFile(filePaths("inputPath") + instFileNames(fileInstPartionCount)).getLines()
    var instTBLLine = instX.buffered
    fileInstPartionCount -= 1 // we have just processed the first file here.

    // Agency Reference File -> Maps to Universal Journal Legal Entity Field
    // AGY_AGENCY_CODE,AGY_AGENCY_KEY,AGY_AGENCY_NAME
    val leTBL: Map[String,(String,String)] = Map.empty
    var agencyRead = 0
    var agencyX = Source.fromFile(filePaths("inputPath") + "aRefAgencyCSV.txt")
    for (line <- agencyX.getLines) {
      val cols = line.split(",").map(_.trim)
      leTBL.+=(cols(1) -> (cols(0),cols(2)))
      agencyRead += 1
    }

    // Fund Reference File -> Map to Universal Journal Center Field
    // FNDDTL_FUND_DETAIL_KEY,FNDDTL_FUND_DETAIL_CODE,FNDDTL_FUND_DETAIL_NAME,FND_FUND_CODE,FND_FUND_NAME
    val centerTBL: Map[String,(String,String,String,String)] = Map.empty
    var fundRead = 0
    var fundX = Source.fromFile(filePaths("inputPath") + "aRefFundCSV.txt")
    for (line <- fundX.getLines) {
      val cols = line.split(",").map(_.trim)
      centerTBL.+=(cols(0) -> (cols(1),cols(2),cols(3),cols(4)))
      fundRead += 1
    }

    // Program Reference File -> Maps to Universal Journal Project Field
    // SPRG_SUB_PROGRAM_KEY,PFCN_FUNCTION_CODE,PFCN_FUNCTION_NAME,PRG_PROGRAM_CODE,PRG_PROGRAM_NAME,SPRG_SUB_PROGRAM_CODE,,SPRG_SUB_PROGRAM_NAME
    val projectTBL: Map[String,(String,String,String,String, String,String,String)] = Map.empty
    var programRead = 0
    var programX = Source.fromFile(filePaths("inputPath") + "aRefProgramCSV.txt")
    for (line <- programX.getLines) {
      val cols = line.split(",").map(_.trim)
      projectTBL.+=(cols(0) -> (cols(1),cols(2),cols(3),cols(4),cols(5),cols(6),cols(7)))
      programRead += 1
    }

    // Object Reference File -> Maps to Universal Journal Nominal Acccounts for Expenses field
    // OBJ_OBJECT_KEY,2-DIGIT OBJECT CODE,2-DIGIT OBJECT NAME,4-DIGIT OBJECT CODE,4-DIGIT OBJECT NAME
    val acctTBL: Map[String,(String,String,String,String,String)] = Map.empty
    var objectRead = 0
    var objectX = Source.fromFile(filePaths("inputPath") + "aRefObjectCSV.txt")
    for (line <- objectX.getLines) {
      val cols = line.split(",").map(_.trim)
      acctTBL.+=("EXP" + cols(0) -> (cols(1),cols(2),cols(3),cols(4),"EXP"))
      objectRead += 1
    }

    // Source Reference File -> Maps to Universal Journal Nominal Acccounts for Revenue field
    // table is added to the table defined above, with the prefix "REV" for all rows
    // SRC_SOURCE_KEY,SRCCLS_SOURCE_CLASS_CODE,SRCCLS_SOURCE_CLASS_NAME,SRC_SOURCE_CODE,,SRC_SOURCE_NAME
    var sourceRead = 0
    var sourceX = Source.fromFile(filePaths("inputPath") + "aRefSourceCSV.txt")
    for (line <- sourceX.getLines) {
      val cols = line.split(",").map(_.trim)
      acctTBL.+=("REV" + cols(0) -> (cols(1),cols(2),cols(3),cols(5),"REV"))
      sourceRead += 1
    }

    //*****************************************************************************************
    // Open output files
    //*****************************************************************************************
    // summary view table
    val newSumTBLwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("sumTBLFileName")))

    // summary view table
    val newAggTBLwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("aggTBLFileName")))

    //    var balWritten = 0
//    var instWritten = 0
    var sumWritten = 0
    var aggWritten = 0


    //-----------------------------------------------------------------------------------------
    // Balance File
    //-----------------------------------------------------------------------------------------

    var balRec = stdBalCLSAccum("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "0", "", "", "")
    var testBalFullKey: String = " "
    var testBalInstID: String = " "
    var balAmtAccum: Double = 0
    var balRead = 0

    def readBal(): Unit = {
      val e: Array[String] = balTBLLine.next.split(',')
      balRec = stdBalCLSAccum(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11),
        e(12), e(13), e(14), e(15), e(16), e(17), e(18)) //19 elements
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
      if (filePartionCount > 0) {
        filePartionCount -= 1
        currentFileNameIndex += 1
        balX = Source.fromFile(filePaths("inputPath") + balFileNames(currentFileNameIndex)).getLines()
        balTBLLine = balX.buffered
        println("New Balance File Started.  Partition number: " + filePartionCount)
      }
      else {
        testBalFullKey = "𝖛" * testBalFullKey.length //this is a hack, a higher value constant would be good
        testBalInstID = "𝖛" * testBalInstID.length
        balEOF = "Y"
      }
    }

    if (balTBLLine.isEmpty) eofBal()
    else readBal()

//    val balRecTemplate = stdBalCLSAccum("0", "0", "ACTUALS", "FIN", "SHRD-3RD-PARTY", "0", "0", "0",
//      "0", "0", "USD", "TXN", "USD", "BASE-LE", "0", "0", "N", " ", " ")

    //-----------------------------------------------------------------------------------------
    // Instrument File
    //-----------------------------------------------------------------------------------------

    // the internal map table of instruments, keyed by Instrument ID
    // Should add effective date at some point perhaps.  That will require something other than a Map function though
    var instMap: Map[String, stdInstCLSAccum] = Map.empty

    var instRead = 0

    def readInst(): Unit = {
      val e: Array[String] = instTBLLine.next.split(',')
      instMap.+=(e(0) -> stdInstCLSAccum(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7)))
      instRead += 1
    }

    var instEOF = "N"

    def eofInst(): Unit = {
      if (fileInstPartionCount > 0) {
        instX = Source.fromFile(filePaths("inputPath") + instFileNames(fileInstPartionCount)).getLines()
        instTBLLine = instX.buffered
        println("New Inst File Started.  Partition number: " + fileInstPartionCount)
        fileInstPartionCount -= 1
      }
      else {
        instEOF = "Y"
      }
    }

    // initial read of file performed after while loop

    //    val balRecTemplate = stdBalCLSAccum("0", "0", "ACTUALS", "FIN", "SHRD-3RD-PARTY", "0", "0", "0",
    //      "0", "0", "USD", "TXN", "USD", "BASE-LE", "0", "0", "N", " ", " ")


    //*****************************************************************************************
    // Tabled Instrument Tables
    //*****************************************************************************************

    var allBalancesTree = new TreeMap[(String,String), stdBalCLSAccum]

    //*****************************************************************************************
    // Higher level aggregation table, with join fields
    //*****************************************************************************************

//    var aggAllKeyAmount: String = ""                // "0",               Transaction Amount
    var aggAllKeyTBLByYear: mutable.Map[aggBalCLS, String] = mutable.Map.empty

    var aggAllSmallTBL: mutable.Map[aggAllSmallCLS, String] = mutable.Map.empty


    //*****************************************************************************************
    // Top Value Instrument Tables
    //*****************************************************************************************

    val topTableMax = topInstCount.toInt  // size of top instruments determiend by screen input parameter
    // generated seeded zero values in the top table provided by toGenSet
    var topGenSet: ListBuffer[String] = ListBuffer.empty
    for (x <- 1 to topTableMax) topGenSet += ("seeded" + x + " ")
    var topValAmtAccumulator: Double = 0.00      // The accumulator of the Instrument Balance
    // A Map to maintain listings of accumulated Instrument Balances mapped to all associated instruments
    // This is needed in case there are two instruments with the same balance
    var topValAmtInstMap: Map[Double, ListBuffer[String]] = Map(topValAmtAccumulator -> topGenSet)
    // This List is a simple listing of the top (topTableMax) balances to be maintained in memory
    var topValAmounts: ListBuffer[Double] = ListBuffer.fill(topTableMax)(topValAmtAccumulator)
    // tabled version of all balances for top instruments
//    var topValBalancesTree = new TreeMap[(String,String), stdBalCLSAccum]

    //*****************************************************************************************
    // process variables for while loop in file processing
    //*****************************************************************************************
    val formatter = java.text.NumberFormat.getInstance
    var chgBalInstID: String = testBalInstID
    var counter: Int = 0


    while (balEOF == "N") {
      if (chgBalInstID == testBalInstID) {
        // store bal record in table
        allBalancesTree += ((balRec.balinstID, balRec.balbalID) -> balRec)
        // update highest level aggregate with balance
        var w = aggAllSmallCLS(
          balRec.balledgerID,
          balRec.baljrnlType,
          balRec.balbookCodeID,
          balRec.ballegalEntityID,
          balRec.balcenterID,
          balRec.balprojectID,
          balRec.balproductID,
          balRec.balaccountID,
          balRec.balfiscalYear)
        if (aggAllSmallTBL contains w) {
          aggAllSmallTBL.+=(w -> (aggAllSmallTBL(w).toDouble + balRec.baltransAmount.toDouble).toString)
        }
        else aggAllSmallTBL.+=(w -> balRec.baltransAmount)

        // read next balance record
        if (balEOF == "N") {
          if (balTBLLine.hasNext) readBal()
          else eofBal()
        }
      }
      else {
        //  Change in Instrument ID Detected.  Before writing the Balance
        //  First accumulate all balances for the last Instrument
        val valKeys = allBalancesTree.keySet.from((chgBalInstID,"")).to((chgBalInstID,"9999999999"))
        for (x <- valKeys) topValAmtAccumulator += allBalancesTree(x).baltransAmount.toDouble
        // Test if the accumulated Instrument Balance is greater than the lowest amount in the list
        if ( topValAmounts.min < topValAmtAccumulator ) {
          // Remove lowest balance from list, and add new balance to list
          // println("Min: " + topValAmounts.min + " New Amt: " + topValAmtAccumulator)
          //-----------------------------------------------------------------------------------------
          // Remove prior elements from tables
          //-----------------------------------------------------------------------------------------
          val l: Double = topValAmounts.min
          var i: ListBuffer[String] = topValAmtInstMap(l)
          println("New Top Amount Found! " + formatter.format(topValAmtAccumulator)
            + " New ID: " + chgBalInstID + " Previous amount: " + formatter.format(l) + " Old ID: " + i.head)
//            + " Partition number: " + fileInstPartionCount)
//          println("Amount to be removed: " + l + " Inst: " + i.head + " Amount to be added: " + topValAmtAccumulator + " Inst: " + chgBalInstID)
          val topValBalKeys = allBalancesTree.keySet.from((i.head,"")).to((i.head,"9999999999"))
          // This is commented out because the topValBalancesTree was only the balances for Top Instruments.
          // All instruments saved now.  Do not delete
//          for (x <- topValBalKeys) {
////            println("element to be removed.  Inst: " + i.head + " Bal: " + x )
//            topValBalancesTree -= x
//          }
//          i -= i.head
          i.remove(0)
          topValAmtInstMap -=l
          if (i.nonEmpty) {
            // put the other equal amount pointer back in the table linking amounts to instruments
            topValAmtInstMap +=((l,i))
          }
          topValAmounts -= l
          //-----------------------------------------------------------------------------------------
          // Top Value Inst: Insert new elements into tables
          //-----------------------------------------------------------------------------------------
          var m: ListBuffer[String] = ListBuffer.empty
          try {
            m = ListBuffer(chgBalInstID) ++ topValAmtInstMap(topValAmtAccumulator)
          }
          catch {
            case e: NoSuchElementException =>
            m = ListBuffer(chgBalInstID)
          }
          topValAmtInstMap += ((topValAmtAccumulator, m))
          topValAmounts+=topValAmtAccumulator
        }
        else {
          if ( topValAmounts.min == topValAmtAccumulator ) {
            //-----------------------------------------------------------------------------------------
            // Top Value Inst. Equal amount found, so two inst with the same balance, Insert Inst ID into Amt set of Inst
            //-----------------------------------------------------------------------------------------
            var m: ListBuffer[String] = topValAmtInstMap(topValAmtAccumulator) //existing list
            m ++= Set(chgBalInstID) // merge in new instruemnt
            topValAmtInstMap += ((topValAmtAccumulator, m))
            topValAmounts+=topValAmtAccumulator
            println("Equal element to be added.  Inst: " + chgBalInstID + " Bal: " + topValAmtAccumulator )
//            println("Set After Insert" + topValAmtInstMap(topValAmtAccumulator))
          }
          else {
            //-----------------------------------------------------------------------------------------
            // Not a Top Value Instrument:  Do not save Balance Rows for the instrument
            //-----------------------------------------------------------------------------------------
            // This is commented out because the topValBalancesTree was only the balances for Top Instruments.
            // All instruments saved now.  Do not delete
//            val valKeys = topValBalancesTree.keySet.from((chgBalInstID,"")).to((chgBalInstID,"9999999999"))
//            for (x <- valKeys) topValBalancesTree -= x
          }
        }
        //-----------------------------------------------------------------------------------------
        // End of If Test for Top Value Instruments
        //-----------------------------------------------------------------------------------------

        // Move to next Instrument
        chgBalInstID = testBalInstID
        topValAmtAccumulator = 0
      }
      //-----------------------------------------------------------------------------------------
      // End of Change in Instrument ID Logic
      //-----------------------------------------------------------------------------------------
      counter += 1
    }
    //-----------------------------------------------------------------------------------------
    // End of While Loop
    //-----------------------------------------------------------------------------------------


    //-----------------------------------------------------------------------------------------
    // Load Instrument Table based upon user response to initial prompt
    //-----------------------------------------------------------------------------------------

    if (loadInstRecs == "Y") {
      println("Loading Instrument Records")
      println("New Inst File Started.  Partition number: " + (fileInstPartionCount+1))
      if (instTBLLine.isEmpty) eofInst()
      else readInst()
      while (instEOF == "N") {
        if (instTBLLine.hasNext) readInst()
        else eofInst()
      }
    }

    //-----------------------------------------------------------------------------------------
    // Begin Interactive Loop
    //-----------------------------------------------------------------------------------------

    println("*" * 100 )
    var screenPrompt = "I" //initialize to show Instrument Report first
    var screenLines = 0
    while (screenPrompt != "N") {

      //-----------------------------------------------------------------------------------------
      // Display Memory Usage
      //-----------------------------------------------------------------------------------------

      if (screenPrompt == "M") {
        // memory info
        val mb = 1024 * 1024
        val runtime = Runtime.getRuntime
        println("")
        println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
        println("** Free Memory:  " + runtime.freeMemory / mb)
        println("** Total Memory: " + runtime.totalMemory / mb)
        println("** Max Memory:   " + runtime.maxMemory / mb)
        println("")
      }

      //-----------------------------------------------------------------------------------------
      // Display Instruments
      //-----------------------------------------------------------------------------------------

      if (screenPrompt == "I") {
        if (loadInstRecs == "Y") {
          // Print Top Value Instrument IDs and Balances with Instrument Names
          println("*************** Top Instrument Amount Table ************************")
          println("Inst. ID \tHolder Name \tAmmount")
          for (x <- topValAmtInstMap) {
            for (x1 <- topValAmtInstMap(x._1)) println(x1 + "\t" + {try instMap(x1).instHolderName
            catch {case e: NoSuchElementException => "Name not found"}}
              + "\t" + formatter.format(x._1))
          }
          println("Table Count: " + formatter.format(topValAmtInstMap.size))
        }
        else { // Print Top Value WITHOUT Instrument Names
          println("*************** Top Instrument Amount Table ************************")
          println("Instrument ID \tAmmount")
          for (x <- topValAmtInstMap) {
            for (x1 <- topValAmtInstMap(x._1)) println(x1.toString + "\t" + formatter.format(x._1))
          }
          println("Table Count: " + formatter.format(topValAmtInstMap.size))
        }
      }

      // aggregate by instrument type, both Revenues and Expenses
      if (screenPrompt == "A.1") {
        if (loadInstRecs == "Y") {
          // Print Top Value Instrument IDs and Balances with Instrument Names
          println("*************** All Instrument Amount Table ************************")
          println("Legal Entity \tLegal Entity Name \tAmmount")
          for {(key, xs) <-
               allBalancesTree.groupBy(_._2.ballegalEntityID)
            //allBalancesTree.groupBy(_._2.balaccountID.substring(0,3))
            //{try instMap(allBalancesTree.groupBy(_._1)).instHolderName
            //catch {case e: NoSuchElementException => "Name not found"}}
               x = xs.map(_._2.baltransAmount.toDouble).sum}
            println(key + "\t" + {
              try {leTBL(key)._2} catch {case e: NoSuchElementException => "Name not found"}}
              + "\t" + formatter.format(x))
          println("Table Count: " + formatter.format(allBalancesTree.size))
        }
//               {
//            for (x1 <- allBalancesTree(x._1)) println(x1 + "\t" + {try instMap(x1).instHolderName
//            catch {case e: NoSuchElementException => "Name not found"}}
//              + "\t" + formatter.format(x._1))
//          }
        else { // Print Top Value WITHOUT Instrument Names
          println("*************** All Instrument Amount Table ************************")
          println("Legal Entity \tLegal Entity Name \tAmmount")
          for {(key, xs) <- allBalancesTree.groupBy(_._2.ballegalEntityID)
            x = xs.map(_._2.baltransAmount.toDouble).sum}
            println(key + "\t" + {
              try {leTBL(key)._2} catch {case e: NoSuchElementException => "Name not found"}}
              + "\t" + formatter.format(x))
          println("Table Count: " + formatter.format(allBalancesTree.size))
        }
      }
      // aggregate by instrument type, both Revenues and Expenses
      if (screenPrompt == "A.2") {
        if (loadInstRecs == "Y") {
          // Print Top Value Instrument IDs and Balances with Instrument Names
          println("*************** All Instrument Amount Table ************************")
          println("Legal Entity \tLegal Entity Name \tAmmount")
          for {(key, xs) <- allBalancesTree.groupBy( rows => ( rows._2.ballegalEntityID,
            try instMap(rows._2.balinstID).instTypeID catch {case e: NoSuchElementException => "Unknown"}))
               x = xs.map(_._2.baltransAmount.toDouble).sum}
            println(key + "\t" + {
              try {leTBL(key._1)._2} catch {case e: NoSuchElementException => "Name not found"}}
              + "\t" + formatter.format(x))
          println("Table Count: " + formatter.format(allBalancesTree.size))
        }
        else { // Print Top Value WITHOUT Instrument Names
          println("*************** All Instrument Amount Table ************************")
          println("Legal Entity \tLegal Entity Name \tAmmount")
          for {(key, xs) <- allBalancesTree.groupBy( rows => ( rows._2.ballegalEntityID, rows._2.balaccountID.substring(0,3)))
               x = xs.map(_._2.baltransAmount.toDouble).sum}
            println(key + "\t" + {
              try {leTBL(key._1)._2} catch {case e: NoSuchElementException => "Name not found"}}
              + "\t" + formatter.format(x))
          println("Table Count: " + formatter.format(allBalancesTree.size))
        }
      }

      if (screenPrompt == "1") {
        println("*************** Accumulated Legal Entity Amounts: All Instruments ************************")
        println("Legal Entity ID" + "\t" + "LE Name" + "\t" + "Amount")
        for {(key, xs) <- aggAllSmallTBL.groupBy(_._1.agglegalEntityID)
             x = xs.map(_._2.toDouble).sum}
          println(key + "\t" + {try {leTBL(key)._2} catch {case e: NoSuchElementException => "Name not found"}}
            + "\t" + formatter.format(x))
        println("Table Count: " + aggAllSmallTBL.size)
      }

      if (screenPrompt == "2") {
        println("*************** Accumulated Center  Amounts: All Instruments ************************")
        println("Center Value" + "\t" + "Center Name" + "\t" + "Amount")
        for {(key, xs) <- aggAllSmallTBL.groupBy(_._1.aggcenterID)
             x = xs.map(_._2.toDouble).sum}
          println(key + "\t" + {try {centerTBL(key)._2} catch {case e: NoSuchElementException => "Name not found"}}
            + "\t" + formatter.format(x))
        println("Table Count: " + aggAllSmallTBL.size)
      }

      if (screenPrompt == "3") {
        println("*************** Accumulated Project  Amounts: All Instruments ************************")
        println("Project" + "\t" + "Project Name" + "\t" + "Amount")
        for {(key, xs) <- aggAllSmallTBL.groupBy(_._1.aggprojectID)
             x = xs.map(_._2.toDouble).sum}
          println(key + "\t" + {try {projectTBL(key)._2} catch {case e: NoSuchElementException => "Name not found"}}
            + "\t" + formatter.format(x))
        println("Table Count: " + aggAllSmallTBL.size)
      }

      if (screenPrompt == "4") {
        println("*************** Accumulated Project  Amounts: All Instruments ************************")
        println("Account" + "\t" + "Account Name" + "\t" + "Amount")
        for {(key, xs) <- aggAllSmallTBL.groupBy(_._1.aggaccountID)
             x = xs.map(_._2.toDouble).sum}
          println(key + "\t" + {try {acctTBL(key)._2} catch {case e: NoSuchElementException => "Name not found"}}
            + "\t" + formatter.format(x))
        println("Table Count: " + aggAllSmallTBL.size)
      }

      if (screenPrompt == "D") {
        println(aggAllSmallTitleTabRow)
        for (x <- aggAllSmallTBL) {
          println(x._1.aggledgerID + "\t" +
            x._1.aggledgerID + "\t" +
            x._1.aggjrnlType + "\t" +
            x._1.agglegalEntityID + "\t" +
            {
              try {
                leTBL(x._1.agglegalEntityID)._2
              } catch {
                case e: NoSuchElementException => "Name not found"
              }
            }
            + "\t" +
            x._1.aggcenterID + "\t" +
            {
              try {
                centerTBL(x._1.aggcenterID)._2
              } catch {
                case e: NoSuchElementException => "Name not found"
              }
            }
            + "\t" +
            x._1.aggprojectID + "\t" +
            {
              try {
                projectTBL(x._1.aggprojectID)._2
              } catch {
                case e: NoSuchElementException => "Name not found"
              }
            }
            + "\t" +
            x._1.aggproductID + "\t" + "\t" +
            x._1.aggaccountID + "\t" +
            {
              try {
                acctTBL(x._1.aggaccountID)._2
              } catch {
                case e: NoSuchElementException => "Name not found"
              }
            }
            + "\t" +
            formatter.format(x._2.toDouble)
          )
          println("Table Count: " + aggAllSmallTBL.size)
        }
      }
//      println("Grand Total: " + formatter.format(aggAllSmallTBL.foldLeft(0.00)(_+_._2.toDouble)))
      println("*" * 100 )
      println("D=Detailed Values All Inst, I=Instrument Accumulation, A=Aggregate by Instrument Type")
      println("1=By Legal Entity, 2=By Center, 3=By Project, 4=Account")
      println("Display another data cut? (Y/N) M for Memory Use, q to quit")
      screenPrompt = scala.io.StdIn.readLine().trim.toUpperCase()
      if (screenPrompt.substring(0,1) == "Q") sys.exit(0)
    }

    //-----------------------------------------------------------------------------------------
    // Prompt for final processes
    //-----------------------------------------------------------------------------------------
    println("*" * 100 )
    println("Would you like to save Aggregate and Summary Top Instrument Tables? (Y/N) ")
    var saveTopValTables = scala.io.StdIn.readLine().trim
    if (saveTopValTables.substring(0,1) == "q") sys.exit(0)

    //*****************************************************************************************
    // Write Summary Structures
    //*****************************************************************************************
    if (saveTopValTables == "Y" || saveTopValTables == "y") {
      sumWritten = writeSum()
      aggWritten = writeAgg()
    }

    //*****************************************************************************************
    // Close files
    //*****************************************************************************************
    newSumTBLwriter.close()
    newAggTBLwriter.close()

    //*****************************************************************************************
    // Control Report
    //*****************************************************************************************

    println("*" * 100 )
    println("Accumulation Program Control Report")
    println("While Loops:                          " + formatter.format(counter))
    println("Years Loaded:                         " + loadYears)
    println("Starting with Year:                   " + startYears)
    println("Balances Read:                        " + formatter.format(balRead))
    println("Instruments Read:                     " + formatter.format(instRead))
    println("Legal Entities Read                   " + formatter.format(agencyRead))
    println("Centers Read:                         " + formatter.format(fundRead))
    println("Projects Read:                        " + formatter.format(programRead))
    println("Expense Accounts Read:                " + formatter.format(objectRead))
    println("Revenue Accounts Read:                " + formatter.format(sourceRead))
    println("Top Instruments Detected:             " + formatter.format(topTableMax))
    println("Summary Top Inst Balances Written:    " + formatter.format(sumWritten))
    println("All Instr. Aggregate Records Written: " + formatter.format(aggWritten))
    println("*" * 100 )

    def writeSum(): Int = {

      newSumTBLwriter.write(stdBalTitleCSVRow + "\n")
      // traverse Top Value Inst. tables to find all balances for Top Inst.
      var sumCount = 0
      val topValAmtSorted = topValAmounts.sortWith(_<_)
      for (amt <- topValAmtSorted) {
        var amtInstIDs: Array[String] = topValAmtInstMap(amt).toArray
        for (topInstID: String <- amtInstIDs) {
          val topValBalKeys: Array[(String, String)] = allBalancesTree.keySet.from((topInstID, "")).to((topInstID, "9999999999")).toArray
          for (x1: (String, String) <- topValBalKeys) {
            val x = allBalancesTree(x1._1, x1._2)
            newSumTBLwriter.write(
              x.balinstID + "," +
                x.balbalID + "," +
                x.balledgerID + "," +
                x.baljrnlType + "," +
                x.balbookCodeID + "," +
                x.ballegalEntityID + "," +
                x.balcenterID + "," +
                x.balprojectID + "," +
                x.balproductID + "," +
                x.balaccountID + "," +
                x.balcurrencyCodeSourceID + "," +
                x.balcurrencyTypeCodeSourceID + "," +
                x.balcurrencyCodeTargetID + "," +
                x.balcurrencyTypeCodeTargetID + "," +
                x.balfiscalYear + "," +
                formatter.format(x.baltransAmount.toDouble) + "," +
                x.balmovementFlg + "," +
                x.balunitOfMeasure + "," +
                x.balstatisticAmount
                + "\n"
            )
            sumCount += 1
          }
        }
      }
      sumCount
    }

    def writeAgg(): Int = {
      for (k <- allBalancesTree.values) {
                var w = aggBalCLS(
                  k.balledgerID,
                  k.baljrnlType,
                  k.balbookCodeID,
                  k.ballegalEntityID,
                  k.balcenterID,
                  k.balprojectID,
                  k.balproductID,
                  k.balaccountID,
                  k.balcurrencyCodeSourceID,
                  k.balcurrencyTypeCodeSourceID,
                  k.balcurrencyCodeTargetID,
                  k.balcurrencyTypeCodeTargetID,
                  k.balfiscalYear)

        if (aggAllKeyTBLByYear contains w) {
          aggAllKeyTBLByYear.+=(w -> (aggAllKeyTBLByYear(w).toDouble + k.baltransAmount.toDouble).toString)
        }
        else aggAllKeyTBLByYear.+=(w -> k.baltransAmount)
      }

      newAggTBLwriter.write(aggBalTitleCSVRow + "\n")
      for ((k, v) <- aggAllKeyTBLByYear) {
        newAggTBLwriter.write(
          k.aggledgerID           + "," +
            k.aggjrnlType           + "," +
            k.aggbookCodeID         + "," +
            k.agglegalEntityID      + "," +
            k.aggcenterID           + "," +
            k.aggprojectID          + "," +
            k.aggproductID          + "," +
            k.aggaccountID          + "," +
            k.aggcurrencyCodeSourceID  + "," +
            k.aggcurrencyTypeCodeSourceID   + "," +
            k.aggcurrencyCodeTargetID  + "," +
            k.aggcurrencyTypeCodeTargetID   + "," +
            k.aggfiscalYear + "," +
            formatter.format(v.toDouble) //amount element of map
            + "\n")

      }
      aggAllKeyTBLByYear.iterator.size
    }
 
  }

}
