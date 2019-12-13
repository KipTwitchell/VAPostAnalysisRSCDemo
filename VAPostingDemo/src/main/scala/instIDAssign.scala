import scala.io.Source
import java.io.{File, FileNotFoundException, IOException, PrintWriter}

//###########################################################################
//  Consolidated Data Supply Chain (CDSC) Demo System
//
//  Program:  instIDAssign - Instrument ID Assignment
//  This program reads:
//    - A transaction file
//    - an Instrument ID table, containing keys and the associated Instrument ID for each key (vendor name)
//    - A Max Key table, containing the highest key assigned to any Instrument
//
//  It writes:
//    - A newly formatted transaction file, with the associated Instrument ID assigned on the front
//    - an updated Instrument ID file, with any newly discovered Instruments (vendors)
//    - An updated Max Key Table, with the new highest used Key
//    - An Instrument Update Transaction file, containing any newly assigned Instruments
//
//	(c) Copyright IBM Corporation. 2017
//  SPDX-License-Identifier: Apache-2.0
//     Kip Twitchell <finsysvlogger@gmail.com>.
//  Created July 2017
//
//  Change Log:
//###########################################################################

object instIDAssign {
  def main(args: Array[String]): Unit = {

    case class stdTransCLS(
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


    println(" ")
    println("******************************************")
    println("*** Stage 1 Inst ID Assignment Program ***")
    println("******************************************")
    println(" ")

    //args.foreach(println)

    // ------------------ uncomment below lines when using in PASSED ARGS parameters
        println("Using Passed Parameters")
    val filePaths = Map("inputPath" -> args(0), "outputPath" -> args(1))
    val fileNames = Map("maxIDKeyFileName" -> args(2), "instIDFileName" -> args(3),
      "tranTBLFileName" -> args(4), "instUPDFileName" -> args(5))
    // ------------------ uncomment above lines when using PASSED ARGS program parameters

    // ------------------ uncomment below lines when using IN PROGRAM file parameters
//    var filePaths = Map("inputPath" -> "/Users/ktwitchell001/workspace/VADataDemo/VATestData/", "outputPath" ->
//      "/Users/ktwitchell001/workspace/VADataDemo/VATestData/")
//    var fileNames = Map("maxIDKeyFileName" -> "VATestMaxID.txt",
//      "instIDFileName" -> "VATestInstID.txt",
//      "balTBLFileName" -> "VATestBalTBL.txt",
//      "tranTBLFileName" -> "VATestTranTBL.txt", // this file is a temp file used as input to sort for transactions
//      "instUPDFileName" -> "VATestInstUPD.txt")
    // ------------------ uncomment above lines when using IN PROGRAM file parameters

    //val debugPrint = "N"

    //*****************************************************************************************
    // Open Instrument, and Transaction (STDin stream) Files
    //*****************************************************************************************
    // instrument file
    val instX = Source.fromFile(filePaths("inputPath") + fileNames("instIDFileName")).getLines()
    val instIDLine = instX.buffered

    // transaction file
    // the Pipe Parameter passed to this program is not used:  I couldn't figure out how to put the file attributes in
    // an if statement and have them be referenceable.
    //   transaction file from STREAM
    // ------------------ uncomment below lines to read PIPE, not fil
    val tranStreamX = Source.fromInputStream(System.in).getLines() // Pipe version of input
        val transLine = tranStreamX.buffered
        println("Processing from Pipe")
    // ------------------ uncomment above lines to read PIPE, not file

    // transaction file from FILE
    // ------------------ uncomment below lines to read FILE, not pipe
//    var tranFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + "sortFY15q2exp.txt").getLines()
//    val transLine: BufferedIterator[String] = tranFileX.buffered
//    println("Module configured for File")
    // ------------------ uncomment above lines to read FILE, not pipe

    //*****************************************************************************************
    // Process MaxKey
    //*****************************************************************************************
    // read MaxKey ID file.  Read all records because file is small and completely rewritten.
    // hold file open with lock until process is finished to control concurrency.
    // desired header record removed because I couldn't do the right kind of IO
    // record structure in header row is "SysID,InstID,BalID,TranID,TimeStamp,Comment"

    var maxInstID: Int = 0 //counter used to increment to new instrument IDs

    val maxKeySave = Source.fromFile(filePaths("inputPath") + fileNames("maxIDKeyFileName")).getLines.mkString
    val Array(maxKeySysID, maxKeyInstID, maxKeyBalID, maxKeyTranID, maxKeyTimeStamp, maxKeyComment) =
      maxKeySave.split(",").map(_.trim)
    if (maxKeySysID == "1") {
      maxInstID = maxKeyInstID.toInt
      // if (debugPrint == "Y") println(maxInstID)
    }

    val startMaxInstID = maxKeyInstID.toInt

    //*****************************************************************************************
    // Open output files
    //*****************************************************************************************
    // max ID record
    val newMaxIDwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("maxIDKeyFileName")))
    // instrument record
    val newInstIDwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("instIDFileName")))
    // tran record
    val newTranwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("tranTBLFileName")))
    // instrument updates
    val newInstUPDwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("instUPDFileName")))

    var instIDWritten = 0
    var transWritten = 0
    var instUPDWritten = 0

    //*****************************************************************************************
    // Perform Initial Reads
    //*****************************************************************************************
    //-----------------------------------------------------------------------------------------
    // Instrument ID File
    //-----------------------------------------------------------------------------------------
    // input instrument ID file
    case class instIDCLS(instIDVendorName: String, instIDVendorID: String)

    case class transCLS(tranVendorName: String, tranAgency: String, tranFund: String, tranProg: String, tranObj: String, tranAmt: String,
                        tranFisDt: String, tranAcctDt: String, tranTranDt: String, tranType: String)

    var instIDRec = instIDCLS("","")
    var testinstVendorName: String = " "
    var testinstVendorID: String = " "
    var instRead = 0


    def readInst(): Unit = {
      val e: Array[String] = instIDLine.next.split(',')
      instIDRec = instIDCLS(e(0), e(1))
      // if (debugPrint == "Y") println("inst vendor: " + instIDRec.instIDVendorName + " inst row: " + instIDRec)
      testinstVendorName = instIDRec.instIDVendorName
      testinstVendorID = instIDRec.instIDVendorID
      instRead +=1
    }
    var instEOF = "N"
    if (instIDLine.isEmpty) {
      // if (debugPrint == "Y") println("inst high values. EOF Inst")
      testinstVendorName = "𝖛" * testinstVendorName.length //this is a hack, a higher value constant would be good
      instEOF = "Y"
    }
    else readInst()

    //-----------------------------------------------------------------------------------------
    // Trans file
    //-----------------------------------------------------------------------------------------
    var transRec = transCLS("","","","","","","","","","")
    var testtranVendorName: String = " "
    var testInstType: String = ""
    var testInstEffectDate: String = ""
    var transRead = 0

    def readTran(): Unit = {
      val e: Array[String] = transLine.next.split(',')
//      transRec = transCLS(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9))
//      e.foreach(x => println(x))

      transRec = transCLS(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7),
        //convert tran date from dd-mmm-yy to ccyy/mm/dd
        if (e(8).substring(2,3) != "-")  e(8)
        else {
          "20" + e(8).substring(7, 9) + "/" + Map(
            "JAN" -> "01", "FEB" -> "02", "MAR" -> "03", "APR" -> "04",
            "MAY" -> "05", "JUN" -> "06", "JUL" -> "07", "AUG" -> "08",
            "SEP" -> "09", "OCT" -> "10", "NOV" -> "11", "DEC" -> "12")
            .apply(e(8).substring(3, 6)) + "/" + e(8).substring(0, 2)},
        e(9))
      // if (debugPrint == "Y") println("trans vendor: " + transRec.tranVendorName + " trans row: " + transRec)
      testtranVendorName = transRec.tranVendorName
      transRead += 1
    }

    var tranEOF = "N"
    if (transLine.isEmpty) {
      // if (debugPrint == "Y") println("tran file is empty")
      testtranVendorName = "𝖛" * testtranVendorName.length
      tranEOF = "Y"
    }
    else readTran()


    val stdTransRec = stdTransCLS("0", "0", "1", " ", "ACTUALS", "FIN", "SHRD-3RD-PARTY", "0", "0", "0",
      "0", "0", "USD", "TXN", "USD", "BASE-LE", "0", "0", "0", "0", "O", "N", "N", "N",
      " ", " ", " ", " ", " ", " ", " ")

    //*****************************************************************************************
    // process variables for while loop in file processing
    //*****************************************************************************************
    var swapInst = "N"
    var saveVendorName: String = ""
    var saveVendorID: String = ""
    var saveInstType: String = ""
    var saveInstEffectDate: String = ""

    var lastInstWriteflg = "N"
    var counter: Int = 0
    var transRowCounter = 0

    //*****************************************************************************************
    // Mainline Routine
    //  test trans = master
    //      if they are equal then write a new tran record with master key
    //  else
    //  test trans < master
    //      this means that the tran is for a master that hasn't been written yet, so
    //      save the last master, set a flag for the save, and then use the tran record to make new master
    //  else
    //  test trans > master
    //      this means that the last master is done.  Write old master record.
    //      Test save flag and either use saved record, or read a new record
    //
    //  if the instr file is shorter than the trans file, the last instrument does not get written within the loop.
    //
    //*****************************************************************************************
    while (tranEOF == "N" || instEOF == "N") {
      if (testtranVendorName == testinstVendorName) {
        // if (debugPrint == "Y") {println("==: tran: " + testtranVendorName + " inst: " + testinstVendorName + " remaining rec: " + transCLS.toString())}
        // matching instrument record found, format transaction record
        formatWriteTran()
        // read next transaction record
        if (tranEOF == "N") {if (transLine.hasNext) {readTran()}
          else {eofTran()}
        }
      }
      else {
        if (testtranVendorName < testinstVendorName) {
          // if (debugPrint == "Y") {println("<<: tran: " + transRec.tranVendorName + " inst: " + testinstVendorName + " Max ID: " + maxInstID.toString)}
          // new vendor found.  test if new vendor is equal to already read and saved inst record from inst file
          if (swapInst == "Y" && testtranVendorName >= saveVendorName) {useSaveVendor()}
          else {
            if (swapInst == "N") {
              // save prior vendor
              swapInst = "Y"
              saveVendorName = testinstVendorName
              saveVendorID = instIDRec.instIDVendorID
            }
            formatInstwTran()  // build new inst record
          }
        }
        else {
          if (testtranVendorName > testinstVendorName) {
            // finished with instrument record
            // if (debugPrint == "Y") println(">>: after tran: " + transRec.tranVendorName + " inst: " + testinstVendorName)
            // finish old inst and write to output file
            writeInstID()
            // test if new vendor is equal to already read and saved inst record from inst file
            if (swapInst == "Y" && (testtranVendorName >= saveVendorName || tranEOF == "Y")) {
              useSaveVendor() // then return to test and write trans record for this inst.
            }
            else {
              if (swapInst == "Y") {formatInstwTran()} // build new inst record without replacing saved instrument from read ahead
              else {
                // read new instrument
                if (instEOF == "N") {if (instIDLine.hasNext) {readInst()}
                  else {eofInst()}
                }
              }
            }
          }
          else {println("an evaluation error has occurred!  Files might be out of sort order")}
        }
      }
      counter += 1
      // if (debugPrint == "Y") println("counter val: " + counter)
    }

    // if instrument file is shorter than trans file, then have to complete last write of insturment
    if (lastInstWriteflg == "Y") {
      writeInstID()
      // if (debugPrint == "Y") println("Finish extra instrument")
      lastInstWriteflg = "N"
    }

    //*****************************************************************************************
    // Write max ID record
    //*****************************************************************************************
    // insert new updated max ID value
    newMaxIDwriter.write(maxKeySysID + "," + 0.toString * (10 - maxInstID.toString.length) + maxInstID.toString
      //left fill with zeros to 10 digits
      + "," + maxKeyBalID + "," + maxKeyTranID + "," + maxKeyTimeStamp + "," + maxKeyComment)

    //*****************************************************************************************
    // Close files
    //*****************************************************************************************
    newMaxIDwriter.close()
    newInstIDwriter.close()
    newTranwriter.close()
    newInstUPDwriter.close()

    //*****************************************************************************************
    // Control Report
    //*****************************************************************************************

    println("*****************************************")
    println("Inst ID Assignment Program Control Report")
    println("While Loops:                " + counter)
    println("Instruments Read:           " + instRead)
    println("Transactions Read:          " + transRead)
    println("Instruments Written:        " + instIDWritten)
    println("Instrument Updates Written: " + instUPDWritten)
    println("Transactions Written:       " + transWritten)
    println("Instrument IDs Assigned:    " + (maxInstID - startMaxInstID))
    println("*****************************************")

    //*****************************************************************************************
    //*****************************************************************************************
    // End of Main def drop through logic
    //*****************************************************************************************
    //*****************************************************************************************

    //*****************************************************************************************
    // Common Routines
    //*****************************************************************************************

    // Used the Saved vendor from prior read
    def useSaveVendor(): Unit = {
      swapInst = "N"
      testinstVendorName = saveVendorName
      testinstVendorID = saveVendorID
      testInstType = saveInstType
      testInstEffectDate = saveInstEffectDate
    }

    // format and write output tran
    def formatWriteTran(): Unit = {
      transRowCounter += 1
      val stdTransRecOut = stdTransRec.copy(transinstID = testinstVendorID,
        transjrnlID = "JE" + transRec.tranType + transRec.tranAcctDt + "-" + transRowCounter.toString,
        transjrnlLineID = "1",
        transjrnlDescript = "\"Orig. Trans. from " + transRec.tranType + " " + transRec.tranFisDt + " file\"",
        translegalEntityID = transRec.tranAgency,
        transcenterID = transRec.tranFund,
        transprojectID = transRec.tranProg,
        transaccountID = transRec.tranType + transRec.tranObj,
        transtransAmount = transRec.tranAmt,
        transfiscalPeriod = transRec.tranFisDt,
        transacctDate = transRec.tranAcctDt,
        transtransDate = transRec.tranTranDt,
        transdirVsOffsetFlg = "D",
        transreconcileFlg = "Y")
      // write transaction
      val tranList = List(stdTransRecOut)
      tranList.foreach(e => newTranwriter.write(e.productIterator.mkString(",") + "\n"))
//      newTranwriter.write(stdTransRecOut.toString + "\n")
      // if (debugPrint == "Y") println("Final output tran: " + stdTransRecOut.toString)
      transWritten += 1
    }

    def writeInstID(): Unit = {
      newInstIDwriter.write(testinstVendorName + "," + testinstVendorID + "\n")
      // if (debugPrint == "Y") println("Write instrument: " + transRec.tranVendorName + " inst: " + testinstVendorName)
      instIDWritten += 1
    }

    def formatInstwTran(): Unit = {
      testinstVendorName = testtranVendorName
      maxInstID += 1 // increment max instrument ID field
      testinstVendorID = 0.toString * (10 - maxInstID.toString.length) + maxInstID.toString //left fill with zeros to 10 digits
      testInstType = transRec.tranType
      testInstEffectDate = transRec.tranTranDt
      writeInstUPD() // a new instrument as been detected.  Write an update transaction as well, to update
                     // actual instrument table in the next program
    }

    def writeInstUPD(): Unit = {
      newInstUPDwriter.write(
        testinstVendorID + ","  +
        testinstVendorName + "," +
        testInstType + ","  +
        testInstEffectDate +
        "\n")
      // if (debugPrint == "Y") println("Write update instrument: " + transRec.tranVendorName + " inst: " + testinstVendorName)
      instUPDWritten += 1
    }

    def eofTran(): Unit = {
      // if (debugPrint == "Y") println("tran high values; EOF tran")
      testtranVendorName = "𝖛" * testtranVendorName.length
      tranEOF = "Y"
    }

    def eofInst(): Unit = {
      // if (debugPrint == "Y") println("inst high values. EOF Inst")
      testinstVendorName = "𝖛" * testinstVendorName.length //this is a hack, a higher value constant would be good
      instEOF = "Y"
      if (tranEOF == "N") lastInstWriteflg = "Y"
    }
  }
}
