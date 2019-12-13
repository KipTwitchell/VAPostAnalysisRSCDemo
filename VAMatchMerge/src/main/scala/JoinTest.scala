

import java.io.{File, PrintWriter}
import scala.io.Source
//import com.ibm.jzos.ZFile

//###########################################################################
//  Consolidated Data Supply Chain (CDSC) Demo System
//
//  Program:  JoinTest - Join Transaction and Vendor Master, and write combined file
//  This program reads:
//    - A transaction file
//    - Instrument table
//
//  It writes:
//    - A combined File
//
//	(c) Copyright IBM Corporation. 2017
//  SPDX-License-Identifier: Apache-2.0
//     Kip Twitchell <finsysvlogger@gmail.com>.
//  Created July 2017
//###########################################################################

//###########################################################################
//  to dos:
//###########################################################################


object JoinTest {
//  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {

    case class stdTransCLSPost(
                                // Element      Data Type                Default Value     KEY col Description
                                transinstID: String, // "0",         X  1  Instrument ID
                                transjrnlID: String, // "0",            2  Business Event / Journal ID
                                transjrnlLineID: String, // "1",            3  Jrnl Line No
                                transjrnlDescript: String, // " ",            4  Journal / Event Description
                                transledgerID: String, // "ACTUALS",   X  5  Ledger
                                transjrnlType: String, // "FIN",       X  6  Jrnl Type
                                transbookCodeID: String, // "SHRD-3RD-PARTY", X  7  Book-code / Basis
                                translegalEntityID: String, // "STATE-OF-VA"X  8  Legal Entity (CO. / Owner)
                                transcenterID: String, // "0",         X  9  Center ID
                                transprojectID: String, // "0",         X  10 Project ID
                                transproductID: String, // "0",         X  11 Product / Material ID
                                transaccountID: String, // "0",         X  12 Nominal Account
                                transcurrencyCodeSourceID: String, // "USD",       X  13 Curr. Code Source
                                transcurrencyTypeCodeSourceID: String, // "TXN",       X  14 Currency Type Code Source
                                transcurrencyCodeTargetID: String, // "USD",       X  15 Curr. Code Target
                                transcurrencyTypeCodeTargetID: String, // "BASE-LE",   X  16 Currency Type Code Target
                                transtransAmount: String, // "0",            17 Transaction Amount
                                transfiscalPeriod: String, // "0",         X  18 Fiscal Period
                                transacctDate: String, // "0",               Acctg. Date
                                transtransDate: String, // "0",               Transaction Date
                                transdirVsOffsetFlg: String, // "O",               Direct vs. Offset Flag
                                transreconcileFlg: String, // "N",               Reconciliable Flag
                                transadjustFlg: String, // "N",               Adjustment Flag
                                transmovementFlg: String, // "N",               Movement Flag
                                transunitOfMeasure: String, // " ",               Unit of Measure
                                transstatisticAmount: String, // " ",               Statistical Amount
                                transextensionIDAuditTrail: String, // " ",               Audit Trial Extension ID
                                transextensionIDSource: String, // " ",               Source Extension ID
                                transextensionIDClass: String, // " ",               Classification Extension ID
                                transextensionIDDates: String, // " ",               Date Extension ID
                                transextensionIDCustom: String) // " "                Other Customization Ext ID


    case class stdInstCLS(
                           // Element      Data Type                Default Value       Description
                           instinstID: String, // "0",               Instrument ID
                           instEffectDate: String, // "0000-00-00",      Record Effective Start Date
                           instEffectTime: String, // "0",               Record Effective Start Time
                           instEffectEndDate: String, // "9999-99-99",      Record Effective End Date
                           instEffectEndTime: String, // "9",               Record Effective End Time
                           instHolderName: String, // "Vendor Name",     Name of Instrument Holder
                           instTypeID: String, // "EXP",             Vendor (EXP) or REV Record
                           instAuditTrail: String) // "0,                Timestamp Updated

    //      case class stdBalCLS(
    //                            // Element      Data Type                Default Value       Description
    //                            balinstID: String,                     // "0",               Instrument ID
    //                            balbalID: String,                      // "0",               Balance ID
    //                            balledgerID: String,                   // "ACTUALS",         Ledger
    //                            baljrnlType: String,                   // "FIN",             Jrnl Type
    //                            balbookCodeID: String,                 // "SHRD-3RD-PARTY",  Book-code / Basis
    //                            ballegalEntityID: String,              // "STATE-OF-VA",     Legal Entity (CO. / Owner)
    //                            balcenterID: String,                   // "0",               Center ID
    //                            balprojectID: String,                  // "0",               Project ID
    //                            balproductID: String,                  // "0",               Product / Material ID
    //                            balaccountID: String,                  // "0",               Nominal Account
    //                            balcurrencyCodeSourceID: String,       // "USD",             Curr. Code Source
    //                            balcurrencyTypeCodeSourceID: String,   // "TXN",             Currency Type Code Source
    //                            balcurrencyCodeTargetID: String,       // "USD",             Curr. Code Target
    //                            balcurrencyTypeCodeTargetID: String,   // "BASE-LE",         Currency Type Code Target
    //                            balfiscalYear: String,                 // "0",               Fiscal Year - main bucket key
    //                            baltransAmount: String,                // "0",               Transaction Amount
    //                            balmovementFlg: String,                // "N",               Movement Flag
    //                            balunitOfMeasure: String,              // " ",               Unit of Measure
    //                            balstatisticAmount: String)            // " ",               Statistical Amount
    //
    //


    println(" ")
    println("********************************")
    println("*** Join Test Program! ***")
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

    //------------------ un comment below lines when using IN PROGRAM file parameters
//    var filePaths = Map("inputPath" -> "/ngsafr/VAMatchMerge/data/", "outputPath" ->
//      "/ngsafr/temp/")
//    var fileNames = Map("maxIDKeyFileName" -> "VATestMaxID.txt",
//      "instTBLFileName" -> "InstTBLFixSmall.txt",
//      "balTBLFileName" -> "VATestBalTBL.txt",
//      "tranTBLFileName" -> "TranTBLFixSmall.txt", // this file is a temp file used as input to sort for transactions
//      "instUPDFileName" -> "VATestInstUPD.txt",
//      "sumTBLFileName" -> "VATestSumTBL.txt")
    // ------------------ uncomment above lines when using IN PROGRAM file parameters

    //val debugPrint = "N"

    //*****************************************************************************************
    // Open Files
    //*****************************************************************************************
    // balance file
    //      val balX = Source.fromFile(filePaths("inputPath") + fileNames("balTBLFileName")).getLines()
    //      val balTBLLine = balX.buffered

    // transaction file
    // the Pipe Parameter passed to this program is not used:  I couldn't figure out how to put the file attributes in
    // an if statement and have them be referenceable.
    //   transaction file from STREAM
    // ------------------ transaction uncomment below lines to read PIPE, not file
    //      val tranStreamX = Source.fromInputStream(System.in).getLines() // Pipe version of input
    //      val transLine = tranStreamX.buffered
    //      println("Processing transactions from Pipe")
    // ------------------ transaction uncomment above lines to read PIPE, not file

    //  transaction file from FILE
    // ------------------ transaction uncomment below lines to read FILE, not pipe
    //    var tranFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + "T103q1exp.txt").getLines()
    var tranFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + fileNames("tranTBLFileName")).getLines()
    val transLine: BufferedIterator[String] = tranFileX.buffered
    println("Module configured for transaction File" + filePaths("inputPath") + fileNames("tranTBLFileName"))
    // ------------------ transaction uncomment above lines to read FILE, not pipe

    // instrument UPD transaction file from FILE
    //    var instUPDFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + "U103q1exp.txt").getLines()
    //      val instUPDFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + fileNames("instUPDFileName")).getLines()
    //      val instUPDLine: BufferedIterator[String] = instUPDFileX.buffered
    //      println("Module configured for inst UPD File")

    // instrument table file from FILE
    //        var instUPDFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + "VATestU13q1exp.txt").getLines()
    val instTBLFileX: Iterator[String] = Source.fromFile(filePaths("inputPath") + fileNames("instTBLFileName")).getLines()
    val instTBLLine: BufferedIterator[String] = instTBLFileX.buffered
    println("Module configured for inst TBL File")


    //*****************************************************************************************
    // Open output files
    //*****************************************************************************************
    // summary view table
    val newSumTBLwriter = new PrintWriter(new File(filePaths("outputPath") + "new" + fileNames("sumTBLFileName")))

    var sumWritten = 0


    //*****************************************************************************************
    // Perform Initial Reads
    //*****************************************************************************************
    //-----------------------------------------------------------------------------------------
    // Instrument File
    //-----------------------------------------------------------------------------------------

    var instRec = stdInstCLS("", "", "", "", "", "", "", "")
    var testInstFullKey: String = " "
    var testInstInstID: String = " "
    var instRead = 0

    def readInst(): Unit = {
      //        val e: Array[String] = instTBLLine.next.split(',')

      val e: Array[String] = new Array[String](8)
      if (instTBLLine.hasNext) {
        val instTBLLineStrValue = instTBLLine.next()
        e(0) = instTBLLineStrValue.substring(0, 10)
        e(1) = instTBLLineStrValue.substring(11, 21)
        e(2) = instTBLLineStrValue.substring(22, 23)
        e(3) = instTBLLineStrValue.substring(24, 34)
        e(4) = instTBLLineStrValue.substring(35, 36)
        e(5) = instTBLLineStrValue.substring(37, 67)
        e(6) = instTBLLineStrValue.substring(68, 71)
        e(7) = instTBLLineStrValue.substring(72, 73)
      }

      instRec = stdInstCLS(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7)) //19 elements
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

    //      var balRec = stdBalCLS("","","","","","","","","","","","","","","","0","","","")
    //      var testBalFullKey: String = " "
    //      var testBalInstID: String = " "
    //      var balAmtAccum: Double = 0
    //      var balRead = 0
    //
    //      def readBal(): Unit = {
    //        val e: Array[String] = balTBLLine.next.split(',')
    //        balRec = stdBalCLS(e(0), e(1),e(2),e(3),e(4),e(5),e(6),e(7),e(8),e(9),e(10),e(11),
    //          e(12),e(13),e(14),e(15),e(16),e(17),e(18)) //19 elements
    //        testBalFullKey = balRec.balinstID + balRec.balledgerID + balRec.baljrnlType + balRec.balbookCodeID + balRec.ballegalEntityID +
    //          balRec.balcenterID + balRec.balprojectID + balRec.balproductID + balRec.balaccountID +
    //          balRec.balcurrencyCodeSourceID + balRec.balcurrencyTypeCodeSourceID +
    //          balRec.balcurrencyCodeTargetID + balRec.balcurrencyTypeCodeTargetID + balRec.balfiscalYear
    //        // if (debugPrint == "Y") println("bal rec full key: " + testBalFullKey)
    //        testBalInstID = balRec.balinstID
    //        balAmtAccum = balRec.baltransAmount.toDouble
    //        balRead += 1
    //      }
    //
    //      var balEOF = "N"
    //      def eofBal(): Unit = {
    //        // if (debugPrint == "Y") println("bal high values. EOF bal")
    //        testBalFullKey = "𝖛" * testBalFullKey.length //this is a hack, a higher value constant would be good
    //        testBalInstID = "𝖛" * testBalInstID.length
    //        balEOF = "Y"
    //        // if (tranEOF == "N") lastBalWriteflg = "Y"  // added balEOF == "Y" statement to eofTran routine.  this isn't needed?
    //      }
    //
    //      if (balTBLLine.isEmpty) eofBal()
    //      else readBal()
    //
    //      val balRecTemplate = stdBalCLS("0", "0", "ACTUALS", "FIN", "SHRD-3RD-PARTY", "STATE-OF-VA", "0", "0",
    //        "0", "0", "USD", "TXN", "USD", "BASE-LE", "0", "0", "N", " ", " ")

    //-----------------------------------------------------------------------------------------
    // Trans file
    //-----------------------------------------------------------------------------------------

    //    case class transCLS(tranVendorName: String, tranAgency: String, tranFund: String, tranProg: String, tranObj: String, tranAmt: String,
    //                        tranFisDt: String, tranAcctDt: String, tranTranDt: String, tranType: String)

    var transRec = stdTransCLSPost("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
      "", "", "", "", "", "", "", "", "", "", "") // 31 elements
    var testtranFullKey: String = " "
    var testTransInstID: String = " "
    var transRead = 0

    def readTran(): Unit = {
      //        val e: Array[String] = transLineStrValue.split(' ')

      val e: Array[String] = new Array[String](31);
      if (transLine.hasNext) {
        val transLineStrValue=transLine.next()
        
        e(0)  = transLineStrValue.substring(0, 10)   
        e(1)  = transLineStrValue.substring(11, 28)  
        e(2)  = transLineStrValue.substring(29, 31)  
        e(3)  = transLineStrValue.substring(31, 71)  
        e(4)  = transLineStrValue.substring(72, 79)  
        e(5)  = transLineStrValue.substring(80, 83)  
        e(6)  = transLineStrValue.substring(84, 98)  
        e(7)  = transLineStrValue.substring(99, 103) 
        e(8)  = transLineStrValue.substring(103, 109)
        e(9)  = transLineStrValue.substring(110, 115)
        e(10) = transLineStrValue.substring(116, 118)
        e(11) = transLineStrValue.substring(118, 124)
        e(12) = transLineStrValue.substring(125, 128)
        e(13) = transLineStrValue.substring(129, 132)
        e(14) = transLineStrValue.substring(133, 136)
        e(15) = transLineStrValue.substring(137, 144)
        e(16) = transLineStrValue.substring(145, 157)
        e(17) = transLineStrValue.substring(158, 165)
        e(18) = transLineStrValue.substring(166, 176)
        e(19) = transLineStrValue.substring(177, 187)
        e(20) = transLineStrValue.substring(188, 189)
        e(21) = transLineStrValue.substring(190, 191)
        e(22) = transLineStrValue.substring(192, 193)
        e(23) = transLineStrValue.substring(194, 195)
        e(24) = transLineStrValue.substring(196, 197)
        e(25) = transLineStrValue.substring(198, 199)
        e(26) = transLineStrValue.substring(200, 201)
        e(27) = transLineStrValue.substring(202, 203)
        e(28) = transLineStrValue.substring(204, 205)
        e(29) = transLineStrValue.substring(206, 207)
        e(30) = transLineStrValue.substring(208, 209)
      }

      transRec = stdTransCLSPost(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9),
        e(10), e(11), e(12), e(13), e(14), e(15), e(16), e(17), e(18), e(19),
        e(20), e(21), e(22), e(23), e(24), e(25), e(26), e(27), e(28), e(29), e(30))

      try {
        testtranFullKey = transRec.transinstID + transRec.transledgerID + transRec.transjrnlType + transRec.transbookCodeID + transRec.translegalEntityID +
          transRec.transcenterID + transRec.transprojectID + transRec.transproductID + transRec.transaccountID +
          transRec.transcurrencyCodeSourceID + transRec.transcurrencyTypeCodeSourceID +
          transRec.transcurrencyCodeTargetID + transRec.transcurrencyTypeCodeTargetID + transRec.transfiscalPeriod.substring(0, 4)
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
      testtranFullKey = "𝖛" * testtranFullKey.length //this is a hack, a higher value constant would be good
      testTransInstID = "𝖛" * testTransInstID.length
      tranEOF = "Y"
    }

    if (transLine.isEmpty) eofTran()
    else readTran()

    //-----------------------------------------------------------------------------------------
    // Instrument Update file
    //-----------------------------------------------------------------------------------------
    // insturment update output record structure
    //      case class instUPDCLS(instUPDID: String, instUPDVendorName: String, instUPDTypeID: String, instUPDEffectDate: String)
    //
    //
    //      var instUPDRec = instUPDCLS("","","","")
    //      var testInstUPDFullKey: String = " "
    //      var testInstUPDInstKey: String = " "
    //      var instUPDRead = 0
    //
    //      def readInstUPD(): Unit = {
    //        val e: Array[String] = instUPDLine.next.split(',')
    //        instUPDRec = instUPDCLS(e(0),e(1),e(2),e(3))
    //        testInstUPDFullKey = instUPDRec.instUPDID
    //        testInstUPDInstKey = instUPDRec.instUPDID
    //        // if (debugPrint == "Y") println("inst Update key: " + testInstUPDFullKey + " Inst UPD row: " + instUPDRec)
    //        instUPDRead += 1
    //      }
    //
    //      var instUPDEOF = "N"
    //      def eofInstUPD(): Unit = {
    //        // if (debugPrint == "Y") println("tran high values; EOF Inst UPD")
    //        testInstUPDFullKey = "𝖛" * testInstUPDFullKey.length  //this is a hack, a higher value constant would be good
    //        testInstUPDInstKey = "𝖛" * testInstUPDInstKey.length
    //        instUPDEOF = "Y"
    //        //      if (instEOF == "Y") writeInst()  // routine only accessible if instUPD > inst record.  must write for last instUPD record.
    //      }
    //
    //      if (instUPDLine.isEmpty) eofInstUPD()
    //      else readInstUPD()

    //*****************************************************************************************
    // process variables for while loop in file processing
    //*****************************************************************************************
    var swapInst = "N"
    var saveInstFullKey: String = ""
    var saveInstFullRecord = stdInstCLS("", "", "", "", "", "", "", "")
    var saveInstID: String = ""
    var swapBal = "N"
    var saveBalFullKey: String = ""
    //      var saveBalFullRecord = stdBalCLS("","","","","","","","","","","","","","","","","","","")
    var saveBalInstID: String = ""
    var saveBalAmtAccum: Double = 0
    var lastBalWriteflg = "N"
    var counter: Int = 0
    var errorCnt: Int = 0

    //*****************************************************************************************
    // Mainline Program Structure:
    //  PERFORM INITIAL POSTING
    //  4 - Match Transactions to Balances
    //  5 - Update or create new balances
    //  REPORT and UNLOAD (FILE SAVE) LOOP
    //  10- Create various reports in the midst of the unload process
    //  11- Save report structures
    //  CLEAN-UP
    //  12- Produce control report
    //*****************************************************************************************

    while (tranEOF == "N" || instEOF == "N") {

      if (testTransInstID == testInstFullKey) {
        // compare attributes of instUPD to inst; save an effective date if they are different, create new inst record
        // read next instUPDsaction record
        writeTran()
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
        if (testTransInstID < testInstFullKey) {
          // new inst found.  test if new inst is equal to already read and saved inst record from inst file
          println("TranKey without Instrument Key, something amiss here. tran: " + testtranFullKey + " inst: " + testInstFullKey)
          if (tranEOF == "N") {
            if (transLine.hasNext) {
              readTran()
            }
            else {
              eofTran()
            }
          }
          errorCnt += 1
        }
        else {
          if (testTransInstID > testInstFullKey) {
            // finished with inst record
            // if (debugPrint == "Y") println(">>: after instUPD: " + testInstUPDFullKey + " inst: " + testInstFullKey)
            // read new instrument
            if (instEOF == "N") {
              if (instTBLLine.hasNext) {
                readInst()
              }
              else {
                eofInst()
              }
            }
          }
          else {
            println("an evaluation error has occurred on the Inst and trans Files!  Files might be out of sort order")
          }
        }
      }


      counter += 1
      // if (debugPrint == "Y") println("counter val: " + counter)
      // One additional loop is required once either the Inst or the InstUPD file is finished to write the Last Balance
      if (instEOF == "Y" && (tranEOF == "N")) {
        // if (debugPrint == "Y") println("Finish extra instrument")

        // Any additional times through means there are trans or balances without instruments
        if (lastBalWriteflg == "Y") {
          println("***************************************************************************************")
          println("WARNING! Transactions or Balances without Instruments.  Critical Error:  Files Invalid!")
          println("***************************************************************************************")
          tranEOF = "Y"
        }

        lastBalWriteflg = "Y"
      }
    }


    //*****************************************************************************************
    // Close files
    //*****************************************************************************************
    newSumTBLwriter.close()

    //*****************************************************************************************
    // Control Report
    //*****************************************************************************************

    println("*****************************************")
    println("Balance Posting Program Control Report")
    println("While Loops:                " + counter)
    println("Instruments Read:           " + instRead)
    //      println("Instrument Updates Read:    " + instUPDRead)
    println("Transactions Read:          " + transRead)
    //      println("Balances Read:              " + balRead)
    println("Transactions Written:       " + sumWritten)
    println("Error Count:                " + errorCnt)
    //      println("Balances Written:           " + balWritten)
    //      println("Balance IDs Assigned:       " + (maxBalID - startMaxBalID))
    println("*****************************************")


    //*****************************************************************************************
    //*****************************************************************************************
    // End of Main def drop through logic
    //*****************************************************************************************
    //*****************************************************************************************

    //*****************************************************************************************
    // Common Routines
    //*****************************************************************************************


    def writeTran(): Unit = {

      newSumTBLwriter.write(
        transRec.transinstID + "," +
          //          0.toString * (10 - maxBalID.toString.length) + maxBalID.toString,  //left fill with zeros to 10 digits
          transRec.transledgerID + "," +
          transRec.transjrnlType + "," +
          transRec.transbookCodeID + "," +
          transRec.translegalEntityID + "," +
          transRec.transcenterID + "," +
          transRec.transprojectID + "," +
          transRec.transproductID + "," +
          transRec.transaccountID + "," +
          transRec.transcurrencyCodeSourceID + "," +
          transRec.transcurrencyTypeCodeSourceID + "," +
          transRec.transcurrencyCodeTargetID + "," +
          transRec.transcurrencyTypeCodeTargetID + "," +
          transRec.transfiscalPeriod.substring(0, 4) + "," +
          transRec.transtransAmount + "," + //16th element
          transRec.transmovementFlg + "," +
          transRec.transunitOfMeasure + "," +
          transRec.transstatisticAmount + "," +
          instRec.instinstID + "," +
          instRec.instEffectDate + "," +
          instRec.instEffectTime + "," +
          instRec.instEffectEndDate + "," +
          instRec.instEffectEndTime + "," +
          instRec.instHolderName + "," +
          instRec.instTypeID + "," +
          instRec.instAuditTrail +
          "\n"
      )
      sumWritten += 1
    }
  }
}
