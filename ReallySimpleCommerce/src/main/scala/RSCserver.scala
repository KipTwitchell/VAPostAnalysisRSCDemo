import scala.io.{BufferedSource, Source}
import java.io.{File, PrintWriter}
import java.lang.management.ManagementFactory
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable
import scala.util.Try
//import com.datastax.driver.core._
//import com.google.common.util.concurrent.ListenableFuture
//	(c) Copyright IBM Corporation. 2017
//  SPDX-License-Identifier: Apache-2.0
//     Kip Twitchell <finsysvlogger@gmail.com>.
//  Created July 2017

object RSCserver {
  def main(args: Array[String]): Unit = {

    //-----------------------------------------------------------------------------------------
    // Start Up Processes
    //-----------------------------------------------------------------------------------------
    println("RSC Server Initialization")

    val filePath = args(0)
    val AuthWriteFileName = args(1)
    val ServerReadFileName = args(2)
    val VendWriteFileName = args(3)
    val CustWriteFileName = args(4)
    val instReadFileName = args(5)
    val transReadFileName = args(6)

//    var inputServerBuffer= Source.fromFile(filePath + ServerReadFileName)

    var mode = ""
    var response = "" // used for user input within a mode execution
    // communication buffer format = Mode, Operation, Value
    case class serverResponse(
                               SRMode: String,
                               SROperator: String,
                               SRValue: String,
                               SRValueType: String,
                               SRValueStruct: String
                             )
    var serverLine: serverResponse = serverResponse("","","","","")

    def quitProg(): Unit = {
      // Send quit signal to all nodes
      println("RSC Logger:  Server Quit" )
      val outputAuthBuffer = new PrintWriter(new File(filePath + AuthWriteFileName))
      outputAuthBuffer.write("Quit,Quit,Quit,Quit,Quit")
      outputAuthBuffer.close()
      val outputVendBuffer = new PrintWriter(new File(filePath + VendWriteFileName))
      outputAuthBuffer.write("Quit,Quit,Quit,Quit,Quit")
      outputVendBuffer.close()
      val outputCustBuffer = new PrintWriter(new File(filePath + CustWriteFileName))
      outputAuthBuffer.write("Quit,Quit,Quit,Quit,Quit")
      outputCustBuffer.close()
      sys.exit(0)
    }

    //-----------------------------------------------------------------------------------------
    // Data Structures for saving values
    //-----------------------------------------------------------------------------------------

    case class inst (
                      instCellNumber: String,
                      instName: String,
                      instEmail: String,
                      instCreditLimit: String,
                      instCLCurrencyCD: String,
                      instAuthorizerID: String
                    )

    var instTBL: mutable.Map[String,(inst)] = mutable.Map.empty

    var newInstCellNumber: String = ""
    var newInstName: String = ""
    var newInstEmail: String = ""
    var newInstCreditLimit: String = ""
    var newInstCLCurrencyCD: String = ""
    var newInstAuthorizerID: String = ""

    def addInst (): Unit = {
      instTBL += (newInstCellNumber -> inst(
        newInstCellNumber,
        newInstName,
        newInstEmail,
        newInstCreditLimit,
        newInstCLCurrencyCD,
        newInstAuthorizerID)
        )
    }

    case class tran (
                      tranInstID: String,
                      tranTimeStamp: String,
                      tranTranCode: String,
                      tranVendID: String,
                      tranTranDate: String,
                      tranLocation: String,
                      tranCurrencyCD: String,
                      tranTranAmount: String
                    )

    var newTranInstID: String = ""
    var newTranTimeStamp: String = ""
    var newTranTranCode: String = ""
    var newTranVendID: String = ""
    var newTranTranDate: String = ""
    var newTranLocation: String = ""
    var newTranCurrencyCD: String = ""
    var newTranTranAmount: String = ""

    case class tranKey (
                         tranInstID: String,
                         tranTimeStamp: String
                       )

//    var tranTBL: mutable.Map[tranKey, (tran)] = mutable.Map.empty
    var tranTBL: mutable.Map[tranKey, (tran)] = mutable.Map.empty

    def addTran (): Unit = {
      tranTBL += (tranKey(newTranInstID,newTranTimeStamp) -> tran(
      newTranInstID,
      newTranTimeStamp,
      newTranTranCode,
      newTranVendID,
      newTranTranDate,
      newTranLocation,
      newTranCurrencyCD,
      newTranTranAmount
        ))
    }

    //-----------------------------------------------------------------------------------------
    // Load Pre-existing data for other cell phones and transactions for Query Processes
    //-----------------------------------------------------------------------------------------

    var inputInstBuffer = Source.fromFile(filePath + instReadFileName)
//    val instHeaders = inputInstBuffer.getLines
    for (line <- inputInstBuffer.getLines.drop(1)) {
      val cols = line.split(",").map(_.trim)
      newInstCellNumber = cols(0)
      newInstName  = cols(1)
      newInstEmail  = cols(2)
      newInstCreditLimit  = cols(3)
      newInstCLCurrencyCD  = cols(4)
      newInstAuthorizerID  = cols(5)
      addInst()
    }
    println(instTBL)

    var inputTranBuffer = Source.fromFile(filePath + transReadFileName)
//    val tranHeader = inputTranBuffer.getLines
    for (line <- inputTranBuffer.getLines.drop(1)) {
      val cols = line.split(",").map(_.trim)
      newTranInstID      = cols(0)
      newTranTimeStamp  = cols(1)
      newTranTranCode    = cols(2)
      newTranVendID     = cols(3)
      newTranTranDate    = cols(4)
      newTranLocation    = cols(5)
      newTranCurrencyCD  = cols(6)
      newTranTranAmount  = cols(7)
      addTran()
    }
    println(tranTBL)


    //-----------------------------------------------------------------------------------------
    // Read and Write Routines to Named Pipes for communication with nodes
    //-----------------------------------------------------------------------------------------

    def readServer(): serverResponse = {
      var inputServerBuffer = Source.fromFile(filePath + ServerReadFileName)
      for (line <- inputServerBuffer.getLines) {
        val cols = line.split(",").map(_.trim)
        serverLine = serverResponse(cols(0), cols(1), cols(2), cols(3), cols(4))
      }
      serverLine
    }

    def writeAuth(message: String): Unit = {
      // buffer format = Mode, Operation, Value
      val outputAuthBuffer = new PrintWriter(new File(filePath + AuthWriteFileName))
      outputAuthBuffer.write(message)
      outputAuthBuffer.close()
    }

    def writeCust(message: String): Unit = {
      // buffer format = Mode, Operation, Value
      val outputCustBuffer = new PrintWriter(new File(filePath + CustWriteFileName))
      outputCustBuffer.write(message)
      outputCustBuffer.close()
    }

    def writeVend(message: String): Unit = {
      // buffer format = Mode, Operation, Value
      val outputVendBuffer = new PrintWriter(new File(filePath + VendWriteFileName))
      outputVendBuffer.write(message)
      outputVendBuffer.close()
    }


    println("RSC Server Started")

    //*****************************************************************************************
    // Display Loop
    //*****************************************************************************************

    var screenPrompt = "S" // used to detect larger loop constructs
    while (screenPrompt != "Q") {

      println("RSC Logger:  Server Loop" )
      serverLine = readServer()
      if (serverLine.SRMode == "Quit") quitProg()
      //-----------------------------------------------------------------------------------------
      // Authorizer Script
      //-----------------------------------------------------------------------------------------

      if (serverLine.SRMode == "A") {

        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "A" | serverLine.SROperator != "new" ) {
          println("Server Side S.A New Tran Process Error.  Program Stopped")
          quitProg()
        }

        // Received new customer request, containing cell phone
        println("RSC Logger:  Server Authorize Request Received.  Cell Number: " + serverLine.SRValue)
        newInstCellNumber = serverLine.SRValue
        // request credit limt
        writeAuth("S.A,CLAmount,?" + ", " + ", ")
        serverLine = readServer()

        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "A" | serverLine.SROperator != "CLAmount") {
          println("Server Side S.A Credit Limit Process Error.  Program Stopped")
          quitProg()
        }
        println("RSC Logger:  Server Authorize Credit Limit Request Received.  Limit: " + serverLine.SRValue)
        newInstCreditLimit = serverLine.SRValue
        // request currency Code
        writeAuth("S.A,Currency,?" + ", " + ", ")
        serverLine = readServer()

        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "A" | serverLine.SROperator != "Currency") {
          println("Server Side S.A Currency Code Process Error.  Program Stopped")
          quitProg()
        }
        println("RSC Logger:  Server Authorize Currency Code Request Received.  Code: " + serverLine.SRValue)
        newInstCLCurrencyCD = serverLine.SRValue
        newInstAuthorizerID = ManagementFactory.getRuntimeMXBean.getName // this may not work on all JVM implementations

        // write to customer to confirm addition
        writeCust("S.A,ConfirmJoin,Authorize" + ", " + ", ")
        serverLine = readServer()

        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "C" | serverLine.SROperator != "ConfirmJoin") {
          println("Server Side S.A Confirm Process Error.  Program Stopped")
          quitProg()
        }
        println("RSC Logger:  Server Customer Confirmed Authorization")
        // write to customer to ask name
        writeCust("S.A,Name,?" + ", " + ", ")
        serverLine = readServer()

        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "C" | serverLine.SROperator != "Name") {
          println("Server Side S.A Name Process Error.  Program Stopped")
          quitProg()
        }
        println("RSC Logger:  Server Customer Name: " + serverLine.SRValue)
        newInstName = serverLine.SRValue
        // write to customer to request e-mail
        writeCust("S.A,email,?" + ", " + ", ")
        serverLine = readServer()

        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "C" | serverLine.SROperator != "email") {
          println("Server Side S.A email Process Error.  Program Stopped")
          quitProg()
        }

        // Show Confirmation message
        println("RSC Logger:  Server Customer email: " + serverLine.SRValue)
        newInstEmail = serverLine.SRValue
        // Add new customer inst to inst table
        addInst()
        println(instTBL)
        val instAdd = instTBL(newInstCellNumber).instCellNumber + "," +
            instTBL(newInstCellNumber).instName + "," +
            instTBL(newInstCellNumber).instEmail + "," +
            instTBL(newInstCellNumber).instCreditLimit + "," +
            instTBL(newInstCellNumber).instCLCurrencyCD + "," +
            instTBL(newInstCellNumber).instAuthorizerID
        // write to customer to confirm addition
        writeCust("S.A,Complete,cell,inst," + instAdd)
        // write to authorizer to confirm addition
        writeAuth("S.A,Complete,cell,inst," + instAdd)

        // Reset for next transaction
        screenPrompt = "S"
      }

      //-----------------------------------------------------------------------------------------
      // Vendor Script
      //-----------------------------------------------------------------------------------------

      if (serverLine.SRMode == "V") {

        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "V" | serverLine.SROperator != "newTran" ) {
          println("Server Side S.V New Tran Process Error.  Program Stopped")
          quitProg()
        }
        println("RSC Logger:  Server Vendor newTran Request Received.  Cell Number: " + serverLine.SRValue )
        //set a number of items in the transaction record for recording later
        newTranInstID = serverLine.SRValue
        newTranTimeStamp = System.currentTimeMillis.toString
        newTranTranCode = "VendPurchase"
        val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        val cal = Calendar.getInstance.getTime
        newTranTranDate = dateFormat.format(cal) // 2016/11/16 12:08:43
        newTranLocation = "At store" //This obviously is going to be a very useful data element in our transactions
        newTranVendID = ManagementFactory.getRuntimeMXBean.getName // this may not work on all JVM implementations

        // write to request transaction amount
        writeVend("S.V,TranAmount,?" + ", " + ", ")
        serverLine = readServer()
        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "V" | serverLine.SROperator != "TranAmount" ) {
          println("Server Side S.V Tran Amount Process Error.  Program Stopped")
          quitProg()
        }
        println("RSC Logger:  Server Vendor Trans Amount Received.  Amount: " + serverLine.SRValue )
        newTranTranAmount = serverLine.SRValue

        // write to request currency
        writeVend("S.V,Currency,?" + ", " + ", ")
        serverLine = readServer()
        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "V" | serverLine.SROperator != "Currency" ) {
          println("Server Side S.V Currency Process Error.  Program Stopped")
          quitProg()
        }
        println("RSC Logger:  Server Vendor Currency Received.  Currency: " + serverLine.SRValue )
        newTranCurrencyCD = serverLine.SRValue

        // Test the trans amount against the credit limit; and give vendor a choice
        val custCL =
          try {instTBL(newTranInstID).instCreditLimit}
          catch {case e: NoSuchElementException => "unavailable" }
        println("CL " + custCL)
        val custAccumTransTBL = tranTBL.groupBy(_._2.tranInstID).mapValues(_.map(_._2.tranTranAmount.toDouble).sum.toString)

        val custAccumTrans : String = {
          try {custAccumTransTBL.get(newTranInstID).getOrElse().toString
          }
          catch {case e: NoSuchElementException => "unavailable" }
          // this should not equal 0, but should be another condition
        }

        println("Cust Accum " + custAccumTrans)

        // coded now to skip these steps, assuming value is below credit limit
        if (custCL == "unavailable" | custAccumTrans == "unavailable") {
          writeVend("S.V,CreditCheck,unavailable" + ", " + ", ")
          serverLine = readServer()
          // **** Check Response
          if (serverLine.SRMode == "Quit") quitProg()
          if (serverLine.SRMode != "V" | serverLine.SROperator != "CreditCheck") {
            println("Server Side S.V Currency Process Error.  Program Stopped")
            quitProg()
          }
          if (serverLine.SRValue != "SkipCheck") {
            println("Server Side S.V Credit Check Not Available, Not Skipped Process Error.  Program Stopped")
            quitProg()
          }
        }
        else {
          // credit limit and spent amount available for display to vendor
          writeVend("S.V,CreditCheck,available," + custCL.toString + "," + custAccumTrans.toString)
          serverLine = readServer()
          // **** Check Response
          if (serverLine.SRMode == "Quit") quitProg()
          if (serverLine.SRMode != "V" | serverLine.SROperator != "CreditCheck") {
            println("Server Side S.V Currency Process Error.  Program Stopped")
            quitProg()
          }
          if (serverLine.SRValue != "Confirm") {
            println("Server Side S.V Credit Check Confirmed by Vendor.  Program Stopped")
            quitProg()
          }
        }
        println("RSC Logger:  Server Vendor Trans Amount Less than Limit: " + serverLine.SRValue )

        // write to customer to confirm transaction
        writeCust("S.V,ConfirmTran," + newTranTranAmount + " " + newInstCLCurrencyCD + ", " + ", ")
        // wait for response from customer
        serverLine = readServer()

        // **** Check Response
        if (serverLine.SRMode == "Quit") quitProg()
        if (serverLine.SRMode != "C" | serverLine.SROperator != "ConfirmTran") {
          println("Server Side S.V Confirm Process Error.  Program Stopped")
          quitProg()
        }
        println("RSC Logger:  Server Customer Confirmed Transaction")

        // write to customer to ask payment Method
        writeCust("S.V,PayMethod," + newTranTranAmount + ", " + ", ")
        // wait for response from customer
        serverLine = readServer()
        if (serverLine.SRMode == "Quit") quitProg()

        // Show Confirmation message
        println("RSC Logger:  Server Customer Payment Method: " + serverLine.SRValue)
        // Add new tran to transaction table
        addTran()
        println(tranTBL)
        val tranAdd =
          newTranInstID  + "," +
          newTranTimeStamp  + "," +
          newTranTranCode  + "," +
          newTranVendID  + "," +
          newTranTranDate  + "," +
          newTranLocation  + "," +
          newTranCurrencyCD  + "," +
          newTranTranAmount

        // write to customer to confirm addition
        writeCust("S.V,TranComplete,iou,tran," + tranAdd)
        // write to vendor to confirm addition
        writeVend("S.V,TranComplete,iou,tran," + tranAdd)

        // Reset for next transaction
        screenPrompt = "S"

      } // End of Vendor Script


    }  // End of on-screen while loop
  } // end of main
}
