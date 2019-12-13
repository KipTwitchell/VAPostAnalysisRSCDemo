import scala.io.{BufferedSource, Source}
import java.io._

import scala.collection.mutable
//import java.io.{File, PrintWriter}
import sys.process._
/*
 * //	(c) Copyright IBM Corporation. 2017
//  SPDX-License-Identifier: Apache-2.0
//     Kip Twitchell <finsysvlogger@gmail.com>.
//  Created July 2017
 */

object RSCclient {
  def main(args: Array[String]): Unit = {

    //-----------------------------------------------------------------------------------------
    // Start Up Processes
    //-----------------------------------------------------------------------------------------

    val filePath = args(0)
    val AuthReadFileName = args(1)
    val ServerReadFileName = args(2)
    val VendReadFileName = args(3)
    val CustReadFileName = args(4)

    var mode = ""
    var response = "" // used for user input within a mode execution

    //-----------------------------------------------------------------------------------------
    // Data Structures for saving values
    //-----------------------------------------------------------------------------------------

    case class serverResponse(
                             SRMode: String,
                             SROperator: String,
                             SRValue: String,
                             SRValueType: String,
                             SRValueStruct: String
                             )
    var serverLine: serverResponse = serverResponse("","","","","")


    case class inst (
                      instCellNumber: String,
                      instName: String,
                      instEmail: String,
                      instCreditLimit: String,
                      instCLCurrencyCD: String,
                      instAuthorizerID: String
                    )

    var instResponse: inst = inst("","","","","","")
    var serverInstTuple = (serverLine,instResponse) // used for Auth file reads

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
    var tranResponse: tran = tran("","","","","","","","")

    var serverTranTuple = (serverLine,tranResponse) // used for vendor file reads
    var serverInstTranTuple = (serverLine,instResponse,tranResponse) // used for customer file reads


    //-----------------------------------------------------------------------------------------
    // Read and Write Routines to Named Pipes for communication with nodes
    //-----------------------------------------------------------------------------------------


    // Quit this session, and signal server to quit all other sessions
    def quitProg(): Unit = {
      val outputServerBuffer = new PrintWriter(new File(filePath + ServerReadFileName))
      outputServerBuffer.write("Quit,Quit,Quit, , ")
      outputServerBuffer.close()
      sys.exit(0)
    }

    def writeServer(message: String): Unit = {
      // buffer format = Mode, Operation, Value
      val outputServerBuffer = new PrintWriter(new File(filePath + ServerReadFileName))
      outputServerBuffer.write(message)
      outputServerBuffer.close()
    }

    def readAuth(): (serverResponse, inst) = {
      var inputAuthBuffer = Source.fromFile(filePath + AuthReadFileName)
      for (line <- inputAuthBuffer.getLines) {
        val cols = line.split(",").map(_.trim)
        serverLine = serverResponse(cols(0),cols(1),cols(2),cols(3),cols(4))
        if (cols(3) == "inst") {
          instResponse = inst(cols(4),cols(5),cols(6),cols(7),cols(8),cols(9))
        }
        else {instResponse = inst("","","","","","")}
      }
      (serverLine, instResponse)
    }

    def readCust(): (serverResponse, inst, tran) = {
      var inputCustBuffer = Source.fromFile(filePath + CustReadFileName)
      for (line <- inputCustBuffer.getLines) {
        val cols = line.split(",").map(_.trim)
        serverLine = serverResponse(cols(0),cols(1),cols(2),cols(3),cols(4))
        if (cols(3) == "inst") {
          instResponse = inst(cols(4),cols(5),cols(6),cols(7),cols(8),cols(9))
        }
        else {instResponse = inst("","","","","","")
          if (cols(3) == "tran") {
            tranResponse = tran(cols(4),cols(5),cols(6),cols(7),cols(8),cols(9),cols(10),cols(11))
          }
          else {tranResponse = tran("","","","","","","","")}
        }
      }
      (serverLine, instResponse, tranResponse)
    }

    def readVend(): (serverResponse, tran) = {
      var inputVendBuffer = Source.fromFile(filePath + VendReadFileName)
      for (line <- inputVendBuffer.getLines) {
        val cols = line.split(",").map(_.trim)
        serverLine = serverResponse(cols(0),cols(1),cols(2),cols(3),cols(4))
        if (cols(3) == "tran") {
          tranResponse = tran(cols(4),cols(5),cols(6),cols(7),cols(8),cols(9),cols(10),cols(11))
        }
        else {tranResponse = tran("","","","","","","","")}
      }
      (serverLine, tranResponse)
    }

    def promptInput(): String = {
      val input = scala.io.StdIn.readLine().trim.toUpperCase()
      if (input.substring(0, 1) == "Q") quitProg()
      input
    }

    def printInstConfirm(x: inst): Unit = {
      println(" _________________________")
      println("| Customer Add Complete  |")
      println("| Cell " + x.instCellNumber + (" " * (18 - x.instCellNumber.length)) + "|")
      println("| Name " + x.instName + (" " * (18 - x.instName.length)) + "|")
      println("| Email " + x.instEmail + (" " * (17 - x.instEmail.length)) + "|")
      println("| Limit " + x.instCreditLimit + (" " * (17 - x.instCreditLimit.length)) + "|")
      println("| Curr CD " + x.instCLCurrencyCD + (" " * (15 - x.instCLCurrencyCD.length)) + "|")
      println("| Auth ID " + x.instAuthorizerID.substring(0,15) + (" " * (15 - x.instAuthorizerID.length)) + "|")
      println(" _________________________")
      println(" _________________________")
      println("| New Transaction? Y/N    |")
      println("|   ( or Q = Quit)        |")
      println("|_________________________|")
    }

    def printTranConfirm(x: tran): Unit = {
      println(" _________________________")
      println("| Transaction Complete   |")
      println("| Cell " + x.tranInstID + (" " * (18 - x.tranInstID.length)) + "|")
      println("| Date " + x.tranTranDate.substring(0,16) + (" " * (16 - x.tranTranDate.length)) + "|")
      println("| Type " + x.tranTranCode + (" " * (18 - x.tranTranCode.length)) + "|")
      println("| Vendor " + x.tranVendID.substring(0,16) + (" " * (16 - x.tranVendID.length)) + "|")
      println("| Amount " + x.tranTranAmount + " " + x.tranCurrencyCD +
        (" " * (16 - (x.tranTranAmount.length + x.tranCurrencyCD.length + 1 )) + "|"))
      println(" _________________________")
      println(" _________________________")
      println("| New Transaction? Y/N    |")
      println("|   ( or Q = Quit)        |")
      println("|_________________________|")
    }

    //*****************************************************************************************
    // Display Loop
    //*****************************************************************************************

    var screenPrompt = "S" // used to detect larger loop constructs.  Stands for Start
    while (screenPrompt != "Q") {

      // Initial Prompt
      if (screenPrompt == "S") {
        println("***************************")
        println("*     Welcome to the      *")
        println("* Really Simple Commerce  *")
        println("*         system          *")
        println("***************************")
        println(" _________________________" )
        println("| Reply with your role:   |")
        println("|   A = Authorizer        |")
        println("|   V = Vendor            |")
        println("|   C = Customer          |")
        println("|   I = Inquiry           |")
        println("|   Q = Quit              |")
        println("|_________________________|")
        mode = promptInput()
      }

      //-----------------------------------------------------------------------------------------
      // Authorizer Script
      //-----------------------------------------------------------------------------------------

      if (mode == "A") {
        "echo -n -e \"\u001b]0;RSC Authorizer Cell Phone\u0007\"".!
        println(" This is now an Authorizer Cell Phone Screen")
        println(" _________________________" )
        println("| Reply with customer     |")
        println("|   cell phone number     |")
        println("|   ( or Q = Quit)        |")
        println("|_________________________|")
        response = promptInput()
        writeServer("A,new," + response + ", " + ", ")
       serverInstTuple = readAuth();serverLine = serverInstTuple._1;instResponse = serverInstTuple._2
        if (serverLine.SRMode != "S.A" | serverLine.SROperator != "CLAmount" ) {
          println("S.A CL Amount Process Error.  Program Stopped")
          quitProg()
        }

        // Request credit limit from user input
        println(" _________________________")
        println("| Reply with customer     |")
        println("|   credit limit amount   |")
        println("|   ( or Q = Quit)        |")
        println("|_________________________|")
        response = promptInput()
        writeServer("A,CLAmount," + response + ", " + ", ")
       serverInstTuple = readAuth();serverLine = serverInstTuple._1;instResponse = serverInstTuple._2
        if (serverLine.SRMode != "S.A" | serverLine.SROperator != "Currency") {
          println("S.A Currency Code Error.  Program Stopped")
          quitProg()
        }

        // Request currency code for Credit Limit from user input
        println(" _________________________")
        println("| In what currency?       |")
        println("|   ( or Q = Quit)        |")
        println("|_________________________|")
        response = promptInput()
        writeServer("A,Currency," + response + ", " + ", ")
       serverInstTuple = readAuth();serverLine = serverInstTuple._1;instResponse = serverInstTuple._2
        if (serverLine.SRMode != "S.A" | serverLine.SROperator != "Complete") {
          println("S.A Complete Error.  Program Stopped")
          quitProg()
        }

        // Show Add Complete Message
        printInstConfirm(instResponse)
        response = promptInput()

        // Prepare for next loop iteration
        screenPrompt = "S"
      } // End of Authorizer Script

        //-----------------------------------------------------------------------------------------
        // Customer Script
        //-----------------------------------------------------------------------------------------

        if (mode == "C") {
          "echo -n -e \"\u001b]0;RSC Customer Cell Phone\u0007\"".!
          println(" This is now a Customer Cell Phone Screen")
          // wait for response from server
          serverInstTranTuple = readCust()
          serverLine = serverInstTranTuple._1
          instResponse = serverInstTranTuple._2
          tranResponse = serverInstTranTuple._3

          // Customer Interaction on Authorize Activity
          if (serverLine.SRMode == "S.A" && serverLine.SROperator == "ConfirmJoin" ) {
            // Request name from user input
            println(" _________________________")
            println("| Do you want to join     |")
            println("| the RSC Network? Y/N    |")
            println("|   ( or Q = Quit)        |")
            println("|_________________________|")
            response = promptInput()
            if (response.substring(0, 1) == "N") quitProg()
            writeServer("C,ConfirmJoin,Yes" + ", " + ", ")
            serverInstTranTuple = readCust()
            serverLine = serverInstTranTuple._1
            instResponse = serverInstTranTuple._2
            tranResponse = serverInstTranTuple._3

            if (serverLine.SRMode != "S.A" | serverLine.SROperator != "Name") {
              println("S.A name Process Error.  Program Stopped")
              quitProg()
            }
            // Request name from user input
            println(" _________________________")
            println("| Reply w/ your full name |")
            println("|   ( or Q = Quit)        |")
            println("|_________________________|")
            response = promptInput()
            writeServer("C,Name," + response + ", " + ", ")
            serverInstTranTuple = readCust()
            serverLine = serverInstTranTuple._1
            instResponse = serverInstTranTuple._2
            tranResponse = serverInstTranTuple._3

            if (serverLine.SRMode != "S.A" | serverLine.SROperator != "email") {
              println("S.A email Process Error.  Program Stopped")
              quitProg()
            }
            // request e-mail address
            println(" _________________________")
            println("| Reply e-mail address    |")
            println("|   ( or Q = Quit)        |")
            println("|_________________________|")
            response = promptInput()
            writeServer("C,email," + response + ", " + ", ")
            serverInstTranTuple = readCust()
            serverLine = serverInstTranTuple._1
            instResponse = serverInstTranTuple._2
            tranResponse = serverInstTranTuple._3

            if (serverLine.SRMode != "S.A" | serverLine.SROperator != "Complete") {
              println("S.A Complete Process Error.  Program Stopped")
              quitProg()
            }
            // Show Add Complete Message
            printInstConfirm(instResponse)
            response = promptInput()
            if (response.substring(0, 1) == "N") sys.exit(0)
            screenPrompt = "S"
          } // end of Authorization Confirmation Script
//          else {println("S.A Confirm Join Process Error.  Program Stopped") ; quitProg()}
          // the above line doesn't work because there are two different if statements in customer section

          // Customer interaction on Vendor Transaction Side
          if (serverLine.SRMode == "S.V" && serverLine.SROperator == "ConfirmTran" ) {
            // Request name from user input
            println(" _________________________")
            println("| Do you want to pay      |")
            println("| " + serverLine.SRValue + (" " * (19 - serverLine.SRValue.length)) + " Y/N |")
            println("|   ( or Q = Quit)        |")
            println("|_________________________|")
            response = promptInput()
            if (response.substring(0, 1) == "N") quitProg() // I could have a reset function here, rather than ending
            writeServer("C,ConfirmTran,Yes" + ", " + ", ")
            serverInstTranTuple = readCust()
            serverLine = serverInstTranTuple._1
            instResponse = serverInstTranTuple._2
            tranResponse = serverInstTranTuple._3

            if (serverLine.SRMode != "S.V" | serverLine.SROperator != "PayMethod") {
              println("S.V ConfirmTran Process Error.  Program Stopped")
              quitProg()
            }
            // Request name from user method of payment
            println(" _________________________")
            println("| How do you want to pay? |")
            println("|  1 - RSC IOU            |")
            println("|  2 - Venmo              |")
            println("|  3 - Apple Pay          |")
            println("|  4 - PayPal             |")
            println("|   ( or Q = Quit)        |")
            println("|_________________________|")
            response = promptInput()
            if (response.substring(0, 1) != "1") quitProg() // I could have a reset function here, rather than ending
            writeServer("C,PayMethod,RSC-IOU," + ", " + ", ")
            serverInstTranTuple = readCust()
            serverLine = serverInstTranTuple._1
            instResponse = serverInstTranTuple._2
            tranResponse = serverInstTranTuple._3

            if (serverLine.SRMode != "S.V" | serverLine.SROperator != "TranComplete") {
              println("S.V TranComplete Process Error.  Program Stopped")
              quitProg()
            }
            // Show Add Complete Message
            printTranConfirm(tranResponse)
            response = promptInput()
            if (response.substring(0, 1) == "N") sys.exit(0)
            screenPrompt = "S"
          } // end of Vendor Customer Portion  Script
//          else {println("S.V Customer Portion of Vendor Process Error.  Program Stopped") ; quitProg()}
          // the above line doesn't work because there are two different if statements in customer section
        } // End of Customer Script

      //-----------------------------------------------------------------------------------------
      // Vendor Script
      //-----------------------------------------------------------------------------------------

      if (mode == "V") {
        "echo -n -e \"\u001b]0;RSC Vendor Cell Phone\u0007\"".!
        println(" This is now an Vendor Cell Phone Screen")
        println(" _________________________" )
        println("| Reply with customer     |")
        println("|   cell phone number     |")
        println("|   ( or Q = Quit)        |")
        println("|_________________________|")
        response = promptInput()
        writeServer("V,newTran," + response + ", " + ", ")
        serverTranTuple = readVend()
        serverLine = serverTranTuple._1
        tranResponse = serverTranTuple._2
        if (serverLine.SRMode != "S.V" | serverLine.SROperator != "TranAmount" ) {
          println("S.V TransAmount Process Error.  Program Stopped")
            quitProg()
          }

        // Request transaction amount
        println(" _________________________")
        println("| Transaction Amount?     |")
        println("|   ( or Q = Quit)        |")
        println("|_________________________|")
        response = promptInput()
        writeServer("V,TranAmount," + response + ", " + ", ")
        serverTranTuple = readVend()
        serverLine = serverTranTuple._1
        tranResponse = serverTranTuple._2
        if (serverLine.SRMode != "S.V" | serverLine.SROperator != "Currency") {
          println("S.V Currency Process Error.  Program Stopped")
          quitProg()
        }

        // Request currency from vendor
        println(" _________________________")
        println("| Currency Code?          |")
        println("|   ( or Q = Quit)        |")
        println("|_________________________|")
        response = promptInput()
        writeServer("V,Currency," + response + ", " + ", ")
        serverTranTuple = readVend()
        serverLine = serverTranTuple._1
        tranResponse = serverTranTuple._2
        if (serverLine.SRMode != "S.V" | serverLine.SROperator != "CreditCheck") {
          println("S.V CreditCheck Process Error.  Program Stopped")
          quitProg()
        }

        // Test Credit Check Process Results
        if (serverLine.SRValue == "unavailable") {
          println(" _________________________")
          println("| CREDIT CHECK ERROR!     |")
          println("| Check is not available  |")
          println("| Proceed? Y/N            |")
          println("|   ( or Q = Quit)        |")
          println("|_________________________|")
          response = promptInput()
          if (response.substring(0, 1) == "N") quitProg()
          writeServer("V,CreditCheck,SkipCheck," + ", " + ", ")
          serverTranTuple = readVend()
          serverLine = serverTranTuple._1
          tranResponse = serverTranTuple._2
        }

        // Test Credit Check Process Results
        if (serverLine.SRValue == "available") {
          val CLremaining: Double = serverLine.SRValueType.toDouble - serverLine.SRValueStruct.toDouble
          println(" _________________________")
          if (CLremaining < 0) {
            println("| NO CUSTOMER CREDIT!     |")
          }
          else {
            println("| Customer Has Credit     |")
          }
          println("| Credit " + serverLine.SRValueType + (" " * (17 - serverLine.SRValueType.length)) + "|")
          println("| Spent " + serverLine.SRValueStruct + (" " * (18 - serverLine.SRValueStruct.length)) + "|")
          println("| Remaining " + CLremaining.toString + (" " * (14 - CLremaining.toString.length)) + "|")
          println("| Proceed? Y/N            |")
          println("|   ( or Q = Quit)        |")
          println("|_________________________|")
          response = promptInput()
          if (response.substring(0, 1) == "N") quitProg()
          writeServer("V,CreditCheck,Confirm," + ", " + ", ")
          serverTranTuple = readVend()
          serverLine = serverTranTuple._1
          tranResponse = serverTranTuple._2
        }

        if (serverLine.SRMode != "S.V" | serverLine.SROperator != "TranComplete") {
          println("S.V TranComplete Process Error.  Program Stopped")
          quitProg()
        }
        // Show Add Complete Message
        printTranConfirm(tranResponse)
        response = promptInput()
        if (response.substring(0, 1) == "N") sys.exit(0)
        screenPrompt = "S"
      }

//      if (screenPrompt.substring(0, 1) == "Q") quitProg()

    }  // End of on-screen while loop
  } // end of main
} // end of object
