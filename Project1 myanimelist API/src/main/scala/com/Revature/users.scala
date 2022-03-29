package com.Revature

import au.com.bytecode.opencsv.CSVWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.{BufferedWriter, FileWriter}
import scala.io.StdIn._


  object users extends App{


      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession
        .builder
        .appName("hello hive")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      println("created spark session")
      val df = spark.read.format("csv").option("header", true).load("U:/home/newyorkheryo/username1.csv").toDF()


//        spark.sql("DROP TABLE IF EXISTS test1;")
//    val a = readLine("Type name: ")
    val df1 = spark.read.format("csv").option("header",true).load("U:/home/newyorkheryo/test2.csv").toDF
//    val df2 = spark.createDataFrame(Seq(
//      (s"$a", 23),
//      ("Shelby", 44),
//      ("Charles", 31))).toDF()



//    df1.union(df2).write.mode("Append").saveAsTable("test1")
    val test1 = spark.table("test1")








      def createUsr() {
        var username = readLine("Username: ")
        val password = readLine("Password: ")
        val password2 = readLine("Re-Enter your Password: ")
        if (!username.equals("") && !password.equals("") && password2.equals(password)) {
        spark.sql("CREATE TABLE IF NOT EXISTS accounts(Username STRING, Password STRING, Admin BOOLEAN);")
          spark.sql(s"INSERT INTO TABLE accounts VALUES('$username','$password',true)")
          spark.sql("SELECT * FROM accounts").show()


//          works if I have two collums in csv and in this data frame
//          val out = new BufferedWriter(new FileWriter("U:/home/newyorkheryo/username.csv"))
//          val writer = new CSVWriter(out)
//          val usrNameSchema = Array(username, password)
//          writer.writeNext(usrNameSchema)
//          out.close
//
//          val newRow = spark.read.format("csv").load("U:/home/newyorkheryo/username.csv").toDF()
//
//          //        spark.sql("DROP TABLE IF EXISTS allUsers;")
//          df.union(newRow).write.mode("Append").saveAsTable("allUsers")
//          val table = spark.table("allUsers")
//          table.show


        }
        else {
          println("Your username or password doesn't match. Please re-enter your username and password")
          createUsr
        }
        println("Your account has been created. Please login")

      }

      def login(username:String, password:String){

        spark.sql("SELECT * FROM accounts").show()
        //put some admin level into the query and create new table with permission level
        //fix exception that throws when you type the wrong username and password
        //get the login function to work
        val test = spark.sql(s"SELECT Admin FROM accounts WHERE Username = '$username' AND Password = '$password'")
        if (test.head().anyNull) {
          println("Invalid information. Please try again")
          login(username,password)
        }
        else {
          println("You have logged in successfully")

        }
      }

//    def start {
//      val read = readLine("Welcome! Choose to login or create new account \n1.Login \n2.Create New Account \n")
//      if (read.toLong == 2) {
//        createUsr
//        login
//      } else if (read.toLong == 1) {
//        login
//      }
//      else {
//        println("invalid input")
//        start
//      }
//    }

      //      val name = readLine("name: ")
      //      val lastname = readLine("lastname: ")
      //      val id = 1
      //      spark.sql("DROP TABLE IF EXISTS test;")
      //      spark.sql("CREATE TABLE IF NOT EXISTS test(name STRING, lastname STRING, ID INT);")
      //      spark.sql(s"INSERT INTO test VALUES('$name','$lastname', $id)")
      //      spark.sql("SELECT * FROM test;").show




}