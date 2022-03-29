package com.Revature


import au.com.bytecode.opencsv.CSVWriter

import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.plans.logical.DropTable
import org.apache.spark.sql.functions.{asc, desc}
import io.delta.tables._

import scala.io.StdIn._


import scala.util.control.Breaks.{break, breakable}

object test1 {


  def main(args: Array[String]): Unit = {

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


    def getNextUserId(): Int = {
      var result: Int = 1;
      val df = spark.sql("select max(ID) from allUsers");
      if (!df.isEmpty)
        if (!df.take(1)(0).isNullAt(0))
          result = df.take(1)(0).getInt(0) + 1

      return result;
    }

    def createUsr() {
      println("You're creating a new account. Please type in a username and password")
      var username = readLine("Username: ")
      val password = readLine("Password: ")
      val password2 = readLine("Re-Enter your Password: ")

      if (!username.equals("") && !password.equals("") && password2.equals(password)) {
        spark.sql("CREATE TABLE IF NOT EXISTS allUsers(ID INT, Username STRING, Password STRING, Admin BOOLEAN);")
        spark.sql(s"INSERT INTO TABLE allUsers VALUES($getNextUserId, '$username','$password',false);")

      }
      else {
        println("Your password doesn't match. Please re-enter your username and password")
        createUsr
      }
      println("Your account has been created. \nEntering Login Screen... ")
    }


    def start {
      val read = readLine("Welcome to MyAnimeList API! \nPlease choose one of the following options: \n1. Login \n2. Create New Account \n3. Quit \n")
      if (read.toLong == 2) {
        createUsr
      } else if (read.toLong == 1) {
        println("Entering Login Screen...")
      }
      else if (read.toLong == 3) {
        break()
      }
      else {
        println("invalid input")
        start
      }

    }
    var lol = 1
    def login(username: String, password: String): Unit = {


      val test = spark.sql(s"SELECT Username FROM allUsers WHERE Username = '$username' AND Password = '$password'")

      if (test.isEmpty) {
        println("Username doesn't exists or invalid password. Please try again")
        lol-=1
      } else {
        println("You have logged in successfully. Entering Home Screen...")
        if(lol<=0){
          lol+=1
        }
      }
    }


    breakable {
      while (true) {
        start
//        val tables = spark.table("allUsers")
//        tables.sort(asc("ID")).show
        println("Please login")
        val username = readLine("Enter your Username: ")
        val password = readLine("Enter your password: ")
       login(username, password)
        while (lol == 1) {
          val home = readLine("Welcome to Home Screen. Please choose one of the followings to continue: \n1. Execute Query \n2. Enter my profile \n3. Logout \n").toLong
          if (home == 1) {
            var num = 1
            if (num <= 0) {
              num += 1
            }
            while (num == 1) {
              println("Please select a query to execute, or enter 0 to return to the Home menu")

              val selection = readLine("1. Top anime \n2. Most common genres \n3. Search Anime \n4. Anime with most members \n5. List of different types of Anime \n6. Highest anime type ratings \n").toLong
              if (selection == 1) {
                println("What are the most popular anime?")
                spark.sql("DROP table IF EXISTS topAnime")
                spark.sql("CREATE TABLE IF NOT EXISTS topAnime(id INT,Name STRING, Genre STRING, Type STRING, Episodes INT, Rating FLOAT, Members LONG)row format delimited fields terminated by ','");
                spark.sql("LOAD DATA LOCAL INPATH 'anime.csv' OVERWRITE INTO TABLE topAnime")
                spark.sql("SELECT * FROM topAnime ORDER BY Rating DESC").show(10, false)
              } else if (selection == 2) {
                println("What are the most common genres of anime?")

                spark.sql("DROP TABLE IF EXISTS genre;")
                spark.sql("CREATE TABLE genre(genres STRING) row format delimited fields terminated by ',' stored as textfile;")
                spark.sql("LOAD DATA LOCAL INPATH 'U:/home/newyorkheryo/genre.txt' overwrite into table genre;")
                spark.sql("SELECT genres, count(*) AS count FROM genre GROUP BY genres;").sort(desc("count")).show()
              } else if (selection == 3) {

                val search = readLine("Would you like to search by? \n1. Name \n2. ID \n3. Go back \n")
                if (search.toLong == 2) {
                  val searchID = readLine("Type in the anime ID you would like to search: ").toLong
                  spark.sql(s"SELECT * FROM topAnime WHERE id = $searchID").show(false)
                }
                else if (search.toLong == 1) {
                  val searchName = readLine("Type the name or part of the name to find what you're looking for: ")
                  spark.sql(s"SELECT * FROM topAnime WHERE Name LIKE '%$searchName%'").show(false)
                }
              } else if (selection == 4) {
                println("List the top animes with the most members?")

                spark.sql("SELECT * FROM topAnime Order BY Members DESC").show(false)
              } else if (selection == 5) {
                println("Give a List of all the different types of anime")

                spark.sql("DROP TABLE IF EXISTS anime_types;")
                spark.sql("CREATE TABLE IF NOT EXISTS anime_types(id INT,name STRING, rating STRING) PARTITIONED BY(type STRING);")
                spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
                spark.sql("INSERT OVERWRITE TABLE anime_types PARTITION(type)SELECT id, name, rating, type from topAnime;")
                spark.sql("ALTER TABLE anime_types DROP PARTITION (type='__HIVE_DEFAULT_PARTITION__');")
                spark.sql("SHOW PARTITIONS anime_types;").show(false)
              } else if (selection == 6) {
                println("Which type of anime has a higher average rating?")

                spark.sql("SELECT type, round(avg(rating),2) AS avg_ratings FROM anime_types GROUP BY type;").sort(desc("avg_ratings")).show

                println("Bucket by type")
                var dfd = spark.read.option("header", true).format("csv").csv("U:/home/newyorkheryo/anime2.csv")
                spark.sql("DROP TABLE IF EXISTS bucketed")
                val b = dfd.write.bucketBy(4, "Type").sortBy("Rating").saveAsTable("bucketed")
                val table = spark.table("bucketed")
                table.show(5, false)
              }
              else {
                num -= 1
              }
            }
          }
          else if (home == 2) {
            val df8 = spark.sql(s"SELECT ID,Admin from allUsers WHERE Username = '$username' AND Password = '$password'")
            val insert: (Int, Boolean) = (df8.take(1)(0).getInt(0), df8.take(1)(0).getBoolean(1))
            println("Welcome to your profile! You can create/add your own anime list here")
            var run = 1
            if(run<=0){
              run+=1
            }
            var abc = 1

            if (insert._2 == true) {
              if(abc<=0){
                abc+=1
              }
              while(abc ==1){
              println("______________________________________________________________________________")
              println("You are logged in as admin. You can delete accounts and/or give admin to other accounts")
              val readInput = readLine("1. Delete an account \n2. Give admin to another account \n3. Show all accounts \n4. Exit admin \n")
              if (readInput.toLong == 1) {

                println("Which account would you like to delete?")
                val deleteAccount = readLine("Account username: ")
                spark.sql("DROP TABLE IF EXISTS allUserstemp")
                spark.sql("CREATE TABLE IF NOT EXISTS allUserstemp(ID INT, Username STRING, Password STRING, Admin BOOLEAN)")
                spark.sql(s"INSERT INTO allUserstemp SELECT * FROM allUsers WHERE Username != '$deleteAccount'")
                spark.sql("DROP TABLE IF EXISTS allUsers")
                spark.sql("ALTER TABLE allUserstemp rename to allUsers")
                spark.sql(s"SELECT ID, Username FROM allUsers").show()
                println("The account has been deleted")
                println("______________________________________________________________________________")
              } else if (readInput.toLong == 2) {

                val giveAdmin = readLine("Which account would you like to give admin to? \nAccount username: ")
                val dfp = spark.sql(s"SELECT ID,Password from allUsers WHERE Username = '$giveAdmin'")
                val getPassword: (Int, String) = (dfp.take(1)(0).getInt(0), dfp.take(1)(0).getString(1))
                spark.sql("DROP TABLE IF EXISTS allUserstemp")
                spark.sql("CREATE TABLE IF NOT EXISTS allUserstemp(ID INT, Username STRING, Password STRING, Admin BOOLEAN)")
                spark.sql(s"INSERT INTO allUserstemp SELECT * FROM allUsers WHERE Username != '$giveAdmin'")
                spark.sql("DROP TABLE IF EXISTS allUsers")
                spark.sql("ALTER TABLE allUserstemp rename to allUsers")
                spark.sql(s"INSERT INTO TABLE allUsers VALUES(${getPassword._1},'$giveAdmin', '${getPassword._2}',true)")
                spark.sql(s"SELECT Username, Admin FROM allUsers WHERE Username = '$giveAdmin'").show()
                println(s"You've successfully given $giveAdmin admin level permission")
                println("______________________________________________________________________________")
              }
              else if (readInput.toLong ==3){
                spark.sql("SELECT * FROM allUsers ORDER BY ID").show
              }else{
                println("______________________________________________________________________________")
                abc-=1
              }
            }}
            while (run == 1) {
              println("Enter 0 to return to the Home menu")
              spark.sql(s"CREATE TABLE IF NOT EXISTS table${insert._1}(AnimeID STRING, Your_Rating FLOAT, Notes STRING)")

              val read = readLine("Choose one of the following options: \n1. Show my list \n2. Add to list \n3. Change username or password \n4. Delete List \n5. Search Anime \n")
              if (read.toLong == 1) {
                spark.sql(s"SELECT * FROM table${insert._1};").show(false)
              } else if (read.toLong == 2) {
                println("You chose to add to your watched list")
                val id = readLine("Type the anime ID: ")
                val rating = readLine("Give your rating: ").toFloat
                val note = readLine("Notes: ")
                spark.sql(s"INSERT INTO table${insert._1} VALUES('$id',$rating,'$note')")
                println("You've successfully added a new anime to your list")
                spark.sql(s"SELECT * FROM table${insert._1};").show(false)
              } else if (read.toLong == 3) {
                val u = readLine("What would you like to change? \n1. Change Username \n2. Change Password \n3. Exit \n")
                if (u.toLong == 1) {
                  val newUsername = readLine("Type in your new Username: ")
                  spark.sql("DROP TABLE IF EXISTS allUserstemp")
                  spark.sql("CREATE TABLE IF NOT EXISTS allUserstemp(ID INT, Username STRING, Password STRING, Admin BOOLEAN)")
                  spark.sql(s"INSERT INTO allUserstemp SELECT * FROM allUsers WHERE Username != '$username'")
                  spark.sql("DROP TABLE IF EXISTS allUsers")
                  spark.sql("ALTER TABLE allUserstemp rename to allUsers")
                  spark.sql(s"INSERT INTO TABLE allUsers VALUES(${insert._1},'$newUsername', '$password',${insert._2})")

                  println("Your username has been changed successfully")
                } else if (u.toLong == 2) {
                  val newPassword = readLine("You chose to change your Password. \nType in your new Password: ")
                  spark.sql("DROP TABLE IF EXISTS allUserstemp")
                  spark.sql(s"CREATE TABLE IF NOT EXISTS allUserstemp(ID INT, Username STRING, Password STRING, Admin BOOLEAN)")
                  spark.sql(s"INSERT INTO allUserstemp SELECT * FROM allUsers WHERE Password != '$password'")
                  spark.sql("DROP TABLE IF EXISTS allUsers")
                  spark.sql(s"ALTER TABLE allUserstemp rename to allUsers")
                  spark.sql(s"INSERT INTO TABLE allUsers VALUES(${insert._1},'$username', '$newPassword',${insert._2})")

                  println("Your password has been changed successfully")
                }
              } else if (read.toLong == 4) {
                val deleteList = readLine("Choose one of the following: \n1. Delete my entire list \n2. Delete a row from list \n").toLong
                if(deleteList==1){
                spark.sql(s"DROP TABLE IF EXISTS table${insert._1}")
                println("Your list has been deleted")
                }else if(deleteList ==2){
                  val deleteID = readLine("Type the anime ID in your list you would like to delete: ").toLong
                  spark.sql(s"DROP TABLE IF EXISTS tempList")
                  spark.sql("CREATE TABLE IF NOT EXISTS tempList(AnimeID STRING, Your_Rating FLOAT, Notes STRING)")
                  spark.sql(s"INSERT INTO tempList SELECT * FROM table${insert._1} WHERE AnimeID != $deleteID")
                  spark.sql(s"DROP TABLE IF EXISTS table${insert._1}")
                  spark.sql(s"ALTER TABLE tempList rename to table${insert._1}")
                  spark.sql(s"SELECT * FROM table${insert._1}").show(false)
                  println(s"You've successfully deleted AnimeID $deleteID from your list")
                }
              } else if (read.toLong == 5) {
                val search = readLine("Would you like to search by? \n1. Name \n2. ID \n3. Go back \n")
                if (search.toLong == 2) {
                  val searchID = readLine("Type in the anime ID you would like to search: ").toLong
                  spark.sql(s"SELECT * FROM topAnime WHERE id = $searchID").show(false)
                }
                else if (search.toLong == 1) {
                  val searchName = readLine("Type the name or part of the name to find what you're looking for: ")
                  spark.sql(s"SELECT * FROM topAnime WHERE Name LIKE '%$searchName%'").show(false)
                }
              }
              else {
              run -= 1
              }
            }
          }
          else {
            lol -= 1
            println("Logging off...")
            println("\n ")
          }
        }
      }
    }
  }
}