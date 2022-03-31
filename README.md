# Project1 myanimelist API

Project1 uses the myanimelist.net API dataset downloaded to a csv file, storing it in hive, and analyzes 
it using a CLI application written in Scala with a user management and access system for basic and admin users.
Each user will have their own list to access and implements all CRUD operations to that list.

## Technologies
- Java - version 1.8.0_311
- Scala - version 2.12.15
- Spark - version 3.1.2
- Spark SQL - version 3.1.2
- Hive - version 3.1.2
- HDFS - version 3.3.0
- Git + GitHub

## Features
- Login and new basic user creation
- Restricted access for basic users, full access for admins
- Execute analytical SQL queries
- Cannot add, update, or delete data from queries
- Promote basic users to admins, and delete basic users
- Can change own username and password
- Default admin account created on initialization
- Hive tables implement bucketing and partitioning
- Creates a personal list for each user 
- Can add or delete data from list
- Search feature to find specfic data or a list of data from a database

## Getting Started
- (Note, these instructions only support Windows 10 and above)
- First, download and install Git
- For Windows, navigate to https://git-scm.com/download/win and install
- Run the following Git command to create a copy of the project repository using either Command Prompt or PowerShell:
- git clone https://github.com/newyorkher/Project1
- Install Java
    - Navigate to https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html and install
- Install IntelliJ
    - Navigate to https://www.jetbrains.com/idea/download/download-thanks.html?platform=windows&code=IIC and install
- Download https://www.kaggle.com/datasets/CooperUnion/anime-recommendations-database

## Usage
