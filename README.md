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
![](Project1%20images/created%20account.PNG)
![](Project1%20images/Logging%20in.PNG)
- All new users created will have basic level permissions and only admins are allowed to give admin to other accounts.
- After logging in successfully. The user is brought to the home screen where they can choose to enter their profile or execute queries.


![](Project1%20images/Executing%20querty.PNG)
- From here, if there are any queries written and saved to the Hive database, the user can execute them by entering the "Execute Query" menu

![](Project1%20images/Entering%20profile%20screen.PNG)
- A user menu will pop up and from here the user can choose one of the following options

![](Project1%20images/Changing%20username.PNG)
- The user will have access to change their username & password as well as access their own list. 

![](Project1%20images/Search%20Feature.PNG)
- This search feature helps users find a particular data or a list of data from a database to add to their list.

![](Project1%20images/Sword%20table.PNG)
![](Project1%20images/Adding%20to%20my%20list.PNG)
- After inputing 'Sword' as the search term. A query will be executed to search all data in the database that contains the searched input. 
- To search for a more specific data. There is a unique ID designated to each data that can also be found on the 'myanimelist.net' website

![](Project1%20images/logging%20off.PNG)
![](Project1%20images/Admin%20account.PNG)
- If an admin account is logged in. An admin menu will pop up for the admin user and has a few more options to choose from. An admin can either delete or promote a basic user to admin status (they can demote or delete existing admins)

![](Project1%20images/delete%20account.PNG)
- In this case. We will be deleting the new user created from before

![](Project1%20images/admin%20list.PNG)
- After exiting the admin menu. The user menu will open and functions the same as seen above 

![](Project1%20images/quiting%20the%20application.PNG)
- Return to home menu, log off and quit to exit the application.
