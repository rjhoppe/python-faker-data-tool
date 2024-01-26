# python-faker-data-tool
A program I created to generate fake data using the Faker library and then load that data to a dev MySQL database. I wanted to force myself to heavily use OOP to structure the project.

## Info

This program can be used to generate data asynchronously and then output that data to a data.csv file. Once the file has been generated, the program connects to a locally hosted MySQL db instance.

Once connected, the program checks for a specific database and table belonging to that database. If one or neither of those are found, the program creates them for you.

Finally, the program iterates through the rows of data in the csv and uploads them to MySQL.  

## Technologies Used:
* Python
* Faker Python library
* pandas
* MySQL
* SQL