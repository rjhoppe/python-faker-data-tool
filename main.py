from faker import Faker
import random
import csv
import asyncio
import os
import mysql.connector
from mysql.connector import connection
from mysql.connector import errorcode
from dotenv import load_dotenv
import warnings
# Silence pandas warning about Pyarrow deprecation
warnings.simplefilter(action='ignore', category=DeprecationWarning)
import pandas as pd

load_dotenv()

fake = Faker()

status_vals = ['Rejected', 'Active', 'Active', 'Active', 'Closed', 'Closed', 
               'Investigator Assigned', 'Courted Scheduled']

pd_vals = ['Peddleton PD', 'Acadia PD', ' Snowview PD', 'Goldburgh PD',
           'Casterley PD', 'North Lexington PD', 'Hardford PD', 'Lakegaard PD', 'Rosefolke PD',
           'Greenhove PD', 'Springhof PD', 'Blackwood PD', 'Canyon Valley PD']

df_cols = ['case_id', 'case_number', 'case_status', 'case_pd', 'case_assignee', 
           'last_date_modified', 'last_modified_by', 'case_num_of_victims', 'case_victim_names', 
           'case_victim_emails', 'case_victim_phone_nums']

df = pd.DataFrame(columns=df_cols)

create_table = ("CREATE TABLE cases (CaseID VARCHAR(255), CaseNumber VARCHAR(255), \
                CaseStatus VARCHAR(255), PoliceDept VARCHAR(255), Assignee VARCHAR(255), \
                LastDateModified VARCHAR(255), LastModifiedBy VARCHAR(255), \
                NumberOfVictims VARCHAR(255), VictimNames VARCHAR(255), \
                VictimEmails VARCHAR(255), VictimPhoneNumbers VARCHAR(255));")

class Victim:
  def __init__(self):
    self.name = fake.first_name() + ' ' + fake.last_name()
    self.phone_number = fake.msisdn()
    self.email = f"{self.name[0]}.{self.name.split(' ')[1:][0]}@{fake.domain_name()}".lower()

class Case:
  def __init__(self):
    self.case_id = fake.unique.random_number(digits=8)
    self.case_number = f"2023-39-INC-23{fake.random_number(digits=6)}"
    self.status = random.choice(status_vals)
    self.police_department = random.choice(pd_vals)
    self.assignee = fake.first_name() + ' ' + fake.last_name() + ' ' + '('+str(fake.random_number(digits=4))+')'
    self.last_date_modified = fake.date_this_decade()
    self.last_modified_by = self.assignee
    self.number_of_victims = random.randrange(1, 4)
    self.victim_names = None
    self.victim_emails = None
    self.victim_phone_numbers = None

  def gen_victim_info(self, number_of_victims):
    for _ in range(number_of_victims):
      _ = Victim()
      if self.victim_names is None:
        self.victim_names = _.name
      else:
        self.victim_names += ', ' + _.name
      if self.victim_phone_numbers is None:
        self.victim_phone_numbers = _.phone_number
      else:
        self.victim_phone_numbers += ', ' + _.phone_number
      if self.victim_emails is None:
        self.victim_emails = _.email
      else:
        self.victim_emails += ', ' + _.email

class Database:
  def __init__(self, type):
    self.type = type
    self.user = os.getenv("DB_USER")
    self.password = os.getenv("DB_PW")
    self.host = os.getenv("DB_HOST")
    self.database = os.getenv("DB_NAME")

  def db_init(self, conn):
    cursor = conn.cursor()
    print("Creating db...")
    db_create = (f"CREATE DATABASE {self.database}")
    cursor.execute(db_create)
    conn.commit()
    print("Creating table...")
    cursor.execute(create_table)
    conn.commit()
    return self.db_conn()
  
  def table_init(self, conn):
    cursor = conn.cursor()
    print("Creating table...")
    cursor.execute(create_table)
    conn.commit()
    return self.db_load_data()

  def db_conn(self):
    try:
      conn = connection.MySQLConnection(user=self.user, password=self.password,
                                        host=self.host, database=self.database)
      print(f"Connected to database: {self.database}")
      cursor = conn.cursor()
      print("Looking for table 'cases'")
      table_check = ("SHOW TABLES")
      cursor.execute(table_check)
      result = cursor.fetchall()
      if len(result) < 1:
        print("Table 'cases' not found.")
        return self.table_init(conn)
      else:
        print("Table 'cases' found")
        return self.db_load_data()

    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
        try:
          self.db_init(conn=connection.MySQLConnection(user=self.user, password=self.password,
                                        host=self.host))
        except mysql.connector.Error as err:
          print(err)   

  def db_load_data(self):
    conn = connection.MySQLConnection(user=self.user, password=self.password,
                      host=self.host, database=self.database)
    cursor = conn.cursor()
    csv_data = csv.reader(open('data.csv'))
    next(csv_data)
    print("Importing file data...")
    for row in csv_data:
      val = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
      insert_data = (f"INSERT INTO {self.database}.cases \
        (CaseID, CaseNumber, CaseStatus, PoliceDept, Assignee, \
        LastDateModified, LastModifiedBy, NumberOfVictims, VictimNames, \
        VictimEmails, VictimPhoneNumbers) \
        VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )")
      cursor.execute(insert_data, val)
      conn.commit()
    print("Job complete")
    cursor.close()

async def gen_data():
  # Change amount of data you would like to generate here
  while len(df.index) < 50:
    _ = Case()
    _.gen_victim_info(_.number_of_victims)
    df.loc[len(df)] = [_.case_id, _.case_number, _.status, _.police_department, 
                  _.last_date_modified, _.last_modified_by, _.assignee, _.number_of_victims, 
                  _.victim_names, _.victim_emails, _.victim_phone_numbers]
  return
    
async def main():
  async with asyncio.TaskGroup() as tg:
    tg.create_task(gen_data())
    tg.create_task(gen_data())
    tg.create_task(gen_data())
    tg.create_task(gen_data())
    tg.create_task(gen_data())

  df.to_csv('data.csv', encoding='utf-8', index=False)
  
asyncio.run(main())

MySQL = Database(mysql)
MySQL.db_conn()
print("Program complete")

    