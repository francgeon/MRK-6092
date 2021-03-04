#!/usr/bin/env python3

# Required Modules:
import mysql.connector

# Globals

def _defGlobals():
    global the_credentials
    global the_table

    the_user = 'fRancis'
    the_password = '1fRancisPW$'
    the_database = 'sentiment'
    the_table = 'francis'
    the_ip_address = '132.203.46.219'
    the_credentials = {
            'user': the_user,
            'password': the_password,
            'db': the_database,
            'use_unicode': True,
            'charset': 'utf8',
            'host': the_ip_address,
            'port': 3307,
            'connection_timeout': 6000,
            'allow_local_infile': True
        }

# Functions
def _execute(the_query, the_type=''):
    the_DB = mysql.connector.connect(**the_credentials)
    c = the_DB.cursor(buffered=True)
    c.execute(the_query)
    the_result = ''

    try:
        if 'fetchone' in the_type:
            the_result = c.fetchone()
            if the_result:
                the_result = the_result[0]
        if 'fetchall' in the_type:
            the_result = c.fetchall()
    except mysql.connector.Error:
        pass

    c.close()
    the_DB.commit()
    the_DB.close()

    return the_result


# process logic
if __name__== "__main__":
    # 0. define globals
    # 1. connect (and get the number of lines of the tripAdvisor table)
    # 2. list first line
    # 3. CREATE a new table
    # 4. INSERT rows

    # 0. define globals
    _defGlobals()

    # 1. Get the number of line of the tripAdvisor table
    the_result = _execute("SELECT COUNT(*) FROM tripAdvisor", "fetchone")
    print(the_result)

    # 2. list first line
    the_lines = _execute("SELECT * from tripAdvisor LIMIT 1", "fetchall")
    for line in the_lines:
        print(line)

    # 3. CREATE a new table
    print("dropping")
    _execute(f"DROP TABLE IF EXISTS {the_table}")
    print("creating")
    _execute(f"CREATE TABLE {the_table} LIKE tripAdvisor")

    # 4. INSERT rows
    print("inserting")
    _execute(f"INSERT INTO {the_table} SELECT * FROM tripAdvisor")
    the_lines = _execute(f"SELECT * FROM {the_table} LIMIT 1", "fetchall")
    for line in the_lines:
        print(line)

    # all done
    print("Done")



#import datetime

#the_name = "Francis"
#the_time = str(datetime.datetime.now())[11:19]
#the_victory_cry = "a réussi à faire tourner son premier script"

#print()
#print()
#print(f"Il est {the_time} et {the_name} {the_victory_cry}!")
#print()
#print()


