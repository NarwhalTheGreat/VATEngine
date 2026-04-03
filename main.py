import numpy as np
import pandas as pd
from numpy.f2py.auxfuncs import throw_error
from sqlalchemy import create_engine, text
import report_by_event
import report_by_dates
import os
import sys

try:
    print("what would you like to do? \n 1 for Report by event \n 2 for Report by date")
    option = input()
    print("Please provide the hostname")
    hostname = input()
    print("Please provide your credentials for accessing the database")
    print("Username:")
    username = input()
    print("Password:")
    password = input()

    if (option == "1"):
        print("Catalogue ID:")
        # '7ef69632-0ff6-48ba-b0d2-c53b5c4a7c7c'
        catalog_id = input()
        print("Catalogue Name:")
        catalog_name = input()
        result = report_by_event.report_by_event(username, password, catalog_id, catalog_name, hostname)

        # this gets the folder where the .exe is located
        if getattr(sys, 'frozen', False):
            base_path = os.path.dirname(sys.executable)  # when running as .exe
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))  # when running in PyCharm

        output_path = os.path.join(base_path, 'output.csv')


    elif (option == "2"):
        print("Date Start (inclusive) (yyyy-mm-dd):")
        date_from = input()
        print("Date End (exclusive) (yyyy-mm-dd):")
        date_to = input()
        result = report_by_dates.report_by_dates(username, password, date_from, date_to, hostname)

        # this gets the folder where the .exe is located
        if getattr(sys, 'frozen', False):
            base_path = os.path.dirname(sys.executable)  # when running as .exe
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))  # when running in PyCharm

        output_path = os.path.join(base_path, 'output.csv')




    else:
        print("Invalid Input")

    result.to_csv(output_path, index=False)
    print(f"File saved to: {output_path}")

except Exception as e:
    print(f"Something went wrong: {e}")

finally:
    input("Press Enter to close...")