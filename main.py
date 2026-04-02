import report_by_event
import report_by_dates

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
    report_by_event.report_by_event(username, password, catalog_id, catalog_name, hostname)

elif (option == "2"):
    print("Date Start (inclusive) (yyyy-mm-dd):")
    date_from = input()
    print("Date End (exclusive) (yyyy-mm-dd):")
    date_to = input()
    report_by_dates.report_by_dates(username, password, date_from, date_to, hostname)

else:
    print("Invalid Input")