## Task A - SQL
DAG for ETL pipeline in SQL
![image](https://github.com/zaldoiii/DE_Company_Challenge_01/assets/43696167/80902123-674f-4ec4-9be8-e35768f49775)



## Task B - Python
DAG for ETL pipeline in SQL
![image](https://github.com/zaldoiii/DE_Company_Challenge_01/assets/43696167/0372110f-6527-4c6d-ad9d-e15dec05065b)


## Assumptions and Formulas
In order to get hourly salary calculation for each branch, year, month, we need to calculate total monthly salary and total work hours for each brand.

Formula I used to calculate work hours that processed for each timesheet item: 

IF ('Checkout' AS TIME >= 'Checkin' AS TIME): 
  work duration in seconds = to_seconds(Checkout - Checkin)
ELSE:
  work duration in seconds = 86400 + to_seconds(Checkout - Checkin)
work hours = work hours in seconds / 3600

I found that there are NULL value either in 'Checkin' or 'Checkout' column on raw timesheets data given (timesheets.csv), so I calculate the average of work duration for those day to fill the work hours value.
