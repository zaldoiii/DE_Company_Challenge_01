## Task A - SQL
DAG for ETL pipeline in SQL ('./sql/')
![image](https://github.com/zaldoiii/DE_Company_Challenge_01/assets/43696167/80902123-674f-4ec4-9be8-e35768f49775)



## Task B - Python
DAG for ETL pipeline in Python ('./python/')
![image](https://github.com/zaldoiii/DE_Company_Challenge_01/assets/43696167/afd2f8a6-2c17-4d5a-85fa-5e534ce8bb56)
Notice that Python's case requirements expected to run daily in incremental mode, I choose to provide two final tables that could've been analyzed due the possible  calculation inconsistency, particularly between salary and work hours. Those tables named according to the context or formula which are 'last 30 days' and 'month-to-date' (1st day until selected date of corresponding month).


## Assumptions and Formulas
In order to get hourly salary calculation for each branch, year, month, we need to calculate total monthly salary and total work hours for each brand.

Formula I used to calculate work hours that processed for each timesheet item: 

- IF ('Checkout' AS TIME >= 'Checkin' AS TIME): 
  - work duration in seconds = to_seconds(Checkout - Checkin)
- ELSE:
  - work duration in seconds = 86400 + to_seconds(Checkout - Checkin)
- work hours = work hours in seconds / 3600

I found that there are NULL value either in 'Checkin' or 'Checkout' column on raw timesheets data given (timesheets.csv), so I calculate the **average of work duration for each corresponding date** to fill the work hours value.
