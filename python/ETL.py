# ===============================
# PACKAGES INITIALIZATION - BEGIN

import csv
import numpy as np
import os
import pandas as pd
import pathlib
import time

from datetime import datetime, timedelta
from pathlib import Path

# PACKAGES INITIALIZATION - END
# =============================




# =========================
# UTILITY FUNCTIONS - BEGIN

def get_datetime(p_date, p_time):
    if (pd.notnull(p_date) & pd.notnull(p_time)):
        v_datetime = p_date + " " + p_time
        v_datetime = datetime.strptime(v_datetime, "%Y-%m-%d %H:%M:%S")
    else:
        v_datetime = np.nan
    return v_datetime


def get_work_duration_secs(p_ts_start, p_ts_end):
    if (pd.notnull(p_ts_start) & pd.notnull(p_ts_end)):
        v_delta = (p_ts_end - p_ts_start).total_seconds()
        if (v_delta < 0):
            v_delta = 86400 + v_delta
    else:
        v_delta = np.nan
    return v_delta


def get_timesheets_mast(raw_timesheets, today_str):
    today_ds = datetime.strptime(today_str, "%Y-%m-%d")
    yesterday_ds = today_ds - timedelta(days=1)
    yesterday_str = yesterday_ds.strftime("%Y-%m-%d")

    timesheets_yesterday_notnull = raw_timesheets.copy()[
        (raw_timesheets['date'] == yesterday_str) &
        (raw_timesheets['checkin'].notnull()) &
        (raw_timesheets['checkout'].notnull())
    ]
    timesheets_yesterday_notnull['checkin_ts'] = timesheets_yesterday_notnull.apply(lambda x: get_datetime(x['date'], x['checkin']), axis=1)
    timesheets_yesterday_notnull['checkout_ts'] = timesheets_yesterday_notnull.apply(lambda x: get_datetime(x['date'], x['checkout']), axis=1)
    timesheets_yesterday_notnull['work_duration_secs'] = timesheets_yesterday_notnull.apply(lambda x: get_work_duration_secs(x['checkin_ts'], x['checkout_ts']), axis=1)
   
    timesheets_yesterday_null = raw_timesheets.copy()[
        (raw_timesheets['date'] == yesterday_str) &
        ((raw_timesheets['checkin'].isnull()) |
        (raw_timesheets['checkout'].isnull()))
    ]
    timesheets_yesterday_null['work_duration_secs'] = round(timesheets_yesterday_notnull['work_duration_secs'].mean(), 2)
   
    v_cols = ['timesheet_id', 'employee_id', 'date', 'checkin', 'checkout', 'work_duration_secs']
    timesheets_yesterday = pd.concat([
        timesheets_yesterday_notnull[v_cols],
        timesheets_yesterday_null[v_cols]
    ]).reset_index(drop=True)
    timesheets_yesterday['date'] = pd.to_datetime(timesheets_yesterday['date'], format='%Y-%m-%d')
    timesheets_yesterday['year'] = timesheets_yesterday['date'].dt.year
    timesheets_yesterday['month'] = timesheets_yesterday['date'].dt.month
    timesheets_yesterday['work_hours'] = timesheets_yesterday['work_duration_secs'].apply(lambda x: round(x/3600, 2))
    
    v_cols = ['timesheet_id', 'employee_id', 'branch_id', 'monthly_salary', 'date', 'year', 'month', 'checkin', 'checkout', 'work_duration_secs', 'work_hours']
    timesheets_employees_yesterday = pd.merge(timesheets_yesterday, employees, left_on='employee_id', right_on='employe_id', how='left')
    timesheets_employees_yesterday.rename(columns={'salary': 'monthly_salary'}, inplace=True)
   
    return timesheets_employees_yesterday[v_cols]


def get_hourly_salary_month_to_date(fact_timesheets_mast, today_str):
    today_ds = datetime.strptime(today_str, "%Y-%m-%d")
    yesterday_ds = today_ds - timedelta(days=1)
    yesterday_str = yesterday_ds.strftime("%Y-%m-%d")
    day1st_ds = datetime(yesterday_ds.year, yesterday_ds.month, 1, 0, 0, 0)
    day1st_str = day1st_ds.strftime("%Y-%m-%d")

    stg_timesheets = fact_timesheets_mast.copy()[
        (fact_timesheets_mast['date'] >= day1st_str) &
        (fact_timesheets_mast['date'] < today_str)
    ]

    stg_employee_branch = stg_timesheets[['employee_id', 'branch_id', 'monthly_salary']].drop_duplicates()
    stg_employee_branch = stg_employee_branch.groupby('branch_id')['monthly_salary'].sum().reset_index()
       
    stg_hourly_salary = stg_timesheets.groupby(['year', 'month', 'branch_id'])['work_hours'].sum().reset_index()
    stg_hourly_salary = pd.merge(stg_hourly_salary, stg_employee_branch, on='branch_id', how='inner')
    stg_hourly_salary = stg_hourly_salary[['year', 'month', 'branch_id', 'monthly_salary', 'work_hours']]
    stg_hourly_salary['salary_per_hour'] = round(stg_hourly_salary['monthly_salary'] / stg_hourly_salary['work_hours'], 2)
    stg_hourly_salary['last_update'] = today_ds

    return stg_hourly_salary


def get_hourly_salary_last30d(fact_timesheets_mast, today_str):
    today_ds = datetime.strptime(today_str, "%Y-%m-%d")
    yesterday_ds = today_ds - timedelta(days=1)
    yesterday_str = yesterday_ds.strftime("%Y-%m-%d")
    prev30d_ds = today_ds - timedelta(days=30)
    prev30d_str = prev30d_ds.strftime("%Y-%m-%d")

    stg_timesheets = fact_timesheets_mast.copy()[
        (fact_timesheets_mast['date'] >= prev30d_str) &
        (fact_timesheets_mast['date'] < today_str)
    ]

    stg_employee_branch = stg_timesheets[['employee_id', 'branch_id', 'monthly_salary']].drop_duplicates()
    stg_employee_branch = stg_employee_branch.groupby(['branch_id'])['monthly_salary'].sum().reset_index()
       
    stg_hourly_salary = stg_timesheets.groupby(['branch_id'])['work_hours'].sum().reset_index()
    stg_hourly_salary.rename(columns={'work_hours': 'l30d_work_hours'}, inplace=True)
    stg_hourly_salary = pd.merge(stg_hourly_salary, stg_employee_branch, on='branch_id', how='inner')
    stg_hourly_salary['l30d_salary_per_hour'] = round(stg_hourly_salary['monthly_salary'] / stg_hourly_salary['l30d_work_hours'], 2)
    stg_hourly_salary['date_start'] = prev30d_ds
    stg_hourly_salary['date_end'] = yesterday_ds
    stg_hourly_salary['last_update'] = today_ds

    return stg_hourly_salary[['date_start', 'date_end', 'branch_id', 'monthly_salary', 'l30d_work_hours', 'l30d_salary_per_hour', 'last_update']]

# UTILITY FUNCTIONS - END
# =======================


# ================================
# VARIABLES INITIALIZATION - BEGIN

src_employees = f"{os.path.dirname(os.getcwd())}\\src\\employees.csv"
employees = pd.read_csv(src_employees, sep=',')

src_timesheets = f"{os.path.dirname(os.getcwd())}\\src\\timesheets.csv"
timesheets = pd.read_csv(src_timesheets, sep=',')

fact_timesheets_cols = {
    'timesheet_id': pd.Series(dtype='int64'),
    'employee_id': pd.Series(dtype='int64'),
    'branch_id': pd.Series(dtype='int64'),
    'monthly_salary': pd.Series(dtype='int64'),
    'date': pd.Series(dtype='str'),
    'year': pd.Series(dtype='int64'),
    'month': pd.Series(dtype='int64'),
    'checkin': pd.Series(dtype='str'),
    'checkout': pd.Series(dtype='str'),
    'work_duration_secs': pd.Series(dtype='float64'),
    'work_hours': pd.Series(dtype='float64')
}
fact_timesheets = pd.DataFrame(fact_timesheets_cols)
fact_timesheets_path = f"{os.path.dirname(os.getcwd())}\\out\\FACT_TIMESHEETS\\"

fact_hourly_salary_month_to_date_cols = {
    'year': pd.Series(dtype='int64'),
    'month': pd.Series(dtype='int64'),
    'branch_id': pd.Series(dtype='int64'),
    'monthly_salary': pd.Series(dtype='int64'),
    'work_hours': pd.Series(dtype='float64'),
    'salary_per_hour': pd.Series(dtype='float64'),
    'last_update': pd.Series(dtype='str')
}
fact_hourly_salary_month_to_date = pd.DataFrame(fact_hourly_salary_month_to_date_cols)
fact_hourly_salary_month_to_date_path = f"{os.path.dirname(os.getcwd())}\\out\\FACT_HOURLY_SALARY_MONTH_TO_DATE\\"

fact_hourly_salary_last30d_cols = {
    'date_start': pd.Series(dtype='str'),
    'date_end': pd.Series(dtype='str'),
    'branch_id': pd.Series(dtype='int64'),
    'monthly_salary': pd.Series(dtype='int64'),
    'l30d_work_hours': pd.Series(dtype='float64'),
    'l30d_salary_per_hour': pd.Series(dtype='float64'),
    'last_update': pd.Series(dtype='str')
}
fact_hourly_salary_last30d = pd.DataFrame(fact_hourly_salary_last30d_cols)
fact_hourly_salary_last30d_path = f"{os.path.dirname(os.getcwd())}\\out\\FACT_HOURLY_SALARY_LAST30D\\"

# VARIABLES INITIALIZATION - END
# ==============================


# ====================
# MAIN PROGRAM - BEGIN

while True:
    print()
    print("Choose start date and end date (use \'YYYY-MM-DD\' format) to simulate scheduled job, both are inclusive.")
    print("It's recommended that the difference between start date and end date passes over a full month.")
    print()
    
    list_ds = []
    ds_start = input("Start date: ")
    ds_end = input("End date: ")
    
    try:
        ds_start = datetime.strptime(ds_start, '%Y-%m-%d')
        ds_end = datetime.strptime(ds_end, '%Y-%m-%d')
    
    except Exception as e:
        print()
        print(e)

    else:
        if (ds_end >= ds_start):
            if (not os.path.exists(fact_timesheets_path)):
                os.makedirs(fact_timesheets_path)
            if (not os.path.exists(fact_hourly_salary_month_to_date_path)):
                os.makedirs(fact_hourly_salary_month_to_date_path)
            if (not os.path.exists(fact_hourly_salary_last30d_path)):
                os.makedirs(fact_hourly_salary_last30d_path)

            while (ds_start <= ds_end):
                list_ds.append(ds_start.strftime('%Y-%m-%d'))
                ds_start += timedelta(days=1)
            
            ts_log_begin = time.time()
            for dd in list_ds:
                ds_yyyymmdd = dd.replace("-", "")
                
                out = get_timesheets_mast(timesheets, dd)
                out.to_csv(f"{fact_timesheets_path}\\FACT_TIMESHEETS_{ds_yyyymmdd}.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
                fact_timesheets = pd.concat([fact_timesheets, out], ignore_index=True)
                
                out = get_hourly_salary_month_to_date(fact_timesheets, dd)
                out.to_csv(f"{fact_hourly_salary_month_to_date_path}\\FACT_HOURLY_SALARY_MONTH_TO_DATE_{ds_yyyymmdd}.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
                fact_hourly_salary_month_to_date = pd.concat([fact_hourly_salary_month_to_date, out], ignore_index=True)
                
                out = get_hourly_salary_last30d(fact_timesheets, dd)
                out.to_csv(f"{fact_hourly_salary_last30d_path}\\FACT_HOURLY_SALARY_LAST30D_{ds_yyyymmdd}.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
                fact_hourly_salary_last30d = pd.concat([fact_hourly_salary_last30d, out], ignore_index=True)
            ts_log_end = time.time()

            print(f"Simulation finished in {round(ts_log_end-ts_log_begin, 3)} seconds.")

        else:
            print()
            print("End date can't be set backwards prior to start date!")

# =======
# END ALL