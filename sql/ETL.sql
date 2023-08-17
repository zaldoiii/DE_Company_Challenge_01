-- Create fact master table for Timesheets data as a result of transformed some Timesheets data and joined Timesheets with Employees table
DROP TABLE IF EXISTS FactTimesheetsMast;
CREATE TABLE FactTimesheetsMast AS 
	WITH StgTimesheets AS ( 
		SELECT
			t.Id AS TimesheetId,
			t.EmployeeId,
			e.BranchId,
			t.`Date`,
			t.Checkin,
			t.Checkout,
			TIME_TO_SEC(TIMEDIFF(t.Checkout, t.Checkin)) AS WorkDurationSec, 
			e.Salary
		FROM Timesheets AS t
		INNER JOIN Employees AS e ON
			(t.EmployeeId = e.Id)
	), StgDailyWorkDurationAvg AS ( 
		SELECT
			temp.`Date`,
			ROUND(AVG(temp.WorkDurationSec), 4) AS WorkDurationSecAvg 
		FROM StgTimesheets AS temp
		WHERE
			temp.Checkout IS NOT NULL
		GROUP BY
			temp.`Date`
	) SELECT
		a.TimesheetId,
		a.EmployeeId,
		a.BranchId,
		a.`Date`,
		a.Checkin,
		a.Checkout,
		IF((a.Checkin IS NULL) OR (a.Checkout IS NULL), b.WorkDurationSecAvg, a.WorkDurationSec) AS WorkDurationSec,
		IF((a.Checkin IS NULL) OR (a.Checkout IS NULL), ROUND(b.WorkDurationSecAvg/3600, 2), ROUND(a.WorkDurationSec/3600, 2)) AS Workhour,
		a.Salary
	FROM StgTimesheets AS a
	LEFT JOIN StgDailyWorkDurationAvg AS b ON
		(a.`Date` = b.`Date`)
;

-- Fill main fact table for requested analytics with overwrite mode and daily schedule
DROP TABLE IF EXISTS FactBranchHourlySalary;
CREATE TABLE FactBranchHourlySalary AS
	WITH StgBranchMonthlyWorkhour AS (
		SELECT
			DATE_FORMAT(temp.`Date`, '%Y-%m') AS YearMonth,
			temp.BranchId,
			SUM(temp.Workhour) AS Workhour
		FROM FactTimesheetsMast AS temp
		GROUP BY 
			DATE_FORMAT(temp.`Date`, '%Y-%m'),
			temp.BranchId
	), StgEmployeeBranchMonthlySalary AS ( 
		SELECT DISTINCT
			DATE_FORMAT(temp.`Date`, '%Y-%m') AS YearMonth,
			temp.EmployeeId,
			temp.BranchId,
			temp.Salary
		FROM FactTimesheetsMast AS temp
	), StgBranchMonthlySalary AS ( 
		SELECT
			temp.YearMonth,
			temp.BranchId,
			SUM(temp.Salary) AS Salary
		FROM StgEmployeeBranchMonthlySalary AS temp
		GROUP BY
			temp.YearMonth,
			temp.BranchId
	)
	SELECT
		IFNULL(A.YearMonth, B.YearMonth) AS YearMonth,
		IFNULL(A.BranchId, B.BranchId) AS BranchId,
		B.Salary,
		A.Workhour,
		ROUND(B.Salary/A.Workhour, 2) AS Salary_PerHour
	FROM StgBranchMonthlyWorkhour AS A
	INNER JOIN StgBranchMonthlySalary AS B ON
		(A.YearMonth = B.YearMonth) AND
		(A.BranchId = B.BranchId)
	ORDER BY
		YearMonth ASC,
		BranchId ASC
;