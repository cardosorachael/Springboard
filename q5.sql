USE springboardopt;

-- -------------------------------------
SET @v1 = 1612521;
SET @v2 = 1145072;
SET @v3 = 1828467;
SET @v4 = 'MGT382';
SET @v5 = 'Amber Hill';
SET @v6 = 'MGT';
SET @v7 = 'EE';			  
SET @v8 = 'MAT';

-- 5. List the names of students who have taken a course from department v6 (deptId), but not v7.
EXPLAIN  SELECT * FROM Student,
	(SELECT studId FROM Transcript, Course WHERE deptId = @v6 AND Course.crsCode = Transcript.crsCode
	AND studId NOT IN
	(SELECT studId FROM Transcript, Course WHERE deptId = @v7 AND Course.crsCode = Transcript.crsCode)) as alias
WHERE Student.id = alias.studId;

-- -------------------------------------------------------------
-- Bottlenecks/How to identify them: 
-- Ran EXPLAIN and found the query references Course table 2 times, transcript table 2 times and Student table 1. Out of those 2 are dependent subqueries that increase computational time. 
-- The SELECT statement also grabs all columns from the table (*) which is redundant since only certain columns are desired in the final output 
-- Solution
-- Avoid correlated subqueries - used INNER JOIN 
-- Select only relevant columns (name) instead of * 
-- Only selected columns where deptId = v6 - redundant to specify not v7 
-- After running EXPLAIN, found that the query does a simple select on couse, transcript and students one time each 

SELECT s.name FROM Student As s INNER JOIN Transcript as t ON s.id = t.studId INNER JOIN Course as c ON t.crsCode = c.crsCode 
WHERE  c.deptId = @v6; 