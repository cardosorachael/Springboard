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

-- 6. List the names of students who have taken all courses offered by department v8 (deptId).
EXPLAIN  SELECT name FROM Student,
	(SELECT studId
	FROM Transcript
		WHERE crsCode IN
		(SELECT crsCode FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))
		GROUP BY studId
		HAVING COUNT(*) = 
			(SELECT COUNT(*) FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))) as alias
WHERE id = alias.studId;


-- ------------------------------------------------------------
-- Bottlenecks/How to identify them: 
-- Running EXPLAIN shows the multiple select statements used with overlapping references to he same table, as well as overlapping sub queries.
-- These nested references are redundant and can be made more efficient by inner joins and simple selects  
-- Solution 
-- Avoid correlated subqueries - used INNER JOIN 
-- Only selected columns where deptId = v8
-- join student -> transcript => course instead of student -> trasnscript -> course -> teaching 
-- -------------------------------------------------------------
SELECT s.name FROM Student As s INNER JOIN Transcript as t ON s.id = t.studId INNER JOIN Course as c ON t.crsCode = c.crsCode 
WHERE  c.deptId = @v8