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

-- 3. List the names of students who have taken course v4 (crsCode).
EXPLAIN ANALYZE SELECT name FROM Student WHERE id IN (SELECT studId FROM Transcript WHERE crsCode = @v4);
-- ------------------------------------------------------------
-- Bottlenecks/How to identify them: 
-- Used EXPLAIN and noticed multiple nested loops used in query - going through 2 tables and then inner join on another select. Also used an EXPLAIN ANALYZE statement and found the computational cost of the first qery was 414.91
-- Solution 
-- Avoid correlated subqueries, insted use an INNER JOIN. 
-- After using EXPLAIN ANALYZE again, computation time reduced to 411. For larger databases this delta will be more significant 
SELECT name from Student As s INNER JOIN Transcript as t ON s.id = t.studId WHERE t.crsCode = @v4