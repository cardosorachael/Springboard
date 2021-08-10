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

-- 2. List the names of students with id in the range of v2 (id) to v3 (inclusive).
EXPLAIN SELECT name FROM Student WHERE id BETWEEN @v2 AND @v3;
-- ---------------------------------------------------
-- Bottlenecks/How to identify them: 
-- Used an EXPLAIN clause to inpsect the performance and found a 'filtered' value of 11.11 with 400 rows accessed. This relatively low filtered value indicates unnecessary time spent analyzing rows to filter 11%.
-- Solution
-- Instead of the BETWEEN clause changed it to a numeric operation so MSQL filters with an index. After running EXPLAIN again we can see the filtered value is 100% indicating the query is highly optimized. 

SELECT name from Student WHERE @v2 <= id <= @v3;