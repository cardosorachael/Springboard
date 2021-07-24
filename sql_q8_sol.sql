-- SELECT match_no, team_id,
--        COUNT(*) shots
-- FROM euro_cup_2016.penalty_shootout
-- GROUP BY match_no
-- HAVING COUNT(*)=
--   (SELECT MAX(shots)
--    FROM
--      (SELECT COUNT(*) shots
--       FROM euro_cup_2016.penalty_shootout
--       GROUP BY match_no) inner_result)
--       
--       
SELECT c.country_name, COUNT(*) AS penalty_shots
FROM euro_cup_2016.soccer_country AS c, euro_cup_2016.penalty_shootout AS p
WHERE c.country_id = p.team_id
GROUP BY c.country_name
ORDER BY penalty_shots DESC
LIMIT 1
;
