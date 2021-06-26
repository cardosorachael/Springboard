SELECT match_no, team_id,
       COUNT(*) shots
FROM penalty_shootout
GROUP BY match_no
HAVING COUNT(*)=
  (SELECT MAX(shots)
   FROM
     (SELECT COUNT(*) shots
      FROM penalty_shootout
      GROUP BY match_no) inner_result)
