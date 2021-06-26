SELECT match_no,country_name,goal_score FROM match_details m JOIN soccer_country s
ON m.team_id=s.country_id
WHERE goal_score > 0
ORDER BY match_no;