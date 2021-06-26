SELECT player_name
FROM goal_details g
JOIN player_mast b ON g.player_id=b.player_id
JOIN soccer_country sc ON g.team_id=sc.country_id
WHERE posi_to_play='DF'
ORDER BY player_name;