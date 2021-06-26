SELECT match_no,country_name,player_name,jersey_no,time_in_out
FROM player_in_out p
JOIN player_mast pm ON p.player_id=pm.player_id
JOIN soccer_country sc ON pm.team_id=sc.country_id
WHERE p.in_out='I'
AND p.play_schedule='NT'
AND p.play_half=1
ORDER BY match_no;