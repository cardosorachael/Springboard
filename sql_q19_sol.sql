SELECT count(DISTINCT player_name)
FROM match_captain mc
JOIN soccer_country sc ON mc.team_id=sc.country_id
JOIN player_mast pm ON mc.player_captain=pm.player_id
AND posi_to_play='GK';