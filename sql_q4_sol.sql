SELECT play_half,play_schedule,COUNT(*) FROM player_in_out WHERE in_out='I'
GROUP BY play_half,play_schedule
ORDER BY play_half,play_schedule,count(*) DESC;