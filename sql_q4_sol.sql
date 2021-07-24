-- SELECT play_half,play_schedule,COUNT(*) FROM euro_cup_2016.player_in_out WHERE in_out='I'
-- GROUP BY play_half,play_schedule
-- ORDER BY play_half,play_schedule,count(*) DESC;

SELECT GROUP_CONCAT(subs_count) AS subs_list
FROM (
	SELECT match_no, ROUND(COUNT(*)/2, 0) AS subs_count
	FROM euro_cup_2016.player_in_out
	GROUP BY match_no
) AS subs_table