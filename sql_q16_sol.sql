SELECT r.referee_name,
       sc.country_name,
       sv.venue_name,
       count(mm.match_no)
FROM match_mast as mm
JOIN referee_mast r ON mm.referee_id=r.referee_id
JOIN soccer_country sc ON r.country_id=sc.country_id
JOIN soccer_venue sv ON mm.venue_id=sv.venue_id
GROUP BY r.referee_name,
         country_name,
         venue_name
ORDER BY referee_name;