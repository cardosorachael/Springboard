-- SELECT r.referee_name,
--        sc.country_name,
--        sv.venue_name,
--        count(mm.match_no)
-- FROM match_mast mm
-- JOIN referee_mast r ON mm.referee_id=r.referee_id
-- JOIN soccer_country sc ON r.country_id=sc.country_id
-- JOIN soccer_venue sv ON mm.venue_id=sv.venue_id
-- GROUP BY r.referee_name,
--          country_name,
--          venue_name
-- ORDER BY referee_name;

select country_name,count(distinct ass_ref_id) as asst_ref_cnt 
from euro_cup_2016.asst_referee_mast arm,
      euro_cup_2016.soccer_country sc
where arm.country_id = sc.country_id
group by country_name
order by asst_ref_cnt desc
limit 1