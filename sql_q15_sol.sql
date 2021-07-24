-- SELECT r.referee_name,
--        count(b.match_no) as matchno
-- FROM euro_cup_2016.player_booked p
-- JOIN match_mast b ON p.match_no=b.match_no
-- JOIN referee_mast r ON b.referee_id=r.referee_id
-- GROUP BY referee_name
-- HAVING count(b.match_no)=(SELECT max(matchno) FROM
--      (SELECT count(b.match_no) as matchno
--       FROM euro_cup_2016.player_booked p
--       JOIN match_mast b ON p.match_no=b.match_no
--       JOIN referee_mast r ON b.referee_id=r.referee_id
--       GROUP BY referee_name) hh);


select rm.referee_name
from
euro_cup_2016.match_mast  mm,
(select match_no,count(1) as num_of_bookings 
from euro_cup_2016.player_booked 
group by match_no) rb,
euro_cup_2016.referee_mast  rm
where rb.match_no = mm.match_no
  and rm.referee_id = mm.referee_id
order by num_of_bookings desc
limit 1