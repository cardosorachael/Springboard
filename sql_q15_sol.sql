SELECT r.referee_name,
       count(b.match_no) as matchno
FROM player_booked p
JOIN match_mast b ON p.match_no=b.match_no
JOIN referee_mast r ON b.referee_id=r.referee_id
GROUP BY referee_name
HAVING count(b.match_no)=(SELECT max(matchno) FROM
     (SELECT count(b.match_no) as matchno
      FROM player_booked p
      JOIN match_mast b ON p.match_no=b.match_no
      JOIN referee_mast r ON b.referee_id=r.referee_id
      GROUP BY referee_name) hh);