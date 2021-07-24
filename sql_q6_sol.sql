-- SELECT COUNT(goal_score) 
-- FROM euro_cup_2016.match_details 
-- WHERE win_lose='W' AND decided_by<>'P'AND goal_score=1;


select win_match.match_no
from
(select match_no,goal_score
 from euro_cup_2016.match_details
 where win_lose = 'L' and decided_by <> 'P') lost_match,
(select match_no,goal_score
 from euro_cup_2016.match_details
 where win_lose = 'W' and decided_by <> 'P') win_match
 where lost_match.match_no = win_match.match_no
   and win_match.goal_score - lost_match.goal_score = 1