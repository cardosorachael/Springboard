SELECT match_no, Booked FROM (
SELECT match_no,COUNT(*) Booked 
FROM player_booked  
GROUP BY match_no) mm where Booked=(
SELECT MAX(m_no) 
FROM (SELECT match_no,COUNT(*) m_no
FROM player_booked  
GROUP BY match_no) mm1);