

with MissingOverview as(
select distinct 
	  g.GameID
	, min(pcs.GameID)	PlayerCatchShoot,		min(tcs.GameID)		TeamCatchShoot
	, min(pd.GameID)	PlayerDefense,			min(td.GameID)		TeamDefense
	, min(pdr.GameID)	PlayerDrives,			min(tdr.GameID)		TeamDrives
	, min(pe.GameID)	PlayerEfficiency,		min(te.GameID)		TeamEfficiency
	, min(pet.GameID)	PlayerElbowTouch,		min(tet.GameID)		TeamElbowTouch
	, min(ph.GameID)	PlayerHustle,			min(th.GameID)		TeamHustle
	, min(ppt.GameID)	PlayerPaintTouch,		min(tpt.GameID)		TeamPaintTouch
	, min(ppa.GameID)	PlayerPassing,			min(tpa.GameID)		TeamPassing
	, min(ppos.GameID)	PlayerPossessions,		min(tpos.GameID)	TeamPossessions
	, min(ppot.GameID)	PlayerPostTouch,		min(tpot.GameID)	TeamPostTouch
	, min(ppu.GameID)	PlayerPullUpShot,		min(tpu.GameID)		TeamPullUpShot
	, min(pr.GameID)	PlayerRebounding,		min(tr.GameID)		TeamRebounding
	, min(psd.GameID)	PlayerSpeedDistance,	min(tsd.GameID)		TeamSpeedDistance

from Game g
inner join TeamBox tb on g.SeasonID = tb.SeasonID and g.GameID = tb.GameID
inner join PlayerBox pb on g.SeasonID = pb.SeasonID and g.GameID = pb.GameID and tb.TeamID = pb.TeamID and tb.MatchupID = pb.MatchupID
left join tracking.PlayerCatchShoot pcs on g.SeasonID = pcs.SeasonID and g.GameID = pcs.GameID and pb.TeamID = pcs.TeamID and pb.MatchupID = pcs.MatchupID and pb.PlayerID = pcs.PlayerID
left join   tracking.TeamCatchShoot tcs on g.SeasonID = tcs.SeasonID and g.GameID = tcs.GameID and pb.TeamID = tcs.TeamID and pb.MatchupID = tcs.MatchupID
left join tracking.PlayerDefense pd on g.SeasonID = pd.SeasonID and g.GameID = pd.GameID and pd.TeamID = pb.TeamID and pd.MatchupID = pb.MatchupID and pb.PlayerID = pd.PlayerID
left join   tracking.TeamDefense td on g.SeasonID = td.SeasonID and g.GameID = td.GameID and pb.TeamID = td.TeamID and pb.MatchupID = td.MatchupID
left join tracking.PlayerDrives pdr on g.SeasonID = pdr.SeasonID and g.GameID = pdr.GameID and pb.TeamID = pdr.TeamID and pb.MatchupID = pdr.MatchupID and pb.PlayerID = pdr.PlayerID
left join   tracking.TeamDrives tdr on g.SeasonID = tdr.SeasonID and g.GameID = tdr.GameID and pb.TeamID = tdr.TeamID and pb.MatchupID = tdr.MatchupID
left join tracking.PlayerEfficiency pe on g.SeasonID = pe.SeasonID and g.GameID = pe.GameID and pb.TeamID = pe.TeamID and pb.MatchupID = pe.MatchupID and pb.PlayerID = pe.PlayerID
left join   tracking.TeamEfficiency te on g.SeasonID = te.SeasonID and g.GameID = te.GameID and pb.TeamID = te.TeamID and pb.MatchupID = te.MatchupID
left join tracking.PlayerElbowTouch pet on g.SeasonID = pet.SeasonID and g.GameID = pet.GameID and pb.TeamID = pet.TeamID and pb.MatchupID = pet.MatchupID and pb.PlayerID = pet.PlayerID
left join   tracking.TeamElbowTouch tet on g.SeasonID = tet.SeasonID and g.GameID = tet.GameID and pb.TeamID = tet.TeamID and pb.MatchupID = tet.MatchupID
left join tracking.PlayerHustle ph on g.SeasonID = ph.SeasonID and g.GameID = ph.GameID and pb.TeamID = ph.TeamID and pb.MatchupID = ph.MatchupID and pb.PlayerID = ph.PlayerID
left join   tracking.TeamHustle th on g.SeasonID = th.SeasonID and g.GameID = th.GameID and pb.TeamID = th.TeamID and pb.MatchupID = th.MatchupID
left join tracking.PlayerPaintTouch ppt on g.SeasonID = ppt.SeasonID and g.GameID = ppt.GameID and pb.TeamID = ppt.TeamID and pb.MatchupID = ppt.MatchupID and pb.PlayerID = ppt.PlayerID
left join   tracking.TeamPaintTouch tpt on g.SeasonID = tpt.SeasonID and g.GameID = tpt.GameID and pb.TeamID = tpt.TeamID and pb.MatchupID = tpt.MatchupID
left join tracking.PlayerPassing ppa on g.SeasonID = ppa.SeasonID and g.GameID = ppa.GameID and pb.TeamID = ppa.TeamID and pb.MatchupID = ppa.MatchupID and pb.PlayerID = ppa.PlayerID
left join   tracking.TeamPassing tpa on g.SeasonID = tpa.SeasonID and g.GameID = tpa.GameID and pb.TeamID = tpa.TeamID and pb.MatchupID = tpa.MatchupID
left join tracking.PlayerPossessions ppos on g.SeasonID = ppos.SeasonID and g.GameID = ppos.GameID and pb.TeamID = ppos.TeamID and pb.MatchupID = ppos.MatchupID and pb.PlayerID = ppos.PlayerID
left join   tracking.TeamPossessions tpos on g.SeasonID = tpos.SeasonID and g.GameID = tpos.GameID and pb.TeamID = tpos.TeamID and pb.MatchupID = tpos.MatchupID
left join tracking.PlayerPostTouch ppot on g.SeasonID = ppot.SeasonID and g.GameID = ppot.GameID and pb.TeamID = ppot.TeamID and pb.MatchupID = ppot.MatchupID and pb.PlayerID = ppot.PlayerID
left join   tracking.TeamPostTouch tpot on g.SeasonID = tpot.SeasonID and g.GameID = tpot.GameID and pb.TeamID = tpot.TeamID and pb.MatchupID = tpot.MatchupID
left join tracking.PlayerPullUpShot ppu on g.SeasonID = ppu.SeasonID and g.GameID = ppu.GameID and pb.TeamID = ppu.TeamID and pb.MatchupID = ppu.MatchupID and pb.PlayerID = ppu.PlayerID
left join   tracking.TeamPullUpShot tpu on g.SeasonID = tpu.SeasonID and g.GameID = tpu.GameID and pb.TeamID = tpu.TeamID and pb.MatchupID = tpu.MatchupID
left join tracking.PlayerRebounding pr on g.SeasonID = pr.SeasonID and g.GameID = pr.GameID and pb.TeamID = pr.TeamID and pb.MatchupID = pr.MatchupID and pb.PlayerID = pr.PlayerID
left join   tracking.TeamRebounding tr on g.SeasonID = tr.SeasonID and g.GameID = tr.GameID and pb.TeamID = tr.TeamID and pb.MatchupID = tr.MatchupID
left join tracking.PlayerSpeedDistance psd on g.SeasonID = psd.SeasonID and g.GameID = psd.GameID and pb.TeamID = psd.TeamID and pb.MatchupID = psd.MatchupID and pb.PlayerID = psd.PlayerID
left join   tracking.TeamSpeedDistance tsd on g.SeasonID = tsd.SeasonID and g.GameID = tsd.GameID and pb.TeamID = tsd.TeamID and pb.MatchupID = tsd.MatchupID
where g.SeasonID = 2025 and g.GameType not in('PRE', 'CUP')
and (pcs.GameID is null  or tcs.GameID is null		--Catch Shoot
or pd.GameID is null   or td.GameID is null		--Defense
or pdr.GameID is null  or tdr.GameID is null	--Drives
or pe.GameID is null   or te.GameID is null		--Efficiency
or pet.GameID is null  or tet.GameID is null	--Elbow Touch
or ph.GameID is null   or th.GameID is null		--Hustle
or ppt.GameID is null  or tpt.GameID is null	--Paint Touch
or ppa.GameID is null  or tpa.GameID is null	--Passing
or ppos.GameID is null or tpos.GameID is null	--Possessions
or ppot.GameID is null  or tpot.GameID is null	--Post Touch
or ppu.GameID is null or tpu.GameID is null		--Pull up Shot
or pr.GameID is null or tr.GameID is null		--Rebounding
or psd.GameID is null or tsd.GameID is null		--Speed Distance
)
group by g.GameID
)select 
sum(case when PlayerCatchShoot is null then 1 else 0 end)		P_CatchShoot
, sum(case when PlayerDefense is null then 1 else 0 end)		P_Defense
, sum(case when PlayerDrives is null then 1 else 0 end)			P_Drives
, sum(case when PlayerEfficiency is null then 1 else 0 end)		P_Efficiency
, sum(case when PlayerElbowTouch is null then 1 else 0 end)		P_ElbowTouch
, sum(case when PlayerHustle is null then 1 else 0 end)			P_Hustle
, sum(case when PlayerPaintTouch is null then 1 else 0 end)		P_PaintTouch
, sum(case when PlayerPassing is null then 1 else 0 end)		P_Passing
, sum(case when PlayerPossessions is null then 1 else 0 end)	P_Possessions
, sum(case when PlayerPostTouch is null then 1 else 0 end)		P_PostTouch
, sum(case when PlayerPullUpShot is null then 1 else 0 end)		P_PullUpShot
, sum(case when PlayerRebounding is null then 1 else 0 end)		P_Rebounding
, sum(case when PlayerSpeedDistance is null then 1 else 0 end)	P_SpeedDistance

, sum(case when TeamCatchShoot is null then 1 else 0 end)		T_CatchShoot
, sum(case when TeamDefense is null then 1 else 0 end)			T_Defense
, sum(case when TeamDrives is null then 1 else 0 end)			T_Drives
, sum(case when TeamEfficiency is null then 1 else 0 end)		T_Efficiency
, sum(case when TeamElbowTouch is null then 1 else 0 end)		T_ElbowTouch
, sum(case when TeamHustle is null then 1 else 0 end)			T_Hustle
, sum(case when TeamPaintTouch is null then 1 else 0 end)		T_PaintTouch
, sum(case when TeamPassing is null then 1 else 0 end)			T_Passing
, sum(case when TeamPossessions is null then 1 else 0 end)		T_Possessions
, sum(case when TeamPostTouch is null then 1 else 0 end)		T_PostTouch
, sum(case when TeamPullUpShot is null then 1 else 0 end)		T_PullUpShot
, sum(case when TeamRebounding is null then 1 else 0 end)		T_Rebounding
, sum(case when TeamSpeedDistance is null then 1 else 0 end)	T_SpeedDistance
from MissingOverview





 
