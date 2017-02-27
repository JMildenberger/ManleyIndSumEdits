/*Pull base census period and base year number for the purpose of rebasing data in Industry Summary.sas*/
libname SQL ODBC DSN=IPSTestDB schema=sas;
data _null_;
	set sql.BaseYear;
	call symput("BaseYearID", BaseYearID);
	call symput("BasePeriod", BaseCensusPeriodID);
run;

data work.LPSource;
	set LPAll.LP_Append;
run;

/* 	The calculated variables from the output, labor, and ulc programs are read in here.
	T37=AnnOut, T36=AnnVP, W25=AnnWrk, L25=AnnHrs, U25=AnnCmp */
Proc sql;
	Create table 	work.LPIndSum as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.LPSource
	where 			DataSeriesID in ("T37", "T36", "W25", "L25", "U25")
	order by 		IndustryID, DataSeriesID, YearID;
quit;

/*	These queries filter out industries that don't have data during the "base census period" because we need overlapping years for linking census chunks */ 
Proc sql;
	Create table 	work.filter as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID
	from 			work.LPIndSum
	where 			CensusPeriodID = &baseperiod
	order by 		IndustryID, DataSeriesID;

	Create table	work.LPIndSum_filter as
	Select 			a.IndustryID, a.DataSeriesID, a.DataArrayID, a.YearID, a.CensusPeriodID, a.Value
	from 			work.LPIndSum a
	right join		work.filter b
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and (a.DataArrayID=b.DataArrayID)			
	order by 		IndustryID, DataSeriesID, YearID;
quit;


/*	The Year Number is extracted from the variable YearID	*/
data work.LPIndSum_filter;
	set work.LPIndSum_filter;
	YearNo=input(substr(YearID,5,1),1.);
run;


/*	Forward linking ratios are calculated for each CensusPeriodID (Year 6/ Year 1) */
Proc sql;
	Create table	work.CensusRatioAdjForward as
	Select 			a.IndustryID, a.DataSeriesID, a.CensusPeriodID, b.Value/a.Value as Ratio
	from 			work.LPIndSum_filter a
	inner join		work.LPIndSum_filter b
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and 
					(a.CensusPeriodID-1=b.CensusPeriodID) and (a.YearNo=1) and (b.YearNo=6)
	where			a.CensusPeriodID>&baseperiod;

/*	Backward linking ratios are calculated for each CensusPeriodID (Year 1 / Year 6) */
	Create table	work.CensusRatioAdjBack as
	Select 			a.IndustryID, a.DataSeriesID, a.CensusPeriodID, b.Value/a.Value as Ratio
	from 			work.LPIndSum_filter a
	inner join		work.LPIndSum_filter b	
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and 
					(a.CensusPeriodID+1=b.CensusPeriodID) and (a.YearNo=6) and (b.YearNo=1)
	where			a.CensusPeriodID<&baseperiod;
quit;

/*	Working files for the compounding of linking ratios are created */
data work.BackWorking;
	set work.CensusRatioAdjBack;
run;

data work.ForwardWorking;
	set work.CensusRatioAdjForward;
run;


/* 	This macro compounds the linking ratios for the CensusPeriods prior to the base period in step 1. In step 2
   	the macro compounds the linking ratios for the Census Periods after the base period
	Step 1 counts down from the base period to Census Period 9 which is the first period of published data. 
	Step 2 counts up from the base period to Census Period 20. Once measures are published beyond Period 20 the code 
	will need to be updated. */
%macro compound;
%do i = %eval(&baseperiod-1) %to 9 %by -1;
	Proc sql;
		Create table	work.BackCompound&i as
		Select			a.IndustryID, a.DataSeriesID, a.CensusPeriodID, 
						case 	when a.CensusPeriodID>=&i then c.ratio 
								else a.ratio*b.ratio 
						end as ratio
		from			work.CensusRatioAdjBack a
		left 			join work.BackWorking b
		on 				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and 
						(b.CensusPeriodID=a.CensusPeriodID+1) 
		left 			join work.BackWorking c
		on 				(a.IndustryID=c.IndustryID) and (a.DataSeriesID=c.DataSeriesID) and 
						(c.CensusPeriodID=a.CensusPeriodID)
		order by 		IndustryID, DataSeriesID, CensusPeriodID;
	quit;

	data work.BackWorking;
		set work.BackCompound&i;
	run;
%end;

%do i = %eval(&baseperiod+1) %to 20;
	Proc sql;
		Create table	work.ForwardCompound&i as
		Select			a.IndustryID, a.DataSeriesID, a.CensusPeriodID, 
						case 	when a.CensusPeriodID<=&i then c.ratio 
								else a.ratio*b.ratio
						end as ratio
		from			work.CensusRatioAdjForward a
		left join 		work.ForwardWorking b
		on 				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and
						(b.CensusPeriodID=a.CensusPeriodID)
		left join 		work.ForwardWorking c
		on 				(a.IndustryID=c.IndustryID) and (a.DataSeriesID=c.DataSeriesID) and 
						(c.CensusPeriodID=a.CensusPeriodID)
		order by 		IndustryID, DataSeriesID, CensusPeriodID;
		quit;

	data work.ForwardWorking;
		set work.ForwardCompound&i;
	run;
%end;
%mend compound;

%compound;


/*	The compounded linking ratios are multiplied by the Census chunk values to create a continuous series */
Proc sql;
	Create table	work.ApplyRatios as
	Select			a.IndustryID, a.DataSeriesID, a.YearID, a.CensusPeriodID, a.YearNo,
					case	when a.CensusPeriodID<&baseperiod then a.Value*b.Ratio
							when a.CensusPeriodID>&baseperiod then a.Value*c.Ratio
							when a.CensusPeriodID=&baseperiod then a.Value
					end as Value
	from			work.LPIndSum_filter a
	left join		work.BackWorking b
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and (a.CensusPeriodID=b.CensusPeriodID)
	left join		work.ForwardWorking c
	on				(a.IndustryID=c.IndustryID) and (a.DataSeriesID=c.DataSeriesID) and (a.CensusPeriodID=c.CensusPeriodID)
	order by		IndustryID, DataSeriesID, YearID;
quit;

/*	Levels were calculated in the "ApplyRatios" table. This query extracts that data and assigns the appropriate DataSeriesID. 
	W25=AnnWrk, L25=AnnHrs, U25=AnnCmp, T36=AnnVP | W20=AllEmp, L20=AllHrs, L02=LComp, T30=ValProd*/
Proc sql;
	Create table	work.FinalLevels as
	Select			IndustryID, YearID, CensusPeriodID, YearNo, Value,
					case	when a.DataSeriesID="W25" then "W20"
							when a.DataSeriesID="L25" then "L20"
							when a.DataSeriesID="U25" then "L02"
							when a.DataSeriesID="T36" then "T30"
					end as DataSeriesID
	from			work.ApplyRatios a
	where			DataSeriesID ne "T37"
	order by		IndustryID, DataSeriesID, YearID;
quit;

/*	This query rebases the linked data to the first year of the base period and assigns the appropriate DataSeriesID. 
	W25=AnnWrk, L25=AnnHrs, U25=AnnCmp, T37=AnnOut | W01=AE, L01=AEH, U11=LCompIdx, T01=OUT*/

Proc sql;
	Create table	work.FinalIndependentIndexes as
	Select			a.IndustryID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value/b.Value*100 as Value,
					case	when a.DataSeriesID="W25" then "W01"
							when a.DataSeriesID="L25" then "L01"
							when a.DataSeriesID="U25" then "U11"
							when a.DataSeriesID="T37" then "T01"
					end as DataSeriesID
	from			work.ApplyRatios a
	inner join		work.ApplyRatios b
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and (b.yearid="&BaseYearID")
	where			a.DataSeriesID ne "T36"
	order by		IndustryID, DataSeriesID, YearID;
quit;

/*	This query calculates the ValProd index. While not a recorded DataSeriesID it is necessary for the calculation of ImPrDef.
	T36=AnnVP */
Proc sql;
	Create table	work.ValProdIndex as
	Select			a.IndustryID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value/b.Value*100 as Value
	from			work.ApplyRatios a
	inner join		work.ApplyRatios b
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and (b.yearid="&BaseYearID")
	where			a.DataSeriesID="T36"
	order by		IndustryID, YearID;
quit;

/*	This query calculates the ImPrDef index. T01=OUT | T05=ImPrDef */
Proc sql;
	Create table	work.ImPrDef as
	Select			a.IndustryID, "T05" as DataSeriesID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value/b.Value*100 as Value
	from			work.ValProdIndex a
	inner join		work.FinalIndependentIndexes b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID)
	where			b.DataSeriesID="T01"
	order by		IndustryID, DataSeriesID, YearID;
quit;

/*	This query calculates the OAEH index. T01=OUT, L01=AEH | L00=OAEH */
Proc sql;
	Create table	work.OAEH as
	Select			a.IndustryID, "L00" as DataSeriesID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value/b.Value*100 as Value
	from			work.FinalIndependentIndexes a
	inner join		work.FinalIndependentIndexes b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID="T01") and (b.DataSeriesID="L01")
	order by		IndustryID, DataSeriesID, YearID;
quit;

/*	This query calculates the OAE index. T01=OUT, W01=AE | W00=OAE */
Proc sql;
	Create table	work.OAE as
	Select			a.IndustryID, "W00" as DataSeriesID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value/b.Value*100 as Value
	from			work.FinalIndependentIndexes a
	inner join		work.FinalIndependentIndexes b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID="T01") and (b.DataSeriesID="W01")
	order by		IndustryID, DataSeriesID, YearID;
quit;

/*	This query calculates the ULC index. U11=LCompIdx, T01=OUT | U10=ULC */
Proc sql;
	Create table	work.ULC as
	Select			a.IndustryID, "U10" as DataSeriesID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value/b.Value*100 as Value
	from			work.FinalIndependentIndexes a
	inner join		work.FinalIndependentIndexes b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID="U11") and (b.DataSeriesID="T01")
	order by		IndustryID, DataSeriesID, YearID;
quit;


/*	This query calculates hourly compensation. L02 (U25) =AnnComp, L20 (L25)=AnnHrs  | H01=HrlyCompLevels */
Proc sql;
	Create table	work.CompHrs as
	Select			a.IndustryID, "H01" as DataSeriesID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value/b.Value as Value
	from			work.FinalLevels a
	inner join		work.FinalLevels b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID="L02") and (b.DataSeriesID="L20")
	order by		IndustryID, DataSeriesID, YearID;
quit;


/*	This query calculates the hourly compensation index. U11=LCompIdx, L01=AEH  | H00=HrCompIdx */
Proc sql;
	Create table	work.CompHrsIdx as
	Select			a.IndustryID, "H00" as DataSeriesID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value/b.Value*100 as Value
	from			work.FinalIndependentIndexes a
	inner join		work.FinalIndependentIndexes b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID="U11") and (b.DataSeriesID="L01")
	order by		IndustryID, DataSeriesID, YearID;
quit;


/* This query merges the calculated variables together*/
Proc sql;
	Create table 	work.CalculatedIndSumVars as
	Select 			IndustryID, DataSeriesID, YearID, CensusPeriodID, YearNo, Value 	from work.FinalLevels union all
	Select 			IndustryID, DataSeriesID, YearID, CensusPeriodID, YearNo, Value 	from work.FinalIndependentIndexes union all
	Select 			IndustryID, DataSeriesID, YearID, CensusPeriodID, YearNo, Value  	from work.ImPrDef union all
	Select 			IndustryID, DataSeriesID, YearID, CensusPeriodID, YearNo, Value  	from work.OAEH union all
	Select 			IndustryID, DataSeriesID, YearID, CensusPeriodID, YearNo, Value  	from work.OAE union all
	Select 			IndustryID, DataSeriesID, YearID, CensusPeriodID, YearNo, Value  	from work.ULC union all
	Select 			IndustryID, DataSeriesID, YearID, CensusPeriodID, YearNo, Value  	from work.CompHrs union all
	Select 			IndustryID, DataSeriesID, YearID, CensusPeriodID, YearNo, Value  	from work.CompHrsIdx
	order by		IndustryID, DataSeriesID, YearID;
quit;

/*The next two queries remove duplicate years and assign new YearIDs with Year numbers 1-5 */

Proc sql;
	Create table	work.Indicator as
	Select			a.IndustryID, a.DataSeriesID, a.YearID, a.CensusPeriodID, a.YearNo, a.Value, 
					case 	when a.YearNo ne 6 then "Keep" 
							when b.IndustryID is null and a.YearNo=6 then "NewYearID"
							else "Delete" 
					end as 	Indicator
	from			work.CalculatedIndSumVars a
	left join		work.CalculatedIndSumVars b
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and 
					(b.CensusPeriodID=a.CensusPeriodID+1) and (b.YearNo=1);
quit;

Proc sql;
	Create table	work.FinalIndSumVars as
	Select			a.IndustryID, a.DataSeriesID, "0000" as DataArrayID,
					case 	when a.Indicator="Keep" then a.YearID
							when a.Indicator="NewYearID" then b.YearID
					end as YearID,
					case 	when a.Indicator="Keep" then a.CensusPeriodID
							when a.Indicator="NewYearID" then b.CensusPeriodID
					end as CensusPeriodID,
					a.Value
	from			work.Indicator a
	left join		LPALL.Report_YearsCensusPeriod b
	on				b.CensusPeriodID=a.CensusPeriodID+1 and b.CensusYear=1
	where			a.Indicator ne "Delete"
	order by		IndustryID, DataSeriesID, YearID;
quit;


/* This query merges the calculated variables together along with the source DataSeriesIDs */
Proc sql;
	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.FinalIndSumVars union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.LPSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;
quit;


data Final.LPSASFinal;
	set LPAll.LP_Append;
run;


proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
