/*
These queriers aim to recreate the COVID19 graphs being used in newspapers and on the Virginia Covid19 dashboard.
The data comes from the Virginia Department of Health COVID 19 Public Use Dataset <https://www.vdh.virginia.gov/coronavirus/ >,
specifically the VDH-COVID-19-PublicUseDataset-Cases-Summary (except for the last query).

The data was saved as an excel workbook as a vertical table on August 16, 2021. 

SAS was used in order to make the data a horizontal table, using a TRANSPOSE procedure on the Report_Date variable.
The resulting dataset was saved as an excel workbook that was then imported in to SSMS.

*/

/* Checking that the data was imported correctly.*/
SELECT *
FROM va_covid..va_covid_summary
ORDER BY 1;

---------------------------------------------------------------------------------------------------------------------
/*--------------------------------------------------- Number of Cases -----------------------------------------*/
---------------------------------------------------------------------------------------------------------------------
/* Trying to mimic the bar graph located at
<https://www.vdh.virginia.gov/coronavirus/see-the-numbers/covid-19-in-virginia/covid-19-in-virginia-cases/>

Columns: Date, Confirmed Cases, Probable Cases, New Cases (reported combined cases)
		 and 7-day moving average using their combined total.
Excluding Dec 23 2020 cause they messed up inputting the data.
*/

SELECT	Report_Date, 
	confirmed_cases - (LAG(confirmed_cases,1) OVER (ORDER BY Report_Date)) as new_confirmed_cases, 
	probable_cases - (LAG(probable_cases,1) OVER (ORDER BY Report_Date)) as new_probable_cases,
	new_cases,
	ROUND((AVG(new_cases) OVER (ORDER BY Report_Date
					ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)),0) as seven_day_avg
FROM va_covid..va_covid_summary
WHERE Report_Date not in ('2020-12-23') ;


---------------------------------------------------------------------------------------------------------------
/*----------------------------------------------- Number of deaths---------------------------------------- */
---------------------------------------------------------------------------------------------------------------
/* From the same website as above. To see the death bar graph, select "deaths" under the "Select Measure" section located to the
top left of the picture of Virginia that has the cases by county colored in.
This needed an extra step of using a CTE since new deaths wasn't a category reported.

Columns: Date, New Deaths, Confirmed deaths, Probable deaths, 
		 and 7-day moving average of deaths (confirmed and probable combined).
Excluded values had data entry problems.
*/

WITH new_death (Report_Date, new_deaths, new_confirmed_deaths, new_probable_deaths) AS 
(
SELECT 	Report_Date,
	total_deaths - (LAG(total_deaths,1) OVER (ORDER BY Report_Date)) as new_deaths,
	confirmed_deaths - (LAG(confirmed_deaths,1) OVER (ORDER BY Report_Date)) as new_confirmed_deaths, 
	probable_deaths-(LAG(probable_deaths,1) OVER (ORDER BY Report_Date)) as new_probable_deaths
FROM va_covid..va_covid_summary
WHERE Report_Date not in ('2020-12-23','2021-04-27','2021-05-09')
)
SELECT *,
	ROUND((AVG(new_deaths) OVER (ORDER BY Report_Date
					ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)),0) as seven_day_avg
FROM new_death
ORDER BY Report_Date;

-------------------------------------------------------------------------------------------------------
/*--------------------------------------- Hospitalizations---------------------------------------*/
--------------------------------------------------------------------------------------------------------
/* Like the graph exploring deaths caused by COVID19, the hospitalizations are seen by selecting "hospitalizations"
on the Virginia COVID19 Dashboard. In the public summary dataset, there is no variable called "new hospitalizations" 
so that is created using a CTE.

Columns: Date, New Hospitalizations, New Confirmed Hospitalizations, New Probable Hospitalizations 
		 and a 7-day moving average using their combined total.
Excluded values had data entry problems.
*/

WITH new_hosp (Report_Date, new_hospitalizations, new_confirmed_hospitalizations, new_probable_hospitalizations) AS 
(
SELECT 	Report_Date,
	total_hospitalizations - (LAG(total_hospitalizations,1) OVER (ORDER BY Report_Date)) as new_hospitalizations,
	confirmed_hospitalizations - (LAG(confirmed_hospitalizations,1) OVER (ORDER BY Report_Date)) as new_confirmed_hospitalizations, 
	probable_hospitalizations-(LAG(probable_hospitalizations,1) OVER (ORDER BY Report_Date)) as new_probable_hospitalizations
FROM va_covid..va_covid_summary
WHERE Report_Date not in ('2020-12-23','2021-04-27','2021-05-09')
)
SELECT 	*,
	round( AVG(new_hospitalizations) OVER (ORDER BY Report_Date
						ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) as seven_day_avg
FROM new_hosp
ORDER BY Report_Date;

------------------------------------------------------------------------------------------------
/*-------------------------------- Vaccination Summary Table ---------------------------------*/
-------------------------------------------------------------------------------------------------
/* The  Virginia Department of Health has plenty of information about how many vaccines
have been given out and the type of vaccine, along with the the dose number, given. However, I believe the
most important aspect of vaccinations is how many people are considered fully vaccinated. For the query below,
that means someone who had two doses and is 2 weeks past the second dose. VDH does not have that information 
in the data they release but have it on their Covid19 dashboard so I will try to create my own vaccination 
summary that includes the number of people fully vaccinated against Covid19.

This project uses a dataset called VDH-COVID-19-PublicUseDataset-Vaccines-DosesAdministered available on 
<https://www.vdh.virginia.gov/coronavirus/ >.
*/

-- First I need to know the different vaccines given so I can make sure to exclude the one dose shots. Excluding the
-- vaccine that only requires one dose will make creating this table easier.
SELECT *
FROM va_covid..va_covid_vaccination
GROUP BY vaccine_manufacturer;


/* USING A CTE to figure out the daily number of shots per dose number. */

/* The 'doses_administered' keeps tracks of each individual vaccine dose by manufacturer (ie 12 Pfizer shots, 4 Moderna shots
on a specific date in a speficic zip code), so by using a SUM function we find the daily total across all zip codes and manufacturers.
That sum is then needed for a rolling cummulative total, so a CTE is used. Finally, the results are stored in a temporary table
so I can manipulate the information gathered without having to manually run the queries again.

 Columns: Date, which dose it was, the number of daily shots per dose number, 
		  and a rolling total of shots per dose number.
 */

WITH vax_dose (administration_date, dose_number, daily_number_of_shots) AS 
(
SELECT	administration_date,
	dose_number,
	SUM(doses_administered) as daily_number_of_shots
FROM va_covid..va_covid_vaccination
WHERE vaccine_manufacturer != 'J&J'			-- since the summary is only focusing on 2 shot regimines
GROUP BY administration_date, dose_number
)
SELECT	*,
	SUM(daily_number_of_shots) OVER 
	(PARTITION BY dose_number ORDER BY administration_date, dose_number) as rolling_total_shots
INTO #vax_shot_count				--this creates a temporary table
FROM vax_dose
WHERE administration_date is not null		-- this excludes Federal doses that were not reported to VDH on a daily basis
ORDER BY administration_date, dose_number;

/* Now that we have the amount of people who have received their shots, we can use a PARTITION/OVER statement
 to present the rolling total number of people who received their second dose at least 14 days prior (how long it takes someone to
 become fully vaccinated) to the date of a given row. This tells us the number of fully vaccinated Virginians on 
 any date.

 Columns: Date, dose number, rolling total vaccinated, and fully vaccinated people.
*/

SELECT	*,
	(SUM(daily_number_of_shots) OVER (Partition BY dose_number ORDER BY administration_date
								ROWS BETWEEN UNBOUNDED PRECEDING 
								AND 14 PRECEDING)) as fully_vaxxd
INTO #vax_dose_2
FROM #vax_shot_count
WHERE dose_number = 2;

-- Need to find the cummulative dose_1 shots in order to build a final table wth all this information
-- as a nice vaccination summary.
--
-- Columns: Date, Daily Number of Shots, Rolling Total of Shots (per dose number)
--
SELECT	administration_date,
	daily_number_of_shots,
	rolling_total_shots
INTO #vax_dose_1
from #vax_shot_count 
where dose_number = 1;

-- Checking the tables that were just created
SELECT * FROM #vax_dose_1;
SELECT * FROM #vax_dose_2;


/* Creating the vaccination summary table.
Finally, what the mission was: a vaccination summary table. We join the the two temporary
tables that were created using a LEFT JOIN since the #vax_dose_1 table is longer/has more
dates than the #vax_dose_2. The result is a table containing the amount of dose 1 shots,
ose 2 shots, their respective cummulative totals, and the number of fully vaccinated people.

Columns: administration_date, daily_dose_1_shots, daily_dose_2_shots,
			cummulative_dose_1, cummulative_dose_2, fully_vaccinated
*/

SELECT	one.administration_date,
	one.daily_number_of_shots as daily_dose_1_shots,
	ISNULL(two.daily_number_of_shots, 0) as daily_dose_2_shots,
	one.rolling_total_shots as cummulative_dose_1,
	ISNULL(two.rolling_total_shots,0) as cummulative_dose_2,
	ISNULL(two.Fully_Vaxxd,0) as fully_vaccinated
FROM #vax_dose_1 one
	LEFT JOIN #vax_dose_2 two
	ON one.administration_date = two.administration_date
ORDER BY one.administration_date;


