-- EXPLORATARY DATA ANALYSIS

SELECT *
FROM layoffs_staging2;

-- max total laid off and max percentage laid off
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;


-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- company with most total laid off   
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC; 

-- laid off based on company in each year--

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC; 

-- RANKING BASED LAID OFF BY  COMPANY AND  YEARS --

WITH Company_Year (company, years,total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
),Company_year_rank AS
(
SELECT * , 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)

SELECT * FROM Company_year_rank
WHERE Ranking <= 5; -- checking for 1 to 5 ranks








-- industry with most total laid off
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- country with most total laid off
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


-- yearly wise total laid off

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC; 


-- STAGE wise total laid off
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC; 


-- layoffs  based on date(month)
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) as total_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;


-- laysoff  by month in cte


WITH Rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) as total_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL 
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_layoffs, SUM(total_layoffs) OVER(ORDER BY `Month`) AS Rolling_total
FROM Rolling_total;

-- toal laid off by month and year -- 

