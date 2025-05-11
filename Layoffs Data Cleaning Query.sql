-- Data Cleaning --

SELECT * 
FROM  world_layoffs.layoffs;


-- 1. Remove Duplicates
-- 2. Standardize the Data 
-- 3. Null Vlaues / Blank values
-- 4. Remove Any Columns

-- first thing we want to do is create a staging table. We want a table with the raw data in case something happens.

CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs;

SELECT *
FROM world_layoffs.layoffs_staging ;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

SELECT *
FROM world_layoffs.layoffs_staging;

-- 1.Removing Dulipcates

-- this assigns row number 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) As row_num
FROM world_layoffs.layoffs_staging;

-- to filter based on row numbers by cte 

WITH duplicate_cte  AS
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company,location,industry,total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) As row_num
	FROM world_layoffs.layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1 ;  -- we have filtered duplicates 


-- CREATING TABLE FOR FILTERING ROW-NUM 2 AND DELETING THEM WHERE ROW-NUM IS > 1

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company,location,industry,total_laid_off, 
    percentage_laid_off, `date`,stage, country, funds_raised_millions) As row_num
FROM world_layoffs.layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE 
FROM layoffs_staging2
WHERE row_num > 1 ;

SELECT * 
FROM layoffs_staging2;

-- 2. STANDARDIZING DATA

SELECT *
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%' ;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- now date  column

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')  -- string to date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')  -- string to date
;

ALTER TABLE layoflayoffs_staging2fs_staging2
MODIFY COLUMN `date` DATE;  -- for changing date type txt to date 



SELECT * FROM layoffs_staging2;


-- null vlaues in  industry is done here

SELECT * 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ;

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''; 

SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE(t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2
SET industry = NULL
where industry = '';

UPDATE  layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE(t1.industry IS NULL )
AND t2.industry IS NOT NULL;


SELECT *
FROM layoffs_staging2;


-- 3. we cannot  remove null values from total_laid_off, percentage_laid_off due to insufficient date given 


-- 4. remove columns and rows if needed..

SELECT * 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ;

DELETE
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ;


SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM layoffs_staging2;

-- now data is cleaned 

