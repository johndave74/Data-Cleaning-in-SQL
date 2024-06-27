-- Data Cleaning

-- Displaying all records of the table & creating a backup
SELECT * FROM layoffs;

CREATE TABLE layoff_staging
LIKE layoffs;

SELECT COUNT(*) FROM layoff_staging;

INSERT INTO layoff_staging
	SELECT * FROM layoffs;
    
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values & Blank values
-- 4. Remove any columns or rows



SELECT *,
ROW_NUMBER() OVER
	(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
    `date`, stage, country, funds_raised_millions)
		AS row_num
FROM layoff_staging;

-- Creating a CTE to remove the dulicates
WITH duplicate_cte AS
(
	SELECT *,
ROW_NUMBER() OVER
	(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
    `date`, stage, country, funds_raised_millions)
		AS row_num
FROM layoff_staging)
DELETE FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoff_staging2` (
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

SELECT * FROM layoff_staging2;

INSERT INTO layoff_staging2
		(SELECT *, ROW_NUMBER() OVER
			(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
			`date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_staging);

DELETE FROM layoff_staging2
WHERE row_num > 1;

SELECT * FROM layoff_staging2;


-- Standardizing data
SELECT * FROM layoff_staging2;

SELECT company, TRIM(company) 
FROM layoff_staging2;

UPDATE layoff_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoff_staging2
ORDER BY 1;

SELECT industry
FROM layoff_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT *
FROM layoff_staging2;

SELECT DISTINCT country
FROM layoff_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoff_staging2
WHERE country LIKE 'United States%';

UPDATE layoff_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoff_staging2;

UPDATE layoff_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- Changing the datatype of the date column
ALTER TABLE layoff_staging2
MODIFY `date` DATE;

SELECT * FROM layoff_staging2;

-- Removing null values
SELECT * FROM layoff_staging2
WHERE industry IS NULL OR industry = '';

SELECT * FROM layoff_staging2
WHERE company = 'Airbnb';

UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '';

SELECT * FROM layoff_staging2 st1
JOIN layoff_staging2 st2
	ON st1.company = st2.company
    AND st1.location = st2.location
WHERE  st1.industry IS NULL
AND st2.industry IS NOT NULL;


UPDATE layoff_staging2 st1
JOIN layoff_staging2 st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE st1.industry IS NULL
AND st2.industry IS NOT NULL;


SELECT * 
FROM layoff_staging2
		WHERE total_laid_off IS NULL 
        AND percentage_laid_off IS NULL;
        
DELETE 
FROM layoff_staging2
		WHERE total_laid_off IS NULL 
        AND percentage_laid_off IS NULL;


-- Drop the row_num column
ALTER TABLE layoff_staging2
DROP COLUMN row_num;


SELECT * FROM layoff_staging2;