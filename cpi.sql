WITH diffs AS (
	SELECT (id - (-1)) AS diff
	FROM a
) SELECT ROUND(EXP(SUM(LN((COALESCE(diff, 1)))))) AS mul
FROM diffs
