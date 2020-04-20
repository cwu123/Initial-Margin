SET NOCOUNT ON
GO

UPDATE SPAN_LOAD
SET sort_dt = convert(DATETIME, convert(CHAR(12), getdate(), 112))
WHERE sort_dt IS NULL
GO

SELECT *
FROM SPAN_LOAD
GO