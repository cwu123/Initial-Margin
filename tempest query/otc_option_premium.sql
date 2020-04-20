/* Note
   I use final_pricing_dt >= getdate() because this will be run in the early
   morning on the backup server.  The assumption here is that this will
   always be run on the day after COB.  If this was run on the same date
   as COB, then the '>=' should be changed to a '>'.
*/
PRINT 'BEGIN'

DROP TABLE OTC_OPTION_PREMIUM_LOAD
GO

PRINT 'CREATE TABLE'

CREATE TABLE OTC_OPTION_PREMIUM_LOAD (
	strategy_num INT NULL
	,desk_cd CHAR(10) NULL
	,book_cd CHAR(10) NULL
	,hdr_num INT NULL
	,total_premium MONEY NULL
	,per_trade_premium MONEY NULL
	,total_trades SMALLINT NULL
	,open_trades SMALLINT NULL
	,open_premium MONEY NULL
	,cost_revenue_ind CHAR(1) NULL
	)
GO

PRINT 'INSERT INTO TABLE'

/* Get all the open OTC options header records. */
INSERT INTO OTC_OPTION_PREMIUM_LOAD (
	strategy_num
	,hdr_num
	)
SELECT strategy_num
	,hdr_num
FROM OTC_OPTION_HDR
WHERE expiration_dt >= convert(CHAR(8), getdate(), 112)
	AND trade_status_ind <> 'C'
GO

/* Get the premium for each header. */
UPDATE OTC_OPTION_PREMIUM_LOAD
SET total_premium = PCR.extended_amt
	,cost_revenue_ind = PCR.cost_revenue_ind
FROM PCR
WHERE PCR.strategy_num = OTC_OPTION_PREMIUM_LOAD.strategy_num
	AND PCR.hdr_num = OTC_OPTION_PREMIUM_LOAD.hdr_num
	AND PCR.trade_status_ind <> 'C'
	AND PCR.pcr_type_ind = 'P'
GO

/* If there is no premium, then there is no need to keep the records. */
DELETE OTC_OPTION_PREMIUM_LOAD
WHERE total_premium = 0
GO

/* Update the aggregation groups. */
UPDATE OTC_OPTION_PREMIUM_LOAD
SET book_cd = S.book_cd
FROM STRATEGY S
WHERE OTC_OPTION_PREMIUM_LOAD.strategy_num = S.strategy_num
GO

/* Only select the first desk code. */
UPDATE OTC_OPTION_PREMIUM_LOAD
SET desk_cd = SO.desk_cd
FROM STRATEGY_OWNER SO
WHERE OTC_OPTION_PREMIUM_LOAD.strategy_num = SO.strategy_num
	AND SO.strategy_owner_num = 1
GO

/* How many total trades exist for each header? */
UPDATE OTC_OPTION_PREMIUM_LOAD
SET total_trades = (
		SELECT count(*)
		FROM OTC_OPTION_TRADE OOT
		WHERE OOT.strategy_num = OTC_OPTION_PREMIUM_LOAD.strategy_num
			AND OOT.hdr_num = OTC_OPTION_PREMIUM_LOAD.hdr_num
		)
GO

/* How many OPEN trades exist for each header? */
UPDATE OTC_OPTION_PREMIUM_LOAD
SET open_trades = (
		SELECT count(*)
		FROM PCR
		WHERE PCR.strategy_num = OTC_OPTION_PREMIUM_LOAD.strategy_num
			AND PCR.hdr_num = OTC_OPTION_PREMIUM_LOAD.hdr_num
			AND PCR.trade_status_ind <> 'C'
			AND PCR.pcr_type_ind = 'S'
			AND PCR.final_pricing_dt >= convert(CHAR(8), getdate(), 112)
		)
GO

/* Calculate the per-trade premium */
UPDATE OTC_OPTION_PREMIUM_LOAD
SET per_trade_premium = total_premium / total_trades
GO

/* Calculate the unrealized premium */
UPDATE OTC_OPTION_PREMIUM_LOAD
SET open_premium = (per_trade_premium * open_trades) / 100.0
GO

/* Adjust for cost/revenue */
UPDATE OTC_OPTION_PREMIUM_LOAD
SET open_premium = open_premium * - 1.0
WHERE cost_revenue_ind = 'C'
GO

GRANT ALL
	ON OTC_OPTION_PREMIUM_LOAD
	TO tempest_users
GO