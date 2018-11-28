-- Stored procedure to test the sp_stats table...

CREATE OR REPLACE FUNCTION clonetest (param1 VARCHAR, param2 VARCHAR)
RETURNS void AS $$

  DECLARE
	rowcount INTEGER := 0;

BEGIN

	FOR counter IN 1..5 LOOP

		rowcount := rowcount +1;

	END LOOP;

	INSERT INTO sp_stats (process, params, step, rowcount)
	    VALUES ('clonetest', param1 || ',' || param2, 'measures', rowcount);

END;
$$ LANGUAGE plpgsql;




select clonetest('param1a','param2a');
select * from sp_stats;
