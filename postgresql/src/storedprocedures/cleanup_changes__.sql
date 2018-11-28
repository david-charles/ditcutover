DROP FUNCTION cleanup_changes_since;

CREATE OR REPLACE FUNCTION cleanup_changes_since (since_date VARCHAR)
  RETURNS VOID AS $$

DECLARE

	cur_tables CURSOR FOR
        SELECT      t.table_name AS table_name
		FROM        information_schema.tables AS t
		WHERE       t.table_name LIKE '%_oplog'
		AND         t.table_schema = 'public'
		AND         t.table_type = 'BASE TABLE'
	    ORDER BY    t.table_name;

BEGIN

    FOR tbl IN cur_tables LOOP

    	EXECUTE 'DELETE FROM ' || tbl.table_name || ' WHERE operation_date >= to_date (' || E'\'' || since_date || E'\',\'YYYY-MM-DD\')';

    END LOOP;

END;
$$ LANGUAGE plpgsql;



DROP FUNCTION cleanup_manual_changes;
CREATE OR REPLACE FUNCTION cleanup_manual_changes ()
  RETURNS VOID AS $$

DECLARE

	cur_tables CURSOR FOR
        SELECT      t.table_name AS table_name
		FROM        information_schema.tables AS t
		WHERE       t.table_name LIKE '%_oplog'
		AND         t.table_schema = 'public'
		AND         t.table_type = 'BASE TABLE'
	    ORDER BY    t.table_name;

BEGIN

    FOR tbl IN cur_tables LOOP

    	EXECUTE 'DELETE FROM ' || tbl.table_name || ' WHERE file_name IS NULL';

    END LOOP;

END;
$$ LANGUAGE plpgsql;



select cleanup_changes_since('2018-09-20');
select cleanup_manual_changes();


DROP FUNCTION cleanup_changes_today;
CREATE OR REPLACE FUNCTION cleanup_changes_today ()
  RETURNS VOID AS $$

DECLARE

	cur_tables CURSOR FOR
        SELECT      t.table_name AS table_name
		FROM        information_schema.tables AS t
		WHERE       t.table_name LIKE '%_oplog'
		AND         t.table_schema = 'public'
		AND         t.table_type = 'BASE TABLE'
	    ORDER BY    t.table_name;

BEGIN

    FOR tbl IN cur_tables LOOP

    	EXECUTE 'DELETE FROM ' || tbl.table_name || ' WHERE operation_date = current_date';

    END LOOP;

END;
$$ LANGUAGE plpgsql;



select cleanup_changes_today();