
DROP FUNCTION end_date;

CREATE OR REPLACE FUNCTION end_date (p_src_regulation_id VARCHAR(255),
								     p_src_measure_type VARCHAR(3),
                                  	p_regulation_role INTEGER,
                                  	p_regulation_id VARCHAR(255),
                                  	p_validity_start_date TIMESTAMP)
  RETURNS void AS $$

  DECLARE
  	l_validity_end_date TIMESTAMP;
  	rowcount INTEGER := 0;

    cur_measures CURSOR (key VARCHAR(255), end_date TIMESTAMP, measure_type VARCHAR(3)) FOR
        SELECT  *
        FROM    measures m
        WHERE   m.measure_generating_regulation_id = key
		AND     m.measure_type_id = measure_type
        AND     (m.validity_end_date IS NULL OR (m.validity_end_date > CURRENT_DATE AND m.validity_end_date > end_date));

		rowcount INTEGER := 0;

  BEGIN
    l_validity_end_date := p_validity_start_date - INTERVAL '1 DAY';

    -- get measures for old reg id and create them for new reg...

    FOR measure IN cur_measures (p_src_regulation_id, l_validity_end_date, p_src_measure_type) LOOP
        Raise Notice 'measure sid %', measure.measure_sid;

		rowcount := rowcount +1;

        -- end date measure
        INSERT INTO measures_oplog (
            measure_sid,
            measure_type_id,
            geographical_area_id,
            goods_nomenclature_item_id,
            validity_start_date,
            	validity_end_date,
            measure_generating_regulation_role,
            measure_generating_regulation_id,
            	justification_regulation_role,
            	justification_regulation_id,
            stopped_flag,
            geographical_area_sid,
            goods_nomenclature_sid,
            ordernumber,
            additional_code_type_id,
            additional_code_id,
            additional_code_sid,
            reduction_indicator,
            export_refund_nomenclature_sid,
            national,
            tariff_measure_number,
            invalidated_by,
            invalidated_at,
            	operation,
            	operation_date
        )
        VALUES (
            measure.measure_sid,
            measure.measure_type_id,
            measure.geographical_area_id,
            measure.goods_nomenclature_item_id,
            measure.validity_start_date,
        		l_validity_end_date,
            measure.measure_generating_regulation_role,
            measure.measure_generating_regulation_id,
        		p_regulation_role,
        		p_regulation_id,
            measure.stopped_flag,
            measure.geographical_area_sid,
            measure.goods_nomenclature_sid,
            measure.ordernumber,
            measure.additional_code_type_id,
            measure.additional_code_id,
            measure.additional_code_sid,
            measure.reduction_indicator,
            measure.export_refund_nomenclature_sid,
            measure.national,
            measure.tariff_measure_number,
            measure.invalidated_by,
            measure.invalidated_at,
            	'U',
            	CURRENT_DATE
        );

    END LOOP;

	INSERT INTO sp_stats (process, params, step, rowcount)
	    VALUES ('end_date',p_src_regulation_id || ',' || p_src_measure_type, 'measures', rowcount);
  END;
$$ LANGUAGE plpgsql;
