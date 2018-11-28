
DROP FUNCTION clone;

CREATE OR REPLACE FUNCTION clone (p_src_regulation_id VARCHAR(255),
								  p_src_measure_type VARCHAR(3),
                                  p_regulation_role INTEGER,
                                  p_regulation_id VARCHAR(255),
                                  p_validity_start_date TIMESTAMP,
								   p_measure_type VARCHAR(3),
                                   p_regulation_group_id VARCHAR(255),
                                   p_information_text TEXT)
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

    cur_measure_components CURSOR (key INTEGER) FOR
        SELECT  *
        FROM    measure_components mc
        WHERE   mc.measure_sid = key;

    cur_measure_conditions CURSOR (key INTEGER) FOR
        SELECT  *
        FROM    measure_conditions mcn
        WHERE   mcn.measure_sid = key;

    cur_measure_condition_components CURSOR (key INTEGER) FOR
        SELECT  *
        FROM    measure_condition_components mcc
        WHERE   mcc.measure_condition_sid = key;

    cur_measure_excluded_geographical_areas CURSOR (key INTEGER) FOR
        SELECT  *
        FROM    measure_excluded_geographical_areas mega
        WHERE   mega.measure_sid = key;

    cur_footnote_association_measures CURSOR (key INTEGER) FOR
        SELECT  *
        FROM    footnote_association_measures fam
        WHERE   fam.measure_sid = key;

  BEGIN
    l_validity_end_date := p_validity_start_date - INTERVAL '1 DAY';

    -- Need to check if exists first so don't try to recreate
	IF p_regulation_group_id IS NOT NULL THEN
	    INSERT INTO base_regulations_oplog (
	        base_regulation_role,
	        base_regulation_id,
	        community_code,
	        regulation_group_id,
	        replacement_indicator,
	        stopped_flag,
	        validity_start_date,
	        information_text,
	        approved_flag,
	        published_date,
	        operation,
	        operation_date )
	    SELECT
	        p_regulation_role,
	        p_regulation_id,
	        1,
	        p_regulation_group_id,
	        0,
	        False,
	        p_validity_start_date,
	        p_information_text,
	        True,
	        CURRENT_DATE,
	        'C',
	        CURRENT_DATE
		WHERE NOT EXISTS (
		    SELECT  1
		    FROM    base_regulations_oplog
			WHERE   base_regulation_id = p_regulation_id
				    AND base_regulation_role = p_regulation_role);
	END IF;

    -- get measures for old reg id and create them for new reg...
    Raise Notice '%', p_src_regulation_id;

    FOR measure IN cur_measures (p_src_regulation_id, l_validity_end_date, p_src_measure_type) LOOP
        Raise Notice 'measure sid %', measure.measure_sid;

        -- end cloned measure
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
            	operation_date )
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
            	CURRENT_DATE);

        -- create new measure
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
            operation_date )
        VALUES (
            (measure.measure_sid + 50000000),
            	p_measure_type,
            measure.geographical_area_id,
            measure.goods_nomenclature_item_id,
            	p_validity_start_date,
            	NULL,
            	p_regulation_role,
            	p_regulation_id,
            	NULL,
            	NULL,
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
            	NULL,
            	NULL,
            'C',
            CURRENT_DATE );

        -- insert components
        FOR measure_component IN cur_measure_components (measure.measure_sid) LOOP
            Raise Notice 'measure sid % duty_expression_id %', measure.measure_sid, measure_component.duty_expression_id;
            INSERT INTO measure_components_oplog (
                measure_sid,
                duty_expression_id,
                duty_amount,
                monetary_unit_code,
                measurement_unit_code,
                measurement_unit_qualifier_code,
                operation,
                operation_date )
            VALUES (
                (measure_component.measure_sid + 50000000),
                measure_component.duty_expression_id,
                measure_component.duty_amount,
                measure_component.monetary_unit_code,
                measure_component.measurement_unit_code,
                measure_component.measurement_unit_qualifier_code,
                'C',
                CURRENT_DATE );
        END LOOP;

        -- insert conditions
        FOR measure_condition IN cur_measure_conditions (measure.measure_sid) LOOP
            Raise Notice 'measure sid % measure_condition_sid %', measure.measure_sid, measure_condition.measure_condition_sid;
            INSERT INTO measure_conditions_oplog (
                measure_condition_sid,
                measure_sid,
                condition_code,
                component_sequence_number,
                condition_duty_amount,
                condition_monetary_unit_code,
                condition_measurement_unit_code,
                condition_measurement_unit_qualifier_code,
                action_code,
                certificate_type_code,
                certificate_code,
                operation,
                operation_date )
            VALUES (
                (measure_condition.measure_condition_sid + 50000000),
                (measure_condition.measure_sid + 50000000),
                measure_condition.condition_code,
                measure_condition.component_sequence_number,
                measure_condition.condition_duty_amount,
                measure_condition.condition_monetary_unit_code,
                measure_condition.condition_measurement_unit_code,
                measure_condition.condition_measurement_unit_qualifier_code,
                measure_condition.action_code,
                measure_condition.certificate_type_code,
                measure_condition.certificate_code,
                'C',
                CURRENT_DATE );

            -- insert measure condition components
            FOR measure_condition_component IN cur_measure_condition_components (measure_condition.measure_condition_sid) LOOP
                Raise Notice 'measure_condition_sid % duty_expression_id %',
                                measure_condition.measure_condition_sid,
                                measure_condition_component.duty_expression_id;
                INSERT INTO measure_condition_components_oplog (
                    measure_condition_sid,
                    duty_expression_id,
                    duty_amount,
                    monetary_unit_code,
                    measurement_unit_code,
                    measurement_unit_qualifier_code,
                    operation,
                    operation_date )
                VALUES (
                    (measure_condition_component.measure_condition_sid + 50000000),
                    measure_condition_component.duty_expression_id,
                    measure_condition_component.duty_amount,
                    measure_condition_component.monetary_unit_code,
                    measure_condition_component.measurement_unit_code,
                    measure_condition_component.measurement_unit_qualifier_code,
                    'C',
                    CURRENT_DATE );
            END LOOP;
        END LOOP;

        -- insert footnote associations
        FOR footnote_association_measures IN cur_footnote_association_measures (measure.measure_sid) LOOP
            Raise Notice 'measure sid %  footnote_type_id %  footnote_id %',
                            measure.measure_sid,
                            footnote_association_measures.footnote_type_id,
                            footnote_association_measures.footnote_id;
            INSERT INTO footnote_association_measures_oplog (
				measure_sid,
				footnote_type_id,
				footnote_id,
                operation,
                operation_date)
			VALUES (
            	(measure.measure_sid + 50000000),
				footnote_association_measures.footnote_type_id,
				footnote_association_measures.footnote_id,
				'C',
                CURRENT_DATE );
		END LOOP;

	 rowcount := rowcount +1;

    END LOOP;

	-- Log stats
	INSERT INTO sp_stats (process, params, step, rowcount)
	    VALUES ('clone',p_src_regulation_id || ',' || p_src_measure_type, 'measures', rowcount);

  END;
$$ LANGUAGE plpgsql;
