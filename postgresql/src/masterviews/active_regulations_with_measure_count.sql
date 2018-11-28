-- Counts of measures by type for all active regulations

CREATE OR REPLACE VIEW active_regulations AS
SELECT  base_regulation_role AS role_type,
        br.base_regulation_id AS measure_reg_id,
        br.base_regulation_id,
        br.regulation_group_id,
        br.information_text,
        br.validity_start_date,
        br.validity_end_date,
        br.effective_end_date,
        m.measure_type_id,
        count (measure_sid)
FROM    base_regulations br
JOIN    measures m ON m.measure_generating_regulation_id = br.base_regulation_id
WHERE   br.complete_abrogation_regulation_id IS NULL
AND     br.explicit_abrogation_regulation_id IS NULL
AND     (coalesce(br.effective_end_date, br.validity_end_date, now() + interval '1 days') > now())
AND     NOT EXISTS (
        SELECT 1
        FROM regulation_replacements rr
        WHERE rr.replaced_regulation_id = br.base_regulation_id
        AND rr.replaced_regulation_role = br.base_regulation_role)
AND     br.base_regulation_id NOT LIKE 'C%'
AND     (m.validity_end_date IS NULL OR m.validity_end_date > now())
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
UNION
SELECT  modification_regulation_role AS role_type,
        mr.modification_regulation_id AS measure_reg_id,
        mr.base_regulation_id,
        br2.regulation_group_id,
        mr.information_text,
        mr.validity_start_date,
        mr.validity_end_date,
        mr.effective_end_date,
        m.measure_type_id,
        count (measure_sid)
FROM    modification_regulations mr
JOIN    base_regulations br2 ON br2.base_regulation_id = mr.base_regulation_id
        AND br2.base_regulation_role = mr.base_regulation_role
JOIN    measures m ON m.measure_generating_regulation_id = mr.modification_regulation_id
WHERE   mr.complete_abrogation_regulation_id IS NULL
AND     mr.explicit_abrogation_regulation_id IS NULL
AND     (coalesce(mr.effective_end_date, mr.validity_end_date, br2.effective_end_date, br2.validity_end_date, now() + interval '1 days') > now())
AND     NOT EXISTS (
        SELECT 1
        FROM regulation_replacements rr
        WHERE rr.replaced_regulation_id = mr.modification_regulation_id
        AND rr.replaced_regulation_role = mr.modification_regulation_role)
AND     mr.modification_regulation_id NOT LIKE 'C%'
AND     (m.validity_end_date IS NULL OR m.validity_end_date > now())
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;


select * from active_regulations;
select distinct (left(measure_reg_id,7)) from active_regulations;
