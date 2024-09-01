WITH composition_audit_details_view AS (
    SELECT
        cv.vo_id AS uid,
        c.ehr_id,
        ad.change_type,
        ad.time_committed,
        CASE
            WHEN ad.change_type = 'deleted' THEN cv.sys_version - 1
            ELSE cv.sys_version
        END AS sys_version,
        cv.root_concept,
        ts.template_id,
        cv.archived,
        encode(substring(c.ehr_id::text, 0, 5)::bytea, 'hex')::int % ? AS table_partition
    FROM
        audit_details ad
    INNER JOIN
        contribution c ON c.has_audit = ad.id
    INNER JOIN (
        SELECT
            contribution_id,
            template_id,
            sys_version,
            vo_id,
            root_concept,
            false AS archived
        FROM
            comp_version
        UNION
        SELECT
            contribution_id,
            template_id,
            sys_version,
            vo_id,
            root_concept,
            true AS archived
        FROM
            comp_version_history
    ) AS cv ON cv.contribution_id = c.id
    INNER JOIN
        template_store ts ON ts.id = cv.template_id
)
SELECT
    table_partition,
    uid,
    ehr_id,
    change_type,
    time_committed,
    CASE
        WHEN change_type = 'deleted' THEN sys_version + 1
        ELSE sys_version
    END AS sys_version,
    root_concept,
    template_id,
    jsonb_object_agg(coalesce(path, ''), fragment) AS aggregate
FROM (
    SELECT
        adw.table_partition,
        adw.uid,
        adw.ehr_id,
        adw.change_type,
        adw.time_committed,
        adw.sys_version,
        adw.root_concept,
        adw.template_id,
        coalesce(cd.entity_idx, cdh.entity_idx) AS path,
        coalesce(cd."data", cdh."data") AS fragment
    FROM
    (
        SELECT
            *
        FROM
            composition_audit_details_view
        WHERE
            true
            {{#fromDate}} AND time_committed >= ? {{/fromDate}}
            {{#toDate}} AND time_committed <= ? {{/toDate}}
            {{#templateId}} AND template_id = ? {{/templateId}}
            {{#rootConcept}} AND root_concept = ? {{/rootConcept}}
            AND (
                false
                {{#partitionOffsets}}
                    OR (table_partition = ?
                    {{^empty}}
                        AND (time_committed > ?
                        OR (time_committed = ? AND uid > ?)
                        OR (time_committed = ? AND uid = ? AND sys_version > ?))
                    {{/empty}})
                {{/partitionOffsets}}
            )
        ORDER BY
            time_committed ASC,
            uid ASC,
            sys_version ASC
        LIMIT ?
    ) AS adw
    LEFT JOIN
        comp_data cd ON cd.vo_id = adw.uid AND NOT adw.archived
    LEFT JOIN
        comp_data_history cdh ON cdh.vo_id = adw.uid AND cdh.sys_version = adw.sys_version AND adw.archived
) AS fragments
GROUP BY
    table_partition,
    uid,
    ehr_id,
    change_type,
    time_committed,
    sys_version,
    root_concept,
    template_id
ORDER BY
    time_committed ASC,
    uid ASC,
    sys_version ASC;
