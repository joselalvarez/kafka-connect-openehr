WITH ehr_status_audit_details_view AS (
    SELECT
        sv.vo_id AS uid,
        c.ehr_id,
        ad.change_type,
        ad.time_committed,
        CASE
            WHEN ad.change_type = 'deleted' THEN cv.sys_version - 1
            ELSE cv.sys_version
        END AS sys_version,
        sv.archived,
        encode(substring(c.ehr_id::text, 0, 5)::bytea, 'hex')::int % ? AS table_partition
    FROM
        audit_details ad
    INNER JOIN
        contribution c ON c.has_audit = ad.id
    INNER JOIN (
        SELECT
            contribution_id,
            sys_version,
            vo_id,
            false AS archived
        FROM
            ehr_status_version
        UNION
        SELECT
            contribution_id,
            sys_version,
            vo_id,
            true AS archived
        FROM
            ehr_status_version_history
    ) AS sv ON sv.contribution_id = c.id
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
    jsonb_object_agg(coalesce(path, ''), fragment) AS aggregate
FROM (
    SELECT
        adw.table_partition,
        adw.uid,
        adw.ehr_id,
        adw.change_type,
        adw.time_committed,
        adw.sys_version,
        coalesce(sd.entity_idx, sdh.entity_idx) AS path,
        coalesce(sd."data", sdh."data") AS fragment
    FROM
    (
        SELECT
            *
        FROM
            ehr_status_audit_details_view
        WHERE
            true
            {{#fromDate}} AND time_committed >= ? {{/fromDate}}
            {{#toDate}} AND time_committed <= ? {{/toDate}}
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
        ehr_status_data sd ON sd.vo_id = adw.uid AND NOT adw.archived
    LEFT JOIN
        ehr_status_data_history sdh ON sdh.vo_id = adw.uid AND sdh.sys_version = adw.sys_version AND adw.archived
) AS fragments
GROUP BY
    table_partition,
    uid,
    ehr_id,
    change_type,
    time_committed,
    sys_version
ORDER BY
    time_committed ASC,
    uid ASC,
    sys_version ASC;
