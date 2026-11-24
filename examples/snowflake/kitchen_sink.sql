-- Every construct the register knows about, for the README example and tests.
SELECT
    e.event_id,
    DATEDIFF(month, e.first_seen, e.last_seen)            AS months_active,
    DATEDIFF(week,  e.first_seen, e.last_seen)            AS weeks_active,
    TRY_TO_NUMBER(e.raw_score, 10, 2)                     AS score,
    SPLIT_PART(e.path, '/', 0)                            AS root_folder,
    e.payload:device.os::string                           AS device_os,
    OBJECT_CONSTRUCT('id', e.event_id, 'os', e.payload:device.os) AS summary,
    f.value:tag::string                                   AS tag,
    e.clicks / e.impressions                              AS ctr,
    CONVERT_TIMEZONE('UTC', 'America/New_York', e.event_ts) AS event_ts_ny,
    LISTAGG(f.value:tag::string, ',') WITHIN GROUP (ORDER BY f.index) OVER (PARTITION BY e.event_id) AS all_tags
FROM raw.events e,
     LATERAL FLATTEN(input => e.payload:tags) f
ORDER BY months_active;
