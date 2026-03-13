WITH src AS (
    SELECT
        COUNT(*) AS total_accounts,
        COUNT(DISTINCT user_id) AS distinct_user_id,
        COUNT(DISTINCT NULLIF(phone, '')) AS distinct_phone,
        COUNT(DISTINCT NULLIF(email, '')) AS distinct_email,
        COUNT(DISTINCT NULLIF(wx_unionid, '')) AS distinct_wx_unionid,
        COUNT(DISTINCT NULLIF(wx_openid, '')) AS distinct_wx_openid,
        COUNT(DISTINCT NULLIF(alipay_id, '')) AS distinct_alipay_id,
        COUNT(DISTINCT NULLIF(tmall_id, '')) AS distinct_tmall_id
    FROM `user`
),
merged AS (
    SELECT COUNT(DISTINCT oneid) AS oneid_cnt
    FROM `oneid_result`
)
SELECT
    src.total_accounts,
    src.distinct_user_id,
    src.distinct_phone,
    src.distinct_email,
    src.distinct_wx_unionid,
    src.distinct_wx_openid,
    src.distinct_alipay_id,
    src.distinct_tmall_id,
    merged.oneid_cnt,
    ROUND((src.distinct_user_id - merged.oneid_cnt) / src.distinct_user_id, 4) AS fusion_rate,
    ROUND(src.total_accounts / merged.oneid_cnt, 2) AS avg_accounts_per_oneid
FROM src
CROSS JOIN merged;
