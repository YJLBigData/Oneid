from mysql_cli import fetch_tsv


SUMMARY_SQL = """
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
CROSS JOIN merged
"""


def main() -> None:
    row = fetch_tsv(SUMMARY_SQL)[0]
    metrics = [
        ("total_accounts", "原始账号总量（user表总记录数）"),
        ("distinct_user_id", "原始用户账号ID去重数"),
        ("distinct_phone", "手机号去重数"),
        ("distinct_email", "邮箱去重数"),
        ("distinct_wx_unionid", "微信unionid去重数"),
        ("distinct_wx_openid", "微信openid去重数"),
        ("distinct_alipay_id", "支付宝ID去重数"),
        ("distinct_tmall_id", "天猫ID去重数"),
        ("merged_oneid_count", "融合后OneID主体数"),
        ("fusion_rate", "融合率 = (账号数 - OneID数) / 账号数"),
        ("avg_accounts_per_oneid", "平均每个OneID关联账号数"),
    ]
    print("==== OneID Statistics / OneID统计结果 ====")
    for (label, desc), value in zip(metrics, row):
        print(f"{label:24s}: {value:<10} | {desc}")


if __name__ == "__main__":
    main()
