-- 1. 查看每个 OneID 关联的账号数 Top 20
SELECT
    oneid,
    COUNT(*) AS account_cnt
FROM `oneid_result`
GROUP BY oneid
ORDER BY account_cnt DESC, oneid
LIMIT 20;

-- 2. 查看某个 OneID 对应的全部原始账号
-- 将 ONEID00000001 替换成目标 oneid
SELECT
    r.oneid,
    u.user_id,
    u.phone,
    u.email,
    u.wx_unionid,
    u.wx_openid,
    u.alipay_id,
    u.tmall_id
FROM `oneid_result` r
JOIN `user` u
    ON r.user_id = u.user_id
WHERE r.oneid = 'ONEID00000001'
ORDER BY u.user_id;

-- 3. 查看某个 user_id 被归并到哪个 OneID
-- 将 usr_xxx 替换成目标 user_id
SELECT
    r.oneid,
    r.user_id
FROM `oneid_result` r
WHERE r.user_id = 'usr_replace_me';

-- 4. 查看某个 user_id 所属 OneID 下的所有兄弟账号
-- 将 usr_xxx 替换成目标 user_id
SELECT
    r2.oneid,
    u.user_id,
    u.phone,
    u.email,
    u.wx_unionid,
    u.wx_openid,
    u.alipay_id,
    u.tmall_id
FROM `oneid_result` r1
JOIN `oneid_result` r2
    ON r1.oneid = r2.oneid
JOIN `user` u
    ON r2.user_id = u.user_id
WHERE r1.user_id = 'usr_replace_me'
ORDER BY u.user_id;
