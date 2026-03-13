DROP TABLE IF EXISTS `oneid_result`;
DROP TABLE IF EXISTS `user`;

CREATE TABLE `user` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '物理主键',
    `user_id` VARCHAR(64) NOT NULL COMMENT '业务用户ID(账号ID)',
    `phone` VARCHAR(20) DEFAULT NULL COMMENT '手机号',
    `email` VARCHAR(128) DEFAULT NULL COMMENT '邮箱',
    `wx_unionid` VARCHAR(64) DEFAULT NULL COMMENT '微信unionid',
    `wx_openid` VARCHAR(64) DEFAULT NULL COMMENT '微信openid',
    `alipay_id` VARCHAR(64) DEFAULT NULL COMMENT '支付宝ID',
    `tmall_id` VARCHAR(64) DEFAULT NULL COMMENT '天猫ID',
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_user_user_id` (`user_id`),
    KEY `idx_user_phone` (`phone`),
    KEY `idx_user_email` (`email`),
    KEY `idx_user_wx_unionid` (`wx_unionid`),
    KEY `idx_user_wx_openid` (`wx_openid`),
    KEY `idx_user_alipay_id` (`alipay_id`),
    KEY `idx_user_tmall_id` (`tmall_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='OneID测试用户表';

CREATE TABLE `oneid_result` (
    `oneid` VARCHAR(32) NOT NULL COMMENT '融合后OneID',
    `user_id` VARCHAR(64) NOT NULL COMMENT '原始业务用户ID',
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`oneid`, `user_id`),
    UNIQUE KEY `uk_oneid_result_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='OneID融合结果';
