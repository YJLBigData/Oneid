# OneID Demo

一个本地可运行的 OneID 小型示例项目，使用 MySQL 作为存储、Python 作为执行引擎，模拟“账号标识图谱 -> 主体融合 -> OneID 结果输出”的完整链路。

## 功能概览

- 创建 `user` 原始账号表和 `oneid_result` 融合结果表
- 生成带随机性、可复现、可融合的测试数据
- 基于“标识为点、关系为边”的思路做连通分量归并
- 输出融合统计结果，包含中文说明
- 提供常用排查 SQL，便于查看某个 `oneid` 或 `user_id` 的归并结果

## 项目结构

- `sql/01_create_tables.sql`: 重建 `user` / `oneid_result` 表
- `sql/02_stats.sql`: 融合统计 SQL
- `sql/03_inspection_queries.sql`: 常用排查 SQL
- `src/config.py`: MySQL 连接配置
- `src/mysql_cli.py`: MySQL CLI 调用封装
- `src/init_schema.py`: 初始化表结构
- `src/migrate_database.py`: 只迁移 OneID 项目相关表，并清理目标库无关表
- `src/generate_user_data.py`: 生成并写入随机测试数据
- `src/run_oneid.py`: 运行 OneID 融合
- `src/stats_report.py`: 输出 OneID 统计信息
- `src/run_all.py`: 一键执行全流程

## 数据设计

- 默认生成 `4200` 个自然人
- 每个自然人随机生成 `3-6` 个账号，所以总账号量通常在 `18000` 左右
- `user_id`、`wx_unionid`、`wx_openid`、`alipay_id`、`tmall_id` 为随机字符串，不使用连续整数风格
- 同一自然人的多个账号会随机共享手机号、邮箱、微信 `unionid`、支付宝 ID、天猫 ID 中的部分标识
- 每个新账号至少复用一个已出现标识，保证图关系连通，最终可以稳定归并到同一个 `oneid`

## 连接配置

默认连接本地 MySQL：

- `MYSQL_HOST=localhost`
- `MYSQL_PORT=3306`
- `MYSQL_USER=root`
- `MYSQL_PASSWORD=toor`
- `MYSQL_DB=oneid`

也支持通过环境变量覆盖。

## 执行方式

执行前先确认 MySQL 可连接：

```bash
mysql --protocol=TCP -hlocalhost -P3306 -uroot -ptoor -D oneid -e "SELECT 1;"
```

一键执行：

```bash
cd /Users/yangjinlong/app/PythonProject/OneIdTest
./.venv/bin/python src/run_all.py
```

自定义自然人数和随机种子：

```bash
./.venv/bin/python src/run_all.py --persons 5000 --seed 20260313 --batch-size 800
```

分步骤执行：

```bash
./.venv/bin/python src/init_schema.py
./.venv/bin/python src/generate_user_data.py --persons 4200 --seed 20260310
./.venv/bin/python src/run_oneid.py
./.venv/bin/python src/stats_report.py
```

从旧库迁移 OneID 项目相关表到新库 `oneid`：

```bash
./.venv/bin/python src/migrate_database.py --source-db test --target-db oneid
```

默认只保留以下项目表：

- `user`
- `oneid_result`

## 输出说明

默认会输出类似以下内容：

```text
==== OneID Statistics / OneID统计结果 ====
total_accounts          : 18743      | 原始账号总量（user表总记录数）
distinct_user_id        : 18743      | 原始用户账号ID去重数
distinct_phone          : 4200       | 手机号去重数
distinct_email          : 4200       | 邮箱去重数
distinct_wx_unionid     : 4200       | 微信unionid去重数
distinct_wx_openid      : 18743      | 微信openid去重数
distinct_alipay_id      : 4200       | 支付宝ID去重数
distinct_tmall_id       : 4200       | 天猫ID去重数
merged_oneid_count      : 4200       | 融合后OneID主体数
fusion_rate             : 0.7759     | 融合率 = (账号数 - OneID数) / 账号数
avg_accounts_per_oneid  : 4.46       | 平均每个OneID关联账号数
```

## 常用查询

直接执行：

```bash
mysql --protocol=TCP -hlocalhost -P3306 -uroot -ptoor -D oneid < sql/03_inspection_queries.sql
```

## 注意事项

- `src/init_schema.py` 会自动创建 `oneid` 数据库（如果不存在）
- `src/migrate_database.py` 会清理 `oneid` 库中的无关表，只保留项目表
- 每次执行 `src/run_all.py` 都会重建 `user` 和 `oneid_result` 表
- 项目依赖 Python 标准库，无需额外安装第三方包
- `user` 是 MySQL 关键字，SQL 中已统一使用反引号包裹
