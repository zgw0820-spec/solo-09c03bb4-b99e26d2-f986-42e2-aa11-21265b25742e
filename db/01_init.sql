-- 预置机房设备故障日志与只读查询角色。
-- 由 docker-entrypoint-initdb.d 在数据库首次初始化时执行一次。

CREATE TABLE IF NOT EXISTS fault_logs (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id    TEXT        NOT NULL,
    site         TEXT        NOT NULL,
    severity     TEXT        NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
    fault_code   TEXT        NOT NULL,
    message      TEXT        NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO fault_logs (device_id, site, severity, fault_code, message, occurred_at)
SELECT * FROM (VALUES
 ('SW-CORE-01', '北京-A区', 'CRITICAL', 'PORT_DOWN',       '核心交换机上联端口中断',           now() - interval '3 hours'),
 ('UPS-02',    '北京-A区', 'WARN',     'BATTERY_LOW',     'UPS 电池电量低于 30%',            now() - interval '2 hours'),
 ('AC-11',     '上海-B区', 'WARN',     'TEMP_HIGH',       '精密空调回风温度超阈值',           now() - interval '90 minutes'),
 ('SRV-DB-07', '上海-B区', 'CRITICAL', 'DISK_FAILURE',    '数据库节点磁盘 SMART 故障',        now() - interval '45 minutes'),
 ('FW-EDGE-03','广州-C区', 'INFO',     'SESSION_FLOOD',   '边界防火墙拦截异常会话洪泛',       now() - interval '20 minutes'),
 ('PDU-04',    '广州-C区', 'WARN',     'PHASE_IMBALANCE','PDU 三相负载不均衡',              now() - interval '8 minutes'),
 ('SW-TOR-22', '北京-A区', 'INFO',     'FLAP',            '机柜顶交换机端口抖动后恢复',       now() - interval '2 minutes')
) AS v(device_id, site, severity, fault_code, message, occurred_at)
WHERE NOT EXISTS (SELECT 1 FROM fault_logs);

-- 分析员只读角色：只有 SELECT 权限，且不能建表/写临时对象外的任何数据。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fault_ro') THEN
        CREATE ROLE fault_ro LOGIN;
    END IF;
END
$$;
ALTER ROLE fault_ro PASSWORD 'fault_ro';
-- Defence in depth: every session opened by this role defaults to read-only,
-- even a direct connection that bypasses the application pool.
ALTER ROLE fault_ro SET default_transaction_read_only = on;

REVOKE ALL ON SCHEMA public FROM fault_ro;
GRANT USAGE ON SCHEMA public TO fault_ro;
GRANT SELECT ON fault_logs TO fault_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM fault_ro;
