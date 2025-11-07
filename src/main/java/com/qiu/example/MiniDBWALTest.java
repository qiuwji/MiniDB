package com.qiu.example;

import com.qiu.core.MiniDB;
import com.qiu.core.Options;

/**
 * WAL 恢复测试示例：
 * 第一次运行时写入 10 条记录；
 * 第二次运行时只读取，验证 WAL 恢复是否生效。
 */
public class MiniDBWALTest {

    private static final String DB_PATH = "example_db";

    public static void main(String[] args) {
        // 控制当前运行模式：true=写入阶段，false=恢复验证阶段
        boolean writePhase = 1 == 12; // ⚠️ 第一次运行设为 true，第二次改为 false

        if (writePhase) {
            runWritePhase();
        } else {
            runReadPhase();
        }
    }

    /** 第一次运行：写入10个键值对 */
    private static void runWritePhase() {
        System.out.println("=== [写入阶段] 打开数据库并写入 10 条记录 ===");
        Options options = Options.builder()
                .createIfMissing(true)
                .build();

        long before = System.currentTimeMillis();

        try (MiniDB db = MiniDB.open(DB_PATH, options)) {
            for (int i = 0; i < 100000; i++) {
                String key = "you_and_me_forever_key_" + i;
                String value = "这是分红一段测试的代码" + i;
                db.put(key.getBytes(), value.getBytes());
//                if (i % 500 == 0) {
//                    System.out.println("写入: " + key + " -> " + value);
//                }
            }
            System.out.println("✅ 写入完成，关闭数据库以触发 WAL flush。");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long after = System.currentTimeMillis();
        System.out.println("时间流逝：" + (after - before));
    }

    /** 第二次运行：重启数据库并验证是否能恢复数据 */
    private static void runReadPhase() {
        System.out.println("=== [恢复验证阶段] 打开数据库并读取记录 ===");
        Options options = Options.builder()
                .createIfMissing(false) // 不允许重新创建，应当从 WAL 恢复
                .build();

        try (MiniDB db = MiniDB.open(DB_PATH, options)) {
            long before = System.currentTimeMillis();
            for (int i = 0; i < 2000; i += 1) {
                String key = "you_and_me_forever_key_" + i;
                byte[] valueBytes = db.get(key.getBytes());
                if (valueBytes != null) {
                    String value = new String(valueBytes);
                    System.out.println("读取成功: " + key + " -> " + value);
                } else {
                    System.out.println("❌ 未找到: " + key);
                }
            }
            long after = System.currentTimeMillis();
            System.out.println("时间流逝：" + (after - before));
            System.out.println("✅ 恢复验证完成。");
            System.out.println(db.getStats());
        } catch (Exception e) {
            System.err.println("❌ 数据库恢复失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
