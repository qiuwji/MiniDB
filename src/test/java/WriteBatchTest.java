import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class WriteBatchTest {

    // 模拟日志常量类（原代码中引用）
    public static class LogConstants {
        public static final int MAX_RECORD_SIZE = 1024 * 1024; // 假设最大1MB
    }

    // 模拟WriteBatch中的操作类
    public static class WriteBatch {
        private final List<WriteOp> operations = new ArrayList<>();

        public List<WriteOp> getOperations() {
            return operations;
        }

        // 添加插入操作
        public void put(byte[] key, byte[] value) {
            operations.add(new WriteOp(key, value, false));
        }

        // 添加删除操作
        public void delete(byte[] key) {
            operations.add(new WriteOp(key, null, true));
        }

        // 操作实体类
        public static class WriteOp {
            public final byte[] key;
            public final byte[] value;
            public final boolean isDelete;

            public WriteOp(byte[] key, byte[] value, boolean isDelete) {
                this.key = key;
                this.value = value;
                this.isDelete = isDelete;
            }
        }
    }

    // 原序列化方法（保持不变）
    private static byte[] serializeWriteBatch(WriteBatch batch) {
        int size = 8; // 序列号（8字节）
        for (WriteBatch.WriteOp op : batch.getOperations()) {
            size += 1; // 操作类型（1字节）
            size += 4 + op.key.length; // 键长度 + 键数据
            if (!op.isDelete) {
                size += 4 + op.value.length; // 值长度 + 值数据
            }
        }

        if (size > LogConstants.MAX_RECORD_SIZE) {
            throw new IllegalStateException("WriteBatch too large: " + size + " bytes");
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(0); // 序列号初始为0

        for (WriteBatch.WriteOp op : batch.getOperations()) {
            buffer.put((byte) (op.isDelete ? 0 : 1)); // 操作类型

            // 写入键（非空校验）
            if (op.key.length > 0) {
                buffer.putInt(op.key.length);
                buffer.put(op.key);
            } else {
                throw new IllegalArgumentException("Key cannot be empty");
            }

            // 写入值（仅插入操作，非空校验）
            if (!op.isDelete) {
                if (op.value != null && op.value.length > 0) {
                    buffer.putInt(op.value.length);
                    buffer.put(op.value);
                } else {
                    throw new IllegalArgumentException("Value cannot be null or empty for put operation");
                }
            }
        }

        return buffer.array();
    }

    // 测试主方法
    public static void main(String[] args) {
        try {
            // 1. 创建一个包含插入和删除操作的WriteBatch
            WriteBatch batch = new WriteBatch();
            batch.put("name".getBytes(), "Alice".getBytes()); // 插入操作
            batch.delete("age".getBytes());                   // 删除操作
            batch.put("id".getBytes(), "1001".getBytes());    // 插入操作

            // 2. 执行序列化
            byte[] serialized = serializeWriteBatch(batch);
            System.out.println("序列化成功，总长度：" + serialized.length + " 字节");

            // 3. 解析并打印序列化结果（验证格式）
            parseAndPrint(serialized);

        } catch (IllegalArgumentException e) {
            System.err.println("参数错误：" + e.getMessage());
        } catch (IllegalStateException e) {
            System.err.println("状态错误：" + e.getMessage());
        }
    }

    // 解析序列化后的字节数组并打印格式细节
    private static void parseAndPrint(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // 读取头部：序列号（8字节）
        long sequence = buffer.getLong();
        System.out.println("头部：序列号 = " + sequence + "（8字节）");

        // 读取操作条目
        int opIndex = 0;
        while (buffer.hasRemaining()) {
            opIndex++;
            System.out.println("\n操作" + opIndex + "：");

            // 操作类型（1字节）
            byte opType = buffer.get();
            System.out.println("  操作类型 = " + (opType == 0 ? "删除（0）" : "插入（1）") + "（1字节）");

            // 键长度（4字节）
            int keyLen = buffer.getInt();
            System.out.println("  键长度 = " + keyLen + "（4字节）");

            // 键数据（keyLen字节）
            byte[] key = new byte[keyLen];
            buffer.get(key);
            System.out.println("  键数据 = " + new String(key) + "（" + keyLen + "字节）");

            // 若为插入操作，读取值
            if (opType == 1) {
                int valLen = buffer.getInt();
                System.out.println("  值长度 = " + valLen + "（4字节）");

                byte[] value = new byte[valLen];
                buffer.get(value);
                System.out.println("  值数据 = " + new String(value) + "（" + valLen + "字节）");
            }
        }
    }
}