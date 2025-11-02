package com.qiu.version;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Manifest文件管理器，负责持久化版本变更
 */
public class Manifest implements AutoCloseable {
    private final String manifestPath;
    private final VersionSet versionSet;
    private FileChannel fileChannel;
    private boolean closed;

    public Manifest(String manifestPath, VersionSet versionSet) throws IOException {
        this.manifestPath = Objects.requireNonNull(manifestPath, "Manifest path cannot be null");
        this.versionSet = Objects.requireNonNull(versionSet, "VersionSet cannot be null");

        Path path = Path.of(manifestPath);

        // 确保父目录存在
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        /*
         * 注意：在 Windows 上不能同时使用 READ + APPEND，因此这里不使用 APPEND。
         * 使用 CREATE + READ + WRITE，然后在写入前把 position 设置到 fileChannel.size() 来实现追加。
         */
        this.fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.closed = false;
    }

    /**
     * 写入版本编辑到Manifest（追加写入）
     */
    public synchronized void writeEdit(VersionEdit edit) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(edit, "Version edit cannot be null");

        byte[] data = encodeVersionEdit(edit);

        // 将通道位置移动到文件末尾以实现“追加”
        fileChannel.position(fileChannel.size());

        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            fileChannel.write(buf);
        }
        fileChannel.force(true); // 强制刷盘
    }

    /**
     * 从Manifest读取下一个版本编辑（按写入顺序）
     */
    public synchronized VersionEdit readEdit() throws IOException {
        checkNotClosed();

        // 读取记录大小（4字节）
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        int bytesRead = readFully(sizeBuffer);
        if (bytesRead == -1) {
            return null; // EOF
        }
        if (bytesRead != 4) {
            throw new IOException("Failed to read manifest record size");
        }
        sizeBuffer.flip();
        int recordSize = sizeBuffer.getInt();

        if (recordSize <= 0) {
            throw new IOException("Invalid manifest record size: " + recordSize);
        }

        ByteBuffer dataBuffer = ByteBuffer.allocate(recordSize);
        bytesRead = readFully(dataBuffer);
        if (bytesRead != recordSize) {
            throw new IOException("Failed to read complete manifest record");
        }
        return decodeVersionEdit(dataBuffer.array());
    }

    /**
     * 从Manifest恢复所有版本编辑
     */
    public void recover() throws IOException {
        checkNotClosed();
        fileChannel.position(0);

        VersionEdit edit;
        while ((edit = readEdit()) != null) {
            // 使用专门的方法，避免写回Manifest
            versionSet.applyEditForRecovery(edit);
        }
    }

    /**
     * 编码版本编辑（动态缓冲，避免固定大小溢出）
     */
    private byte[] encodeVersionEdit(VersionEdit edit) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // 写入比较器名称
            if (edit.getComparatorName() != null) {
                dos.writeByte(1);
                byte[] nameBytes = edit.getComparatorName().getBytes();
                dos.writeInt(nameBytes.length);
                dos.write(nameBytes);
            }

            // 写入日志编号
            if (edit.getLogNumber() != null) {
                dos.writeByte(2);
                dos.writeLong(edit.getLogNumber());
            }

            // 写入下一个文件编号
            if (edit.getNextFileNumber() != null) {
                dos.writeByte(3);
                dos.writeLong(edit.getNextFileNumber());
            }

            // 写入最后序列号
            if (edit.getLastSequence() != null) {
                dos.writeByte(4);
                dos.writeLong(edit.getLastSequence());
            }

            // 写入新增文件
            for (VersionEdit.LevelFile newFile : edit.getNewFiles()) {
                dos.writeByte(5);
                dos.writeInt(newFile.getLevel());

                FileMetaData file = newFile.getFile();
                dos.writeLong(file.getFileNumber());
                dos.writeLong(file.getFileSize());

                byte[] smallestKey = file.getSmallestKey();
                dos.writeInt(smallestKey.length);
                dos.write(smallestKey);

                byte[] largestKey = file.getLargestKey();
                dos.writeInt(largestKey.length);
                dos.write(largestKey);
            }

            // 写入删除文件
            for (VersionEdit.DeletedFile deletedFile : edit.getDeletedFiles()) {
                dos.writeByte(6);
                dos.writeInt(deletedFile.getLevel());
                dos.writeLong(deletedFile.getFileNumber());
            }

            // 结束标记
            dos.writeByte(0);
            dos.flush();

            byte[] recordData = baos.toByteArray();

            // 在前面加4字节长度前缀
            ByteBuffer finalBuf = ByteBuffer.allocate(4 + recordData.length);
            finalBuf.putInt(recordData.length);
            finalBuf.put(recordData);
            return finalBuf.array();
        } catch (IOException e) {
            // 不太可能发生（内存流），但包装为 RuntimeException 以便调用方处理
            throw new RuntimeException("Failed to encode VersionEdit", e);
        }
    }

    /**
     * 解码版本编辑
     */
    private VersionEdit decodeVersionEdit(byte[] data) {
        VersionEdit edit = new VersionEdit();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        while (buffer.hasRemaining()) {
            byte tag = buffer.get();
            if (tag == 0) {
                break; // 结束标记
            }

            switch (tag) {
                case 1: // 比较器名称
                    int nameLen = buffer.getInt();
                    byte[] nameBytes = new byte[nameLen];
                    buffer.get(nameBytes);
                    edit.setComparatorName(new String(nameBytes));
                    break;

                case 2: // 日志编号
                    edit.setLogNumber(buffer.getLong());
                    break;

                case 3: // 下一个文件编号
                    edit.setNextFileNumber(buffer.getLong());
                    break;

                case 4: // 最后序列号
                    edit.setLastSequence(buffer.getLong());
                    break;

                case 5: // 新增文件
                    int level = buffer.getInt();
                    long fileNumber = buffer.getLong();
                    long fileSize = buffer.getLong();

                    int smallKeyLen = buffer.getInt();
                    byte[] smallestKey = new byte[smallKeyLen];
                    buffer.get(smallestKey);

                    int largeKeyLen = buffer.getInt();
                    byte[] largestKey = new byte[largeKeyLen];
                    buffer.get(largestKey);

                    FileMetaData fileMeta = new FileMetaData(fileNumber, fileSize, smallestKey, largestKey);
                    edit.addFile(level, fileMeta);
                    break;

                case 6: // 删除文件
                    int delLevel = buffer.getInt();
                    long delFileNumber = buffer.getLong();
                    edit.removeFile(delLevel, delFileNumber);
                    break;

                default:
                    throw new IllegalArgumentException("Unknown manifest tag: " + tag);
            }
        }

        return edit;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            if (fileChannel != null) {
                fileChannel.close();
            }
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Manifest is closed");
        }
    }

    public String getManifestPath() {
        return manifestPath;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * 从 channel 读取指定缓冲区直到填满或 EOF（返回已读字节或 -1）
     */
    private int readFully(ByteBuffer buffer) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int n = fileChannel.read(buffer);
            if (n == -1) {
                return total == 0 ? -1 : total;
            }
            total += n;
        }
        return total;
    }
}
