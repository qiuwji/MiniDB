package com.qiu.version;

import java.util.*;

/**
 * 版本编辑记录，描述版本之间的变更
 */
public class VersionEdit {
    private String comparatorName;
    private Long logNumber;
    private Long nextFileNumber;
    private Long lastSequence;

    private final List<LevelFile> newFiles;
    private final List<DeletedFile> deletedFiles;

    public VersionEdit() {
        this.newFiles = new ArrayList<>();
        this.deletedFiles = new ArrayList<>();
    }

    // 设置比较器名称
    public void setComparatorName(String name) {
        this.comparatorName = name;
    }

    // 设置日志文件编号
    public void setLogNumber(long logNumber) {
        this.logNumber = logNumber;
    }

    // 设置下一个文件编号
    public void setNextFileNumber(long nextFileNumber) {
        this.nextFileNumber = nextFileNumber;
    }

    // 设置最后序列号
    public void setLastSequence(long lastSequence) {
        this.lastSequence = lastSequence;
    }

    // 添加新文件
    public void addFile(int level, FileMetaData file) {
        if (level < 0) {
            throw new IllegalArgumentException("Level cannot be negative");
        }
        Objects.requireNonNull(file, "File cannot be null");
        newFiles.add(new LevelFile(level, file));
    }

    // 删除文件
    public void removeFile(int level, long fileNumber) {
        if (level < 0) {
            throw new IllegalArgumentException("Level cannot be negative");
        }
        if (fileNumber < 0) {
            throw new IllegalArgumentException("File number cannot be negative");
        }
        deletedFiles.add(new DeletedFile(level, fileNumber));
    }

    // Getters
    public String getComparatorName() { return comparatorName; }
    public Long getLogNumber() { return logNumber; }
    public Long getNextFileNumber() { return nextFileNumber; }
    public Long getLastSequence() { return lastSequence; }
    public List<LevelFile> getNewFiles() { return new ArrayList<>(newFiles); }
    public List<DeletedFile> getDeletedFiles() { return new ArrayList<>(deletedFiles); }

    /**
     * 检查是否为空编辑（无实质性变更）
     */
    public boolean isEmpty() {
        return comparatorName == null &&
                logNumber == null &&
                nextFileNumber == null &&
                lastSequence == null &&
                newFiles.isEmpty() &&
                deletedFiles.isEmpty();
    }

    /**
     * 清空编辑内容
     */
    public void clear() {
        comparatorName = null;
        logNumber = null;
        nextFileNumber = null;
        lastSequence = null;
        newFiles.clear();
        deletedFiles.clear();
    }

    @Override
    public String toString() {
        return String.format("VersionEdit{comparator=%s, logNumber=%s, nextFile=%s, lastSeq=%s, newFiles=%d, deletedFiles=%d}",
                comparatorName, String.valueOf(logNumber), String.valueOf(nextFileNumber),
                String.valueOf(lastSequence), newFiles.size(), deletedFiles.size());
    }

    /**
     * 层级文件（层级 + 文件元数据）
     */
    public static class LevelFile {
        private final int level;
        private final FileMetaData file;

        public LevelFile(int level, FileMetaData file) {
            this.level = level;
            this.file = Objects.requireNonNull(file, "File cannot be null");
        }

        public int getLevel() { return level; }
        public FileMetaData getFile() { return file; }

        @Override
        public String toString() {
            return String.format("LevelFile{level=%d, file=%s}", level, file);
        }
    }

    /**
     * 删除的文件（层级 + 文件编号）
     */
    public static class DeletedFile {
        private final int level;
        private final long fileNumber;

        public DeletedFile(int level, long fileNumber) {
            this.level = level;
            this.fileNumber = fileNumber;
        }

        public int getLevel() { return level; }
        public long getFileNumber() { return fileNumber; }

        @Override
        public String toString() {
            return String.format("DeletedFile{level=%d, fileNumber=%d}", level, fileNumber);
        }
    }
}
