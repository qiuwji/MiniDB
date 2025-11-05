package com.qiu.compaction;

import com.qiu.sstable.SSTable;
import com.qiu.sstable.TableBuilder;
import com.qiu.version.FileMetaData;
import com.qiu.version.VersionEdit;
import com.qiu.version.VersionSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * 压缩任务
 */
public class Compaction {
    private final VersionSet versionSet;
    private final int level;
    private final List<FileMetaData>[] inputs; // 每个层级的输入文件
    private final List<FileMetaData> outputs;  // 输出文件元数据
    private final VersionEdit edit;
    private boolean done;

    @SuppressWarnings("unchecked")
    public Compaction(VersionSet versionSet, int level) {
        this.versionSet = Objects.requireNonNull(versionSet, "VersionSet cannot be null");
        this.level = level;
        this.inputs = new ArrayList[versionSet.getMaxLevels()];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = new ArrayList<>();
        }
        this.outputs = new ArrayList<>();
        this.edit = new VersionEdit();
        this.done = false;
    }

    public void addInputFile(int inputLevel, FileMetaData file) {
        if (inputLevel < 0 || inputLevel >= inputs.length) {
            throw new IllegalArgumentException("Invalid input level: " + inputLevel);
        }
        Objects.requireNonNull(file, "File cannot be null");
        inputs[inputLevel].add(file);
    }

    public List<FileMetaData> getInputs() {
        List<FileMetaData> all = new ArrayList<>();
        for (List<FileMetaData> li : inputs) all.addAll(li);
        return all;
    }

    public List<FileMetaData> getInputs(int level) {
        if (level < 0 || level >= inputs.length) return Collections.emptyList();
        return new ArrayList<>(inputs[level]);
    }

    public List<FileMetaData> getOutputs() {
        return new ArrayList<>(outputs);
    }

    public int getLevel() {
        return level;
    }

    public boolean isTrivialMove() {
        List<FileMetaData> allInputs = getInputs();
        if (allInputs.size() == 1 && level + 1 < versionSet.getMaxLevels()) {
            FileMetaData in = allInputs.get(0);
            for (FileMetaData n : versionSet.current().getFiles(level + 1)) {
                if (filesOverlap(in, n)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public void run() throws IOException {
        if (done) throw new IllegalStateException("Compaction already completed");

        try {
            if (isTrivialMove()) {
                doTrivialMove();
            } else {
                doCompaction();
            }
            installCompactionResults();
            done = true;
        } catch (IOException e) {
            cleanupOutputFiles();
            throw e;
        }
    }

    private void doTrivialMove() {
        FileMetaData inputFile = getInputs().get(0);
        int nextLevel = level + 1;

        edit.removeFile(level, inputFile.getFileNumber());
        edit.addFile(nextLevel, inputFile);

        System.out.printf("Trivial move: file %d from L%d to L%d%n",
                inputFile.getFileNumber(), level, nextLevel);
    }

    private void doCompaction() throws IOException {
        MergingIterator merging = createMergingIterator();
        if (merging == null || !merging.isValid()) {
            return;
        }

        byte[] lastKey = null;
        TableBuilder builder = null;

        int outputLevel = (level == 0) ? 1 : level + 1;

        // 确保输出层级不会超过最大层级
        if (outputLevel >= versionSet.getMaxLevels()) {
            outputLevel = versionSet.getMaxLevels() - 1;
            if (level != 0 && level == outputLevel) {
                System.err.println("Warning: Compacting last level back onto itself. This should not happen.");
            }
        }

        merging.seekToFirst();
        while (merging.isValid()) {
            byte[] key = merging.key();
            byte[] value = merging.value();

            if (value == null) {
                // skip
                merging.next();
                lastKey = key;
                continue;
            }

            if (builder == null || shouldFinishBuilder(builder)) {
                if (builder != null) {
                    if (builder.getEntryCount() > 0) {
                        finishBuilder(builder, outputLevel);
                    } else {
                        builder.abandon();
                    }
                }
                builder = startNewBuilder(outputLevel);
            }

            builder.add(key, value);
            lastKey = key;
            merging.next();
        }

        if (builder != null) {
            if (builder.getEntryCount() > 0) {
                finishBuilder(builder, outputLevel);
            } else {
                builder.abandon();
            }
        }

        System.out.printf("Compaction completed: L%d -> L%d, inputs=%d, outputs=%d%n",
                level, outputLevel, getInputs().size(), outputs.size());
    }

    private MergingIterator createMergingIterator() throws IOException {
        List<MergingIterator.TableIterator> tblIters = new ArrayList<>();

        for (FileMetaData inputFile : getInputs()) {
            String path = versionSet.getTableFileName(inputFile.getFileNumber());
            SSTable table = new SSTable(path);
            SSTable.TableIterator baseIter = table.iterator();
            MergingIterator.TableIterator wrapped = new MergingIterator.TableIterator(baseIter, inputFile.getFileNumber());

            if (wrapped.isValid()) {
                tblIters.add(wrapped);
            } else {
                tblIters.add(wrapped);
            }
        }

        if (tblIters.isEmpty()) return null;
        return new MergingIterator(tblIters);
    }

    private TableBuilder startNewBuilder(int outputLevel) throws IOException {
        long fileNumber = versionSet.getNextFileNumber();
        String tablePath = versionSet.getTableFileName(fileNumber);
        return new TableBuilder(tablePath);
    }

    private void finishBuilder(TableBuilder builder, int outputLevel) throws IOException {
        builder.finish();

        if (builder.getEntryCount() == 0) {
            builder.abandon();
            return;
        }

        String outPath = builder.getFilePath();
        long fileNumber = parseFileNumberFromPath(outPath);

        byte[] minKey = builder.getFirstKey();
        byte[] maxKey = builder.getLastKey();

        if (minKey == null || maxKey == null) {
            builder.abandon();
            throw new IOException("Compaction failed: Builder (file " + fileNumber + ") has entries but keys are null");
        }

        FileMetaData f = new FileMetaData(fileNumber, builder.getFileSize(), minKey, maxKey);
        outputs.add(f);
        edit.addFile(outputLevel, f);
    }

    private long parseFileNumberFromPath(String path) {
        String name = Path.of(path).getFileName().toString();
        if (name.endsWith(".sst")) name = name.substring(0, name.length() - 4);
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException e) {
            String digits = name.replaceAll("\\D", "");
            return digits.isEmpty() ? System.currentTimeMillis() : Long.parseLong(digits);
        }
    }

    private boolean shouldFinishBuilder(TableBuilder builder) {
        try {
            return builder.getFileSize() >= 2L * 1024 * 1024; // 2MB
        } catch (IOException e) {
            return true;
        }
    }

    private void installCompactionResults() throws IOException {
        // 只做逻辑删除标记，物理删除交给 VersionSet 处理
        for (FileMetaData in : getInputs()) {
            int lvl = getInputLevel(in);
            if (lvl >= 0) {
                edit.removeFile(lvl, in.getFileNumber());
                // 不再进行物理文件删除
            }
        }

        // 将新文件写入 VersionSet，物理删除在 VersionSet 中处理
        versionSet.logAndApply(edit);
    }

    private void cleanupOutputFiles() {
        // 只清理内存状态，不删除物理文件
        outputs.clear();
    }

    private int getInputLevel(FileMetaData file) {
        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i].contains(file)) return i;
        }
        return -1;
    }

    private boolean filesOverlap(FileMetaData a, FileMetaData b) {
        if (a == null || b == null) return false;
        byte[] aLargest = a.getLargestKey(), bSmallest = b.getSmallestKey();
        byte[] aSmallest = a.getSmallestKey(), bLargest = b.getLargestKey();
        if (aLargest == null || bSmallest == null || aSmallest == null || bLargest == null) {
            return false;
        }
        return !(compareKeys(aLargest, bSmallest) < 0 ||
                compareKeys(aSmallest, bLargest) > 0);
    }

    private int compareKeys(byte[] a, byte[] b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int c = Byte.compare(a[i], b[i]);
            if (c != 0) return c;
        }
        return Integer.compare(a.length, b.length);
    }

    public boolean isDone() { return done; }

    public VersionEdit getEdit() { return edit; }

    public CompactionStats getStats() {
        long inBytes = 0L, outBytes = 0L;
        for (FileMetaData in : getInputs()) inBytes += in.getFileSize();
        for (FileMetaData out : outputs) outBytes += out.getFileSize();
        return new CompactionStats(level, getInputs().size(), outputs.size(), inBytes, outBytes);
    }

    @Override
    public String toString() {
        return String.format("Compaction{level=%d, inputs=%d, outputs=%d, done=%s}",
                level, getInputs().size(), outputs.size(), done);
    }
}