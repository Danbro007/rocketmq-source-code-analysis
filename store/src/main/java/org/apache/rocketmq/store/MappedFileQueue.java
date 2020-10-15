/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 维护多个 MappedFile，可以看做是 CommitLog。
 */
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;
    // 映射文件的存储路径
    private final String storePath;
    // 映射文件大小，默认为 1024 MB
    private final int mappedFileSize;
    // 内存映射文件集合
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
    // 创建映射文件的服务类
    private final AllocateMappedFileService allocateMappedFileService;
    // 当前刷盘的指针
    private long flushedWhere = 0;
    // 当前数据提交指针，内存中 ByteBuffer 当前的写指针，该值大于等于 flushedWhere 既刷盘指针后置于提交指针。
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 通过时间查找映射文件，判断依据是映射文件的上一次修改时间 >= 指定的时间则返回，
     * 实在找不到则返回最后一个映射文件。
     * @param timestamp 时间戳
     * @return 映射文件
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;
        // 遍历所有的映射文件数组，判断每个映射文件的最后修改时间，
        // 如果最后修改时间大于等于我们指定的时间则返回符合条件映射文件对象。
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        // 边路所有的 MappedFile
        for (MappedFile file : this.mappedFiles) {
            // 计算出文件的尾部偏移量，就是文件名 + 文件大小 1073741824 B
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            // 如果文件尾部偏移量大于 offset
            if (fileTailOffset > offset) {
                // offset 大于等于 文件的起始偏移量则设置好文件的写指针、提交指针和刷盘指针。
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    // offset小于文件的起始偏移量,说明该文件是有效文件后面创建的,释放mappedFile占用内存,删除文件
                    // 把文件添加到待删除的队列里
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }
        // 执行删除
        this.deleteExpiredFile(willRemoveFiles);
    }
    // 真正执行删除过期文件
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {
            // 迭代器遍历每个过期文件
            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                // 如果 MappedFile 数组里没有它则这个文件删除
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    public boolean load() {
        // 存储路径
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            // 遍历所有的存储文件，如果发现存储文件大小不为 1024 MB 说明有问题。
            for (File file : files) {
                // 判断文件长度是否符合要求既 1024 MB
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    // 把存储斍转换成 MappedFile 文件，设置好一些参数并加载到内存中。
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    // 由于我们已经把存储文件存储到磁盘中了，所以这些写指针、刷盘指针、提交指针都为文件末尾。
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    // 加载到内存中
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }
    // 返回最后一个 MappedFile 文件
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        // 尝试获取最后一个 MappedFile 文件
        MappedFile mappedFileLast = getLastMappedFile();
        // 如果没有则说明需要创建一个新的，先获取这个新的 MappedFile 的消息偏移量的起始位置。
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        // 说明 MappedFile 已经满了，准备新的 MappedFile 的消息偏移量起始位置
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }
        // 如果设置的是需要创建新的 MappedFile
        if (createOffset != -1 && needCreate) {
            // 新的 MappedFile 文件路径
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            // 新的 MappedFile 的下一个文件路径，这样做是为了提升效率，因为频繁创建文件会降低磁盘 IO，当我们创建一个的同时可以异步的创建下下个 MappedFile，
            // 如果创建失败了也不要紧。
            String nextNextFilePath = this.storePath + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;
            // 创建新的 MappedFile 文件，同时创建两个。
            if (this.allocateMappedFileService != null) {
                // 新创建的 MappedFile
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    // 如果没有开启则自己手动创建 MappedFile
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                // MappedFile 数组为空，则表示这是第一次创建 MappedFile 文件，则把当前 MappedFile 文件标记为第一个。
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                // 添加到 MappedFile 数组里。
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }
    //
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }
    // 获取映射文件最小的偏移量
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }
    // 获取映射文件最大的偏移量
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }
    // 获取最新映射文件的写指针
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }
    // 通过过期时间来删除过期文件
    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            // 遍历每个 MappedFile 文件
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 获取文件的可存活时间
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                // 如果文件超过存活时间（72小时）或者要求立即删除当前文件
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    // 执行删除
                    if (mappedFile.destroy(intervalForcibly)) {
                        // 把待删除文件添加到待删除列表里
                        files.add(mappedFile);
                        deleteCount++;
                        // 批量删除的文件最大数，最大批量删除数是 10 个文件。
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }
                        // 删除文件间隔大于 0 并且还有剩余文件需要被删除
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                // 等待 100 ms
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        // 执行删除超时文件
        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * CommitLog 刷盘
     * @param flushLeastPages 每次刷盘至少需要的页数
     * @return 刷盘成功的结果
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 通过 CommitLog 的刷盘 offset 找到 MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            // 执行刷盘并返回写入了多少
            int offset = mappedFile.flush(flushLeastPages);
            // mappedFile 文件刷盘后的指针 = 文件名 + 写入大小
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            // 更新 CommitLog 的刷盘指针
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 提交数据到 FileChannel ,前提是 transientStorePoolEnable 开启了
     * @param commitLeastPages 最少提交的页数
     * @return 提交结果
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        // 通过提交 offset 找到对应 MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        // 如果找到了映射文件则把消息提交到 FileChannel
        if (mappedFile != null) {
            // 提交数据后的指针，此时数据已经在 FileChannel 里了等待刷盘
            int offset = mappedFile.commit(commitLeastPages);
            // 当前消息偏移量 + 已提交的偏移量
            long where = mappedFile.getFileFromOffset() + offset;
            // 如果相等则为 true，否则为 false
            result = where == this.committedWhere;
            // 更新已提交数据的指针
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * 通过消息的偏移量查找映射文件，如果找不到则返回第一个映射文件。
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            // 第一个映射文件
            MappedFile firstMappedFile = this.getFirstMappedFile();
            // 最后一个映射文件
            MappedFile lastMappedFile = this.getLastMappedFile();
            // 如果最后一个和第一个映射文件都存在则执行下面的操作
            if (firstMappedFile != null && lastMappedFile != null) {
                // 判断我们指定的消息偏移量是否小于第一个映射文件的消息偏移量或者大于等于（最后一个映射文件的偏移量加上当前映射文件的大小）
                // 如果符合说明这个消息偏移量超过范围了会先打印错误日志。
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    // 通过消息偏移量计算出映射文件数组的下标索引，然后通过这个下标索引找到对应的映射文件。
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    // 如果符合这三个条件会返回目标文件
                    // 1、通过下标索引找到的目标映射文件存在
                    // 2、目标文件的偏移量 < 指定的消息偏移量 < 目标文件的偏移量 + 当前映射文件的大小
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    // 遍历所有的映射文件符合下面条件：
                    // 映射文件的偏移量 <= 指定的消息偏移量 < 映射文件的偏移量 + 映射文件大小
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }
                // 到这里说明没有找到符合条件的映射文件，如果开启了 returnFirstOnNotFound 则返回第一个映射文件否则返回 null。
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }
    // 删除第一个文件
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        // 把第一个 MappedFile 文件关闭，如果成功把这个文件放入过期文件列表里。
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
