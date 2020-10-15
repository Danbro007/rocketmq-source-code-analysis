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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class MappedFile extends ReferenceResource {
    // 操作系统每页的大小，默认为 4 KB，当页的内部数据大小超过 4 KB，之后的数据会放入新的页里。
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 当前 JVM 实例中 MappedFile 虚拟内存
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    // 当前 JVM 实例中 MappedFile 的个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    // 当前映射文件的写指针
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    // 当前映射文件的提交指针
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    // 当前映射文件的刷盘指针
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // 每个映射文件的大小
    protected int fileSize;
    // 文件通道
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     *
     * 如果写缓存不为 null 则会把消息将先放在写缓存里，然后重新放入FileChannel
     *
     * Message -----> WriteBuffer ------> FileChannel -----> 磁盘
     *          put               commit               flush
     */
    protected ByteBuffer writeBuffer = null;
    /// 堆外内存池
    protected TransientStorePool transientStorePool = null;
    // MappedFile 文件名
    private String fileName;
    // 当前映射文件的处理偏移量
    private long fileFromOffset;
    // 映射文件对象
    private File file;
    // 物理映射文件对应的内存映射缓冲区
    private MappedByteBuffer mappedByteBuffer;
    // 文件最后一次内容写入时间
    private volatile long storeTimestamp = 0;
    // 是否是 MappedFileQueue 队列中第一个文件标志位
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }
    // 开启了 transientStorePoolEnable 既使用了堆外内存
    // 这里先把数据存储到堆外内存，然后通过 commit 线程将数据提交到内存映射 Buffer 中，再通过 flush 线程将内存映射 Buffer 中数据持久化到磁盘中。
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        // 开启了堆外内存
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }
    // MappedFile 的初始化操作，主要把 CommitLog 目录下的存储文件找到然后把它们加载到内存中
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        // 通过文件名获取 MappedFile 的起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        // 校验文件的目录
        ensureDirOK(this.file.getParent());

        try {
            // 获取文件的 channel
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 堆外内存映射
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            // 对虚拟内存加上 MappedFile 的文件大小
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            // MappedFile 总数自增加 1
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 存储消息到映射文件里
     * @param messageExt 要存储的消息
     * @param cb 存储消息后要执行的回调函数
     * @return 存储结果
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        // 当前映射文件的写指针
        int currentPos = this.wrotePosition.get();
        // 如果写指针大于映射文件大小既 1024 MB 则说明文件已经满了，因为之前我们已经判断过映射文件是否已满
        // 如果这里还出现文件已满那只能返回 UNKNOWN_ERROR 结果
        if (currentPos < this.fileSize) {
            // 如果开启了 transientStorePoolEnable 则会使用 writeBuffer ，否则是使用 mappedByteBuffer 开辟一个缓冲区。
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            // 在 ByteBuffer 里标记要写入的位置
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            // 写入单条消息
            if (messageExt instanceof MessageExtBrokerInner) {
                // 执行回调函数，它的作用是把消息序列化并写入到缓冲区里最后返回写入结果
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            }
            // 写入多条消息
            else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 更新写入指针的位置
            this.wrotePosition.addAndGet(result.getWroteBytes());
            // 记录存储时间
            this.storeTimestamp = result.getStoreTimestamp();
            // 返回结果
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 追加消息
     * @param data 要写入的数据
     * @return
     */
    public boolean appendMessage(final byte[] data) {
        // 当前的写指针位置
        int currentPos = this.wrotePosition.get();
        // 判断要写入的数据是否超过文件剩余容量
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                // 把数据写入到 FileChannel 里
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 更新写指针
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     * 从 offset 到 offset + length 之间的数据会被写入到文件上
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     *
     * 执行刷盘操作并返回更新后的刷盘指针
     *
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        // 判断是否符合刷盘条件
        if (this.isAbleToFlush(flushLeastPages)) {
            // 上同步锁
            if (this.hold()) {
                // 获取当前 MappedFile 的读指针
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    // 把在 FileChannel 或者 mappedByteBuffer 里的数据强制写入磁盘中。
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        // 从 mmap 刷新数据到磁盘
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                // 更新当前 MappedFile 的刷盘位置
                this.flushedPosition.set(value);
                // 释放同步锁
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                // 更新刷盘指针
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 提交数据到 FileChannel
     * @param commitLeastPages 本次提交的最小页数。
     * @return 提交完后返回提交指针
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            // writeBuffer 为空说明没有数据需要提交到 channel，仅仅把当前的写指针当作提交指针返回。
            return this.wrotePosition.get();
        }
        // 判断是否满足提交条件
        if (this.isAbleToCommit(commitLeastPages)) {
            // 加同步锁
            if (this.hold()) {
                // 真正执行提交操作，把 MappedFile 里的数据放入 writeBuffer 里然后在提交到 FileChannel 里
                // 此时的数据存储在 FileChannel 里
                commit0(commitLeastPages);
                // 释放锁
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        // 此时所有的脏数据都已经被提交到 FileCchannel 里，FileChannel 里的数据就等着存储到磁盘上。
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            // 把 writeBuffer 里的数据放入 transientStorePool 里的一个堆外内存中用来临时存储
            this.transientStorePool.returnBuffer(writeBuffer);
            // 清空 writeBuffer
            this.writeBuffer = null;
        }
        // 返回提交数据后的指针
        return this.committedPosition.get();
    }

    /**
     * 把消息放入 writeBuffer 里，然后把 writeBuffer 里的数据写入到 FileChannel
     */
    protected void commit0(final int commitLeastPages) {
        // 写指针
        int writePos = this.wrotePosition.get();
        // 上次提交位置
        int lastCommittedPosition = this.committedPosition.get();
        // 进入这个判断说明还有数据没有提交上去
        if (writePos - this.committedPosition.get() > 0) {
            try {
                // 把提交的数据放入 writeBuffer 里
                ByteBuffer byteBuffer = writeBuffer.slice();
                // 设置提交位置为上次提交的位置
                byteBuffer.position(lastCommittedPosition);
                // 设置共享内存区数据的最大提交量
                byteBuffer.limit(writePos);
                // 设置 channel 位置为上次提交位置
                this.fileChannel.position(lastCommittedPosition);
                // 将从 lastCommittedPosition 到 writePos 的数据提交到 FileChannel 中
                this.fileChannel.write(byteBuffer);
                // 更新提交位置
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();
        // 映射文件是否已满
        if (this.isFull()) {
            return true;
        }
        // 最小刷盘页数大于 0
        if (flushLeastPages > 0) {
            // 计算是否满足刷盘条件
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        // 最小刷盘页数小于 0 则每次有更新都会执行刷盘操作
        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();
        // 当前的 MappedFile 已满
        if (this.isFull()) {
            return true;
        }
        // 文件内容长度是否达到 commitLeastPages 页，如果达到则执行刷盘操作。
        // 如果 commitLeastPages 为 0 表示立即提交。
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 查找 pos 到当前最大可读之间的数据
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        // 获取最大可读指针
        int readPosition = getReadPosition();
        // 如果 pos < 最大可读指针 && pos >= 0
        if (pos < readPosition && pos >= 0) {
            // 尝试上锁
            if (this.hold()) {
                // 复制 mappedByteBuffer 读共享区
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                // 设置该共享区的位置
                byteBuffer.position(pos);
                // pos 到 最大可读指针之间数据的长度
                int size = readPosition - pos;
                // 通过这个共享区创建新的缓冲区
                ByteBuffer byteBufferNew = byteBuffer.slice();
                // 设置缓冲区的容量
                byteBufferNew.limit(size);
                //
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     *
     * 获取当前文件的最大可读指针，如果 writeBuffer 为空则直接返回当前的写指针，如果 writeBuffer 不为空，则返回上一次提交的指针。
     * 在 MappedFile 设置中，只有提交了的数据（写入到 MappedByteBuffer 或者 FileChannel 中的）才是安全的。
     *
     */
    public int getReadPosition() {
        // 如果 writeBuffer 为 null ，则刷盘的位置就是当前的写指针否则为上一次提交的指针。
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
