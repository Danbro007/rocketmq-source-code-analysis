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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    /**
     * hash 总 Slot 数 500W
     */
    private final int hashSlotNum;
    /**
     * 能存的索引数 2000W
     */
    private final int indexNum;
    /**
     * 对应的映射文件
     */
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 先获取 MessageKey 的 HashCode，然后取模获取 Slot 的位置，
     * @param key
     * @param phyOffset
     * @param storeTimestamp
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 1、先判断 IndexFile 里存储的 MsgKey 数量有没有超过 2000W 个
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 2、获取 MessageKey 的 HashCode
            int keyHash = indexKeyHashMethod(key);
            // 3、对 MessageKey 的 hashcode 取模获取要存放的 Slot 数，hashSlotNum 默认是 500W 个。
            int slotPos = keyHash % this.hashSlotNum;
            // 4、absSlotPos = 40 + slotPos + 4
            // 因为 IndexFile 文件的头 40 个字节要存储 IndexHeader 的，每个 Slot 的大小为 4 个字节，所以要做下调整。
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                // 5、如果存在 hash 冲突，获取这个 slot 存的前一个 index 的计数，如果没有则值为0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                // 6、计算当前 Msg 的存储时间和第一条 Msg 相差秒数
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;
                // 对第一条 MSg 的时间进行校正
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                // 7、获取真正存储的索引位置，40 + 4 * 500W + 当前索引数 * 20
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                // 8、生产一个 Index 的 Unit 内容
                // 在 absIndexPos 位置设置 MessageKey 的 HashCode
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 在 absIndexPos + 4 设置当前信息 CommitLog Offset
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // absIndexPos + 12 的位置设置当前消息的存储时间与第一条 Msg 的时间差
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // absIndexPos + 16 的位置设置与当前消息 MessageKey 的 HashCode 产生哈希冲突的前一个 Index 的 offset
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                // 9、更新 Index 的总数
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                // 10、如果是第一条消息，更新 header 中的起始 offset 和起始 time
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }
                // 11、已使用的 hash  Slot 数 + 1
                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                // index 数自增1
                this.indexHeader.incIndexCount();
                // 更新 IndexFile 的最大消息 offset
                this.indexHeader.setEndPhyOffset(phyOffset);
                // 更新存储时间
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    /**
     * @param begin 开始时间
     * @param end 结束时间
     * @return
     */
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 从 IndexFile 文件里读取 offset
     * @param phyOffsets 存储消息在 CommitLog Offset 的列表
     * @param key MessageKey
     * @param maxNum 最大读取数
     * @param begin 起始 offset
     * @param end 结束 offset
     * @param lock 锁
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            // 1、获取查询消息 MessageKey 的 HashCode
            int keyHash = indexKeyHashMethod(key);
            // 获取 Slot 的位置
            int slotPos = keyHash % this.hashSlotNum;
            // 计算出真正的位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }
                // 2、获取该 Slot 上存储的 index
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }
                // 对获取的 index 和 IndexFile 的 header 进行校验
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    // 通过 Index 找到到哪个 IndexLinkedList 查找，这是因为有 hash 冲突，所以最多会找到 maxNum 条。
                    // 这个 IndexLinkedList 是逻辑连续，物理上不是连续的。
                    for (int nextIndexToRead = slotValue; ; ) {
                        // 查询到的消息数量 >= 最大查询数则跳出循环
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        /**
                         * 回顾下 IndexFile 文件的数据结构
                         * ----------------------------------------------|
                         * Header       Slot Table     Index Linked List |
                         *-----------------------------------------------|
                         * 40 Byte      500W * 4 Byte    Index * 20 Byte |
                         * ----------------------------------------------|
                         *
                         * Index 对应的数据就存储在 Index Linked List 里
                         *
                         * 获取消息在 IndexFile 中的索引位置
                         *
                         * Index 的位置 =  40 + 500W * 4 + Index * 20 Byte
                         *
                         */
                        //3、找到index的位置
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;
                        /**
                         * 这里我们需要回顾下一个 Index 存储单元的数据结构：
                         * -----------------------------------------------------------------|
                         * key hashCode     CommitLog offset    timestamp   NextIndexOffset |
                         * -----------------------------------------------------------------|
                         *    4 Byte            8 Byte           4 Byte      4 Byte        |
                         *------------------------------------------------------------------|
                         * 这样一个存储单元大小为 20 Byte。
                         *
                         * HashCode 冲突会生成一个单向链表。
                         *
                         * 最新的数据在单向链表的首位，所以这里的 NextIndexOffset 其实是当前消息的上一条消息 offset。
                         *
                         */
                        // MessageKey 的 hashCode
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        // 4、在 CommitLog 中的 offset
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        // 时间戳
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        // 5、相同 hashcode 的上一条消息的 offset
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }
                        // * 1000 相当于由原来的 s 转成 ms
                        timeDiff *= 1000L;
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);
                        // 6、如果 MessageKey 的 hashCode 和时间匹配的把消息的 offset 放入 phyOffsets 里。
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }
                        // 7、往前继续找，直到 Index 为 0
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
