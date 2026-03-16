package com.jddm.vo;

import org.apache.iceberg.data.GenericRecord;

/**
 * 单条数据操作对象（RowOperation）。
 * 封装了从物理 Binlog 解析出来的单行变更，包含操作类型、新旧记录以及位移信息。
 * 主要用于在内存 Batch 阶段进行主键合并（Merge-on-PK）以及 Equality Delete 逻辑的判定。
 */
public class RowOperation {

    public enum OpType {
        INSERT,
        DELETE,
        UPDATE
    }
    private boolean fullLoad = false;
    public boolean isFullLoad() { return fullLoad; }
    public void setFullLoad(boolean fullLoad) { this.fullLoad = fullLoad; }
    private final OpType type;

    /** INSERT/UPDATE 操作产生的新记录流；DELETE 操作下通常为 null */
    private final GenericRecord newRecord;

    /** DELETE/UPDATE 操作关联的历史旧记录；INSERT 操作下通常为 null */
    private final GenericRecord oldRecord;

    /** 物理偏移量（Binlog Offset），用于确保相同主键在合并冲突时，以“最后一次操作”为准 */
    private final long binlogOffset;

    private RowOperation(OpType type, GenericRecord newRecord, GenericRecord oldRecord, long binlogOffset) {
        this.type        = type;
        this.newRecord   = newRecord;
        this.oldRecord   = oldRecord;
        this.binlogOffset = binlogOffset;
    }


    public static RowOperation insert(GenericRecord record, long offset) {
        return new RowOperation(OpType.INSERT, record, null, offset);
    }

    public static RowOperation delete(GenericRecord record, long offset) {
        return new RowOperation(OpType.DELETE, null, record, offset);
    }

    public static RowOperation update(GenericRecord oldRecord, GenericRecord newRecord, long offset) {
        return new RowOperation(OpType.UPDATE, newRecord, oldRecord, offset);
    }

    // ---------- Getter ----------

    public OpType getType()          { return type; }
    public GenericRecord getNewRecord()  { return newRecord; }
    public GenericRecord getOldRecord()  { return oldRecord; }
    public long getBinlogOffset()    { return binlogOffset; }

    @Override
    public String toString() {
        return "RowOperation{type=" + type + ", offset=" + binlogOffset + "}";
    }
}