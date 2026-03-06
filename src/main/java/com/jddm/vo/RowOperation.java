package com.jddm.vo;

import org.apache.iceberg.data.GenericRecord;

/**
 * Single binlog op with old/new values. Used for PK merge before flush.
 */
public class RowOperation {

    public enum OpType {
        INSERT,
        DELETE,
        UPDATE
    }

    private final OpType type;

    /** New value for INSERT/UPDATE; null for DELETE */
    private final GenericRecord newRecord;

    /** Old value for DELETE/UPDATE; null for INSERT */
    private final GenericRecord oldRecord;

    /** Binlog offset for ordering same-PK ops */
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