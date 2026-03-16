package com.jddm.vo;

import com.dsg.analysis.vo.PackageReturnVo;

/**
 * 有序数据包（SequencedPackage）。
 * 包装了底层的 PackageReturnVo，并分配一个全局单调递增的序列号。
 * 核心目的：在多线程并发处理多个 CDC 数据包时，通过序列号强制维持数据到达的先后顺序，避免跨线程 DML 冲突。
 */
public class SequencedPackage {
    private final PackageReturnVo packageVo;
    private final long sequence;

    public SequencedPackage(PackageReturnVo packageVo, long sequence) {
        this.packageVo = packageVo;
        this.sequence = sequence;
    }

    public PackageReturnVo getPackageVo() {
        return packageVo;
    }

    public long getSequence() {
        return sequence;
    }
}
