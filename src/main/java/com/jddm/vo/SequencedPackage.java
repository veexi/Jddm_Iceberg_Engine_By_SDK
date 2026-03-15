package com.jddm.vo;

import com.dsg.analysis.vo.PackageReturnVo;

/**
 * Wraps PackageReturnVo with a sequence number assigned at the moment of arrival
 * to preserve strict CDC order across multiple worker threads.
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
