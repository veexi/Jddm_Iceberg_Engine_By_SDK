package com.jddm.common;

public class ConstantColType {

	public static enum OraTypeMappingSet{
		
		ora_0x01((byte)0x01,"string"),
		ora_0x02((byte)0x02,"string"),
		ora_bigint((byte)0x02,"bigint"),
		ora_double((byte)0x02,"double"),
		ora_decimal((byte)0x02,"decimal"),
		ora_0x08((byte)0x08,"string"),
		ora_0x09((byte)0x09,"string"),
		ora_0x12((byte)0x12,"timestamp"),  //0x0c datetime
		ora_0xb4((byte)0xb4,"timestamp"),  //0xb4 0xb4 --->TIMESTAMP 2012-12-12 12:12:12.123456789
		ora_0xb5((byte)0xb5,"timestamp"),  //0xb5 --->TIMESTAMP 2012-12-12 12:12:12.123456789 +时区
		ora_other((byte)0x00,"string");
		
		private final byte oraType;
		private final String hiveColType;
		
		private OraTypeMappingSet(byte oraType,String hiveColType) {
			this.oraType = oraType;
			this.hiveColType = hiveColType;
		}

		public byte getOraType(byte inputType) {
			
	        return oraType;
		}

		public String getHiveColType() {
			return hiveColType;
		}

	}
}
