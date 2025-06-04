package com.jddm.utils;

import com.dsg.analysis.tableInfo.vo.TableInfoVo;
import com.jddm.boot.StartIcebergEngine;
import com.jddm.common.Constant;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.operation.ddl.IceBergTableOperationByEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * className: IcebergValidator<br>
 * description: <br>
 * author: wjl<br>
 * date: 2025/5/21 14:49<br>
 */
public class IcebergValidator {
    public static final String testTableName = "jddm_validation_temp_table";
    public static HiveCatalog catalog = new HiveCatalog();
    public static Map<String, String> properties = new HashMap<String, String>();
    public static Schema testSchema = null;
    public static final String version = IcebergValidator.class.getPackage().getImplementationVersion();

    public  void validateTableOperations() throws Exception {
        Table table = null;
        ImmutableList.Builder<GenericRecord> immTableBuilder = null;
        TableIdentifier tableIdentifier = TableIdentifier.of("default",testTableName);
        try {

            initCatalog();
//            catalog.dropTable(TableIdentifier.of("default", "jddm_validation_temp_table"));
            if (!catalog.tableExists(tableIdentifier)){
                properties.put("engine.hive.enabled", "true");
                table = createTestTable(tableIdentifier);
            }else {
                table = catalog.loadTable(tableIdentifier);
                testSchema = table.schema();
            }
            // 验证表存在
            if (!catalog.tableExists(tableIdentifier)) {
                throw new Exception("Table creation verification failed");
            }
            //插入测试数据
            GenericRecord iceBergRecord = GenericRecord.create(testSchema);
            iceBergRecord.setField("validation_build_time", StartIcebergEngine.getBuildTimeString());
            iceBergRecord.setField("validation_version",version);
            String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    .format(new Date());
            iceBergRecord.setField("validation_datetime",now);
            UUID uuid = UUID.randomUUID();
            iceBergRecord.setField("validation_uuid",uuid.toString());
/*            System.out.println("----->>>:"+iceBergRecord.toString());
            System.out.println("===> validation_build_time:"+StartIcebergEngine.getBuildTimeString());
            System.out.println("===> validation_version:"+version);
            System.out.println("===> validation_datetime:"+now);
            System.out.println("===> validation_uuid:"+uuid.toString());*/
            immTableBuilder = ImmutableList.builder();
            immTableBuilder.add(iceBergRecord);
            immTableBuilder.build();
            assert table != null;
            String filepath = table.location() + "/" + UUID.randomUUID().toString();
            OutputFile file = table.io().newOutputFile(filepath);
            DataWriter<GenericRecord> dataWriter =
                    Parquet.writeData(file)
                            .schema(testSchema)
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .overwrite()
                            .withSpec(PartitionSpec.unpartitioned())
                            .build();
            try {
                dataWriter.write(iceBergRecord);
            }finally {
                dataWriter.close();
            }
            table.newAppend().appendFile(dataWriter.toDataFile()).commit();




            // 清理测试表
//            catalog.dropTable(tableIdentifier);
        } catch (Exception e) {
            throw new Exception("Iceberg operation validation failed", e);
        }
    }
/*** *** *** ***
 * @functionName（方法名称）: createTestTable
 * @description （方法说明）: 创建测试表
 * @param       （传入参数）: null
 * @return      （返回）   : Table
 * @exception   （异常）   :
 * @author      （创建人）: wjl
 * @since       （创建时间）: 2025/5/21 15:47
 ***/
    private static Table createTestTable(TableIdentifier tableIdentifier) {
        PartitionSpec spec = null;
        Table returnIceTable = null;
        Types.NestedField nestedField0 = null;
        Types.NestedField nestedField1 = null;
        Types.NestedField nestedField2= null;
        Types.NestedField nestedField3= null;

        // 创建测试表
        List<Types.NestedField> iceBergTablefields = new ArrayList<>();
        nestedField0=Types.NestedField.required(1,"validation_build_time",Types.StringType.get());
        iceBergTablefields.add(nestedField0);
        nestedField1=Types.NestedField.optional(2,"validation_version",Types.StringType.get());
        iceBergTablefields.add(nestedField1);
        nestedField2=Types.NestedField.optional(3,"validation_datetime",Types.StringType.get());
        iceBergTablefields.add(nestedField2);
        nestedField3=Types.NestedField.optional(4,"validation_uuid",Types.StringType.get());
        iceBergTablefields.add(nestedField3);


        Set<Integer> identifierFieldIds = new HashSet<>(Collections.singletonList(1));

        testSchema = new Schema(iceBergTablefields,identifierFieldIds);
//        testSchema = new Schema(iceBergTablefields);
        spec = PartitionSpec.builderFor(testSchema).build();
        returnIceTable = catalog.createTable(tableIdentifier, testSchema,spec,properties);



        return returnIceTable;
    }
    /*** *** *** ***
     * @functionName（方法名称）: initCatalog
     * @description （方法说明）: 初始化Hive Catalog
     * @param       （传入参数）: null
     * @return      （返回）   : HiveCatalog
     * @exception   （异常）   :
     * @author      （创建人）: wjl
     * @since       （创建时间）: 2025/5/21 15:47
     ***/
    public static void initCatalog(){

        Configuration conf = new Configuration();
        catalog.setConf(conf);

        //	        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "hdfs://10.0.0.47:8020");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, Constant.fsDefaultInfo);
        //	        properties.put(CatalogProperties.URI, "thrift://10.0.0.47:9083");
        properties.put(CatalogProperties.URI, Constant.hiveMetastoreUris);
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hive.HiveCatalog");
        properties.put("format-version", "2");
        // 初始化catalog
        catalog.initialize("hive", properties);
    }
    public static byte[] uuidToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }
}

