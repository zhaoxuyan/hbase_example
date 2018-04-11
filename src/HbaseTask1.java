import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTask1 {

    /**
     * (1) 列出Hbase所有的表的相关信息，例如表名
     * 命令：
     * hbase> list
     */
    private static void listTables() throws IOException {
        HTableDescriptor hTableDescriptors[] = HbaseBasicApi.admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println("表名:" + hTableDescriptor.getNameAsString());
        }
    }

    /**
     * (2) 在终端打印出指定的表的所有的记录
     * 命令：
     * hbase> scan 's1'
     */
    //在终端打印出指定的表的所有记录数据
    private static void getData(String tableName) throws IOException {
        Table table = HbaseBasicApi.connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            printRecoder(result);
        }
    }

    //打印一条记录的详情
    private static void printRecoder(Result result) throws IOException {
        for (Cell cell : result.rawCells()) {
            System.out.print("行健: " + new String(CellUtil.cloneRow(cell)));
            System.out.print("列簇: " + new String(CellUtil.cloneFamily(cell)));
            System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
            System.out.println("时间戳: " + cell.getTimestamp());
        }
    }

    /**
     * (3) 向已创建好的表添加和删除指定的列或列族
     * 命令：
     * hbase> create 's1','score'
     * hbase> put 's1','zhangsan','score:Math','69'
     * hbase> delete 's1','zhangsan','score:Math'
     * 用法：insertRow("s1",'zhangsan','score','Math','69')
     * deleteRow("s1",'zhangsan','score','Math')
     */
    //向表添加数据
    private static void insertRow(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        Table table = HbaseBasicApi.connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }


    //删除数据
    private static void deleteRow(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = HbaseBasicApi.connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        //删除指定列族
//        delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.delete(delete);
        table.close();
    }

    /**
     * (4) 清空指定的表的所有记录数据
     * 命令：
     * hbase> truncate 's1'
     */
    //清空指定的表的所有记录数据
    public static void clearRows(String tableName) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        HbaseBasicApi.admin.disableTable(tablename);
        HbaseBasicApi.admin.deleteTable(tablename);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HbaseBasicApi.admin.createTable(hTableDescriptor);
    }

    /**
     * (5) 统计表的行数
     * 命令：
     * hbase> count 's1'
     */
    //(5)统计表的行数
    private static void countRows(String tableName) throws IOException {
        Table table = HbaseBasicApi.connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        int num = 0;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            num++;
        }
        System.out.println("行数:" + num);
        scanner.close();
    }

    public static void main(String[] args) throws IOException {
        HbaseBasicApi.init();//建立连接
        listTables();
        getData("student");
        System.out.println("=====================执行插入操作=====================");
        insertRow("student","s001","score","English","99");
        getData("student");
        System.out.println("=====================执行删除操作=====================");
        deleteRow("student","s001","score","English");
        getData("student");
        System.out.println("=====================统计行数操作=====================");
        getData("student");
        countRows("student");
        HbaseBasicApi.close();//关闭连接
    }
}
