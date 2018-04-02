import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTask1 {
    private static Connection connection; // 管理Hbase
    private static Admin admin; // 管理Hbase数据库的表信息

    /**
     * (1) 列出Hbase所有的表的相关信息，例如表名
     * 命令：
     *      hbase> list
     */
    public static void listTables() throws IOException {
        HbaseBasicApi.init();//建立连接
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
            System.out.println("表名:"+hTableDescriptor.getNameAsString());
        }
        HbaseBasicApi.close();//关闭连接
    }

    /**
     * (2) 在终端打印出指定的表的所有的记录
     * 命令：
     *      hbase> scan 's1'
     */
    //打印一条记录的详情
    private static void printRecoder(Result result)throws IOException{
        for(Cell cell:result.rawCells()){
            System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
            System.out.print("列簇: "+new String(CellUtil.cloneFamily(cell)));
            System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
            System.out.print(" 值: "+new String(CellUtil.cloneValue(cell)));
            System.out.println("时间戳: "+cell.getTimestamp());
        }
    }
    //在终端打印出指定的表的所有记录数据
    public static void getData(String tableName)throws  IOException{
        HbaseBasicApi.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result:scanner){
            printRecoder(result);
        }
        HbaseBasicApi.close();
    }

    /**
     * (3) 向已创建好的表添加和删除指定的列或列族
     * 命令：
     *      hbase> create 's1','score'
     *      hbase> put 's1','zhangsan','score:Math','69'
     *      hbase> delete 's1','zhangsan','score:Math'
     * 用法：insertRow("s1",'zhangsan','score','Math','69')
     *      deleteRow("s1",'zhangsan','score','Math')
     */
    //向表添加数据
    public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
        HbaseBasicApi.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
        HbaseBasicApi.close();
    }


    //删除数据
    public static void deleteRow(String tableName,String rowKey,String colFamily,String col) throws IOException {
        HbaseBasicApi.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        //删除指定列族
        delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        table.close();
        HbaseBasicApi.close();
    }

    /**
     * (4) 清空指定的表的所有记录数据
     * 命令：
     *      hbase> truncate 's1'
     */
    //清空指定的表的所有记录数据
    public static void clearRows(String tableName)throws IOException{
        HbaseBasicApi.init();
        TableName tablename = TableName.valueOf(tableName);
        admin.disableTable(tablename);
        admin.deleteTable(tablename);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        admin.createTable(hTableDescriptor);
        HbaseBasicApi.close();
    }

    /**
     * (5) 统计表的行数
     * 命令：
     *      hbase> count 's1'
     */
    //(5)统计表的行数
    public static void countRows(String tableName)throws IOException{
        HbaseBasicApi.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        int num = 0;
        for (Result result = scanner.next();result!=null;result=scanner.next()){
            num++;
        }
        System.out.println("行数:"+ num);
        scanner.close();
        HbaseBasicApi.close();
    }


}
