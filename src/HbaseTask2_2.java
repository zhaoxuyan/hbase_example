import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTask2_2 {
    private static Connection connection; // 管理Hbase
    private static Admin admin; // 管理Hbase数据库的表信息

    /**
     * 创建表
     * @param tableName 表名
     * @param fields 各个字段名称的数组
     * @throws IOException IO error
     * 用法：createTable('student', 'score')
     */
    public static void createTable(String tableName,String[] fields) throws IOException {
        HbaseBasicApi.init();
        TableName tablename = TableName.valueOf(tableName);
        if(admin.tableExists(tablename)){
            System.out.println("table is exists!");
            admin.disableTable(tablename);
            admin.deleteTable(tablename);//删除原来的表
            System.out.println("remove old table successful");
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tablename);
        for(String str:fields){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        HbaseBasicApi.close();
    }

    /**
     * 向表，行，和字符数组fields指定的单元格中添加对应的数据values
     * @param tableName 表名
     * @param row 行
     * @param fields 各个字段名称的数组
     * @param values 对应各个字段名称的数组的值
     * @throws IOException IO error
     * 用法：addRecord('student', 's001', 'score:Math', '100');
     */
    public static void addRecord(String tableName,String row,String[] fields,String[] values) throws IOException {
        HbaseBasicApi.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for(int i = 0;i != fields.length;i++){
            Put put = new Put(row.getBytes());
            String[] cols = fields[i].split(":");
            put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
            table.put(put);
        }
        table.close();
        HbaseBasicApi.close();
    }

    /**
     * scan 某表的某column
     * @param tableName 表名
     * @param column 列族
     * @throws IOException IO error
     */
    public static void scanColumn(String tableName,String column)throws  IOException{
        HbaseBasicApi.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(column));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            showCell(result);
        }
        table.close();
        HbaseBasicApi.close();
    }
    //格式化输出
    private static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }

    /**
     * 修改数据
     * @param tableName 表名
     * @param row 行
     * @param column 列族
     * @param val 值
     * @throws IOException IO error
     */
    public static void modifyData(String tableName,String row,String column,String val)throws IOException{
        HbaseBasicApi.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(row.getBytes());
        put.addColumn(column.getBytes(),null,val.getBytes());
        table.put(put);
        table.close();
        HbaseBasicApi.close();
    }

    /**
     * 删除行
     * @param tableName 表名
     * @param row 行
     * @throws IOException IO error
     */
    public static void deleteRow(String tableName,String row)throws IOException{
        HbaseBasicApi.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(row.getBytes());
        //删除指定列族
        //delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        //delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        table.close();
        HbaseBasicApi.close();
    }

}
