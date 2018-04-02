import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HbaseBasicApi {
    private static Connection connection; // 管理Hbase
    private static Admin admin; // 管理Hbase数据库的表信息

    /**
     * 建立连接
     */
    private static void init() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     * 注意：一切涉及对计算机资源的操作：如网络，文件，端口的操作，开了都要关。
     */
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * 命令：create 'student', 'score'
     */
    private static void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table exits");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String string : colFamily) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(string);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    /**
     * 插入数据
     * 用法：insertData("student","zhangsan","score","English",69)
     * 命令：put 'student', 'zhangsan', 'score:Math', '99'
     */
    private static void insertData(String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
        // 定位表名
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 定位行键
        Put put = new Put(rowKey.getBytes());
        // 定位列族，列
        put.addColumn(colFamily.getBytes(), col.getBytes(), value.getBytes());
        table.put(put);
        table.close();
    }

    /**
     * 浏览单元格数据
     * 用法：getData("student","zhangsan","score","English")
     * 命令: get 'student', 'zhangsan', {COLUMN=>'score:English'}
     */
    private static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        // 定位表名
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 定位行键
        Get get = new Get(rowKey.getBytes());
        // 定位列族，列
        Result result = table.get(get);
        try {
            System.out.println(new String(result.getValue(colFamily.getBytes(), col == null ? null : col.getBytes())));
        }catch (NullPointerException e){
            e.printStackTrace();
            System.out.println("=====空的col=====");
        }
        table.close();
    }

    public static void main(String[] args) throws IOException {
        init();
        System.out.println("=================================divider=================================");
        createTable("student",new String[]{"student_info",""});
        insertData("student","zhangsan","score","English","98");
        insertData("student","zhangsan","score","Math","99");
        insertData("student","zhangsan","score","CS","100");
        getData("student","zhangsan","score","CS");
        System.out.println("=================================divider=================================");
        close();
    }
}
