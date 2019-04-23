import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ForTesting {
    public static void main(String[] args) {
        String SourceFileName = "";
        String sourceFilePath = ForTesting.class.getClassLoader().getResource(SourceFileName).getPath();
        SparkSession session = SparkSession.builder().appName("Test")
                .master("local[*]").enableHiveSupport()
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("hive.metastore.warehouse.dir", "target/spark-warehouse")
                .config("spark.sql.warehouse.dir", "target/spark-warehouse")
                .getOrCreate();
        Dataset<Row> ds = session.read().option("header", "true").csv(sourceFilePath);
        test(ds);
    }

    private static void test(Dataset<Row> ds){

    }
}
