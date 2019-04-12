import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class SparkFunctions {

    private static final Logger LOG = LoggerFactory.getLogger(SparkFunctions.class);

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().appName("TestJoin")
                .master("local[*]").enableHiveSupport()
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("hive.metastore.warehouse.dir", "target/spark-warehouse")
                .config("spark.sql.warehouse.dir", "target/spark-warehouse")
                .getOrCreate();

        String studentInfoFile = SparkFunctions.class.getClassLoader().getResource("studentInfo.csv").getPath();
        String studentGradeFile = SparkFunctions.class.getClassLoader().getResource("studentGrade.csv").getPath();
        Dataset<Row> infoDs = session.read().option("header", "true").csv(studentInfoFile);
        Dataset<Row> gradeDs = session.read().option("header", "true").csv(studentGradeFile);

        //Test join
        join(infoDs, gradeDs);

        //Test withColumn
        withColumn(infoDs, gradeDs);

    }

    private static void join(Dataset<Row> infoDs, Dataset<Row> gradeDs){
        //Test leftsemi join
        Dataset<Row> leftSemiJoin = infoDs.join(gradeDs, infoDs.col("ID").equalTo(
                gradeDs.col("ID")), "leftsemi");
        LOG.info("LeftSemi join");
        leftSemiJoin.show();
        /**
         * +---+----+---+
         * | ID|Name|AGE|
         * +---+----+---+
         * |001| Ada| 16|
         * +---+----+---+
         */

        //Test leftanti join
        Dataset<Row> leftAntiJoin = infoDs.join(gradeDs, infoDs.col("ID").equalTo(
                gradeDs.col("ID")), "leftanti");
        LOG.info("LeftAnti join");
        leftAntiJoin.show();
        /**
         * +---+-----+---+
         * | ID| Name|AGE|
         * +---+-----+---+
         * |002|David| 17|
         * +---+-----+---+
         */

        //Test inner join
        Dataset<Row> innerJoin = infoDs.join(gradeDs, infoDs.col("ID").equalTo(
                gradeDs.col("ID")), "inner");
        LOG.info("Inner join");
        innerJoin.show();
        /**
         * +---+----+---+---+-----+-------+
         * | ID|Name|AGE| ID|GRADE|SUBJECT|
         * +---+----+---+---+-----+-------+
         * |001| Ada| 16|001|   97|ENGLISH|
         * +---+----+---+---+-----+-------+
         */

        //Test outer join
        Dataset<Row> outerJoin = infoDs.join(gradeDs, infoDs.col("ID").equalTo(gradeDs.col("ID")),
                "outer");
        LOG.info("Outer join");
        outerJoin.show();
        /**
         * +----+-----+----+----+-----+-------+
         * |  ID| Name| AGE|  ID|GRADE|SUBJECT|
         * +----+-----+----+----+-----+-------+
         * |null| null|null| 003|   90|ENGLISH|
         * | 001|  Ada|  16| 001|   97|ENGLISH|
         * | 002|David|  17|null| null|   null|
         * +----+-----+----+----+-----+-------+
         */

        //Test full join
        LOG.info("Full join");
        Dataset<Row> fullJoin = infoDs.join(gradeDs, infoDs.col("ID").equalTo(gradeDs.col("ID")),
                "full");
        fullJoin.show();
        /**
         * +----+-----+----+----+-----+-------+
         * |  ID| Name| AGE|  ID|GRADE|SUBJECT|
         * +----+-----+----+----+-----+-------+
         * |null| null|null| 003|   90|ENGLISH|
         * | 001|  Ada|  16| 001|   97|ENGLISH|
         * | 002|David|  17|null| null|   null|
         * +----+-----+----+----+-----+-------+
         */
    }

    private static void withColumn(Dataset<Row> infoDs, Dataset<Row> gradeDs){

        Dataset<Row> addGenderDs = infoDs.withColumn("GENDER", lit("M"));

        addGenderDs.show();
    }
}
