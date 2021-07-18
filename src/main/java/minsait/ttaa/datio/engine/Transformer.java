package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.utils.Properties;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.when;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {

        Properties properties = new Properties();


        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        this.spark = spark;
        Dataset<Row> df = readInput(properties.position(0));

        df.printSchema();

        df = cleanData(df);
        if (properties.position(2).equals(number_1)) {
            df = menores23(df);
        }
        df = exampleWindowFunction(df);
        df = exampleWindowFunction2(df);
        df = exampleWindowFunction3(df);
        df = exampleWindowFunction4(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);

        // Uncomment when you want write your final output
        write(df, properties.position(1));
        //write2(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                catHeightByPosition.column(),
                playerCat.column(),
                potentialVsOverall.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput(String file) {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(file);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(20), A)
                .when(rank.$less(50), B)
                .otherwise(C);

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction2(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(3), A)
                .when(rank.$less(5), B)
                .when(rank.$less(10), C)
                .otherwise(D);

        df = df.withColumn(playerCat.getName(), rule);

        return df;
    }

    private Dataset<Row> exampleWindowFunction3(Dataset<Row> df) {

        df = df.withColumn(potentialVsOverall.getName(), potential.column().divide(overall.column()));

        return df;
    }

    private Dataset<Row> exampleWindowFunction4(Dataset<Row> df) {

        df = df.filter(playerCat.column().equalTo(A)
                .or(playerCat.column().equalTo(B))
                .or(playerCat.column().equalTo(C).and(potentialVsOverall.column().geq(1.15)))
                .or(playerCat.column().equalTo(D).and(potentialVsOverall.column().geq(1.25))));

        return df;
    }

    private Dataset<Row> menores23(Dataset<Row> df) {

        df = df.filter(age.column().lt(23));

        return df;
    }


}
