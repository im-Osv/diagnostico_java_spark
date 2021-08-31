package minsait.ttaa.datio.engine;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;
import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        //df = exampleWindowFunction(df);
        df = generateAgeRange(df);
        df = methodRankNatPos(df);
        df = avrPotentialOverall(df);
        df = columnSelection(df);

        filterDF(df, 0);
        filterDF(df, 1);
        filterDF(df, 2);
        filterDF(df, 3);
        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);


        df.printSchema();

        // Uncomment when you want write your final output
        write(df, OUTPUT_PATH);
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
                ageRange.column(),
                rankByNP.column(),
                potentialVsOverall.column()
                //catHeightByPosition.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull()
                .and(
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

        Column rule = when(rank.$less(20), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information
     * @return add to the Dataset the column "age_range"
     * Point 2
     */
    private Dataset<Row> generateAgeRange(Dataset<Row> df) {

        Column range = df.col(age.getName());

        Column rule = when(range.$less(23), "A")
                .when(range.between(23, 27), "B")
                .when(range.between(27, 32), "C")
                .when(range.$greater(32), "D");
        df = df.withColumn(ageRange.getName(), rule);
        return df;
    }

    /**
     * @param df is a Dataset with players information
     * @return add to the Dataset the column "rank_by_nationality_position"
     * Point 3
     */
    private  Dataset<Row> methodRankNatPos(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column());

        Column rule = row_number().over(w);

        df = df.withColumn(rankByNP.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information
     * @return add to the Dataset the column "potential_vs_overall"
     * Point 4
     */
    private Dataset<Row> avrPotentialOverall(Dataset<Row> df) {
        Column pot = df.col(potential.getName());
        Column ovrall = df.col(overall.getName());

        Column rule = pot.$div(ovrall);

        df = df.withColumn(potentialVsOverall.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information
     * @return add to the Dataset the column "age_range"
     * Point 5
     */
    private void filterDF(Dataset<Row> df, int option) {
        Column ageR = df.col(ageRange.getName());
        Column rankB = df.col(rankByNP.getName());
        Column pVsO = df.col(potentialVsOverall.getName());

        switch (option){
            case 0:
                df = df.filter(rankB.$less(3));
                df.show(100, false);
                write(df, FILTER1_PATH);
                break;
            case 1:
                df = df.filter(ageR.equalTo("B").or(ageR.equalTo("C")).and(pVsO.$greater(1.15)));
                df.show(100, false);
                write(df, FILTER2_PATH);
                break;
            case 2:
                df = df.filter(ageR.equalTo("A").and(pVsO.$greater(1.25)));
                df.show(100, false);
                write(df, FILTER3_PATH);
                break;
            case 3:
                df = df.filter(ageR.equalTo("D").and(rankB.$less(5)));
                df.show(100, false);
                write(df, FILTER4_PATH);
                break;
        }

        /*df = df.filter(rankB.$less(3));
        df = df.filter(ageR.equalTo("B").or(ageR.equalTo("C")).and(pVsO.$greater(1.15)));
        df = df.filter(ageR.equalTo("A").and(pVsO.$greater(1.25)));
        df = df.filter(ageR.equalTo("D").and(rankB.$less(5)));
        */
    }



}
