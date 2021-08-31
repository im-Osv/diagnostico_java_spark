package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.teamPosition;
import static org.apache.spark.sql.SaveMode.Overwrite;

abstract class Writer {

    static void write(Dataset<Row> df, String cad) {
        df
                .coalesce(1)
                .write()
                .partitionBy(teamPosition.getName())
                .mode(Overwrite)
                .parquet(cad);
    }

}
