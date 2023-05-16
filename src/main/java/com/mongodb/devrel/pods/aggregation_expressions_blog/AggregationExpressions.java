package com.mongodb.devrel.pods.aggregation_expressions_blog;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.mql.MqlDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.mql.MqlValues.current;
import static com.mongodb.client.model.mql.MqlValues.of;


public class AggregationExpressions {

    public static void main(String[] args) {

        // Replace the placeholder with your MongoDB deployment's connection string,
        // database name and collection name
        String uri = "mongodb://localhost:27017";
        String dbName = "DIA_Aircraft_Optimized";
        String collName = "aircraftData";

        try (MongoClient mongoClient = MongoClients.create(uri)) {

            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collName);

            //Create aggregation by building a BSON Document

            //Create the from and to dates for the match stage
            String sFromDate = "2023-01-31T12:00:00.000-07:00";
            TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(sFromDate);
            Instant fromInstant = Instant.from(ta);
            Date fromDate = Date.from(fromInstant);

            String sToDate = "2023-02-01T00:00:00.000-07:00";
            ta = DateTimeFormatter.ISO_INSTANT.parse(sToDate);
            Instant toInstant = Instant.from(ta);
            Date toDate = Date.from(toInstant);

            Document matchStage = new Document("$match",
                    new Document("positionReports",
                            new Document("$elemMatch",
                                    new Document("callsign", Pattern.compile("^UAL"))
                                            .append("timestamp", new Document("$gte", fromDate))
                                            .append("timestamp", new Document("$lt", toDate))
                            )
                    )
            );

            Document groupStage = new Document("$group",
                    new Document("_id", "$model")
                    .append("aircraftSet",
                            new Document("$addToSet", "$tailNum")
                    )
            );

            Document projectStage = new Document("$project",
                    new Document("airline", "United")
                            .append("model", "$_id")
                            .append("count",
                                    new Document("$size", "$aircraftSet"))
                            .append("manufacturer",
                                    new Document("$cond",
                                            new Document("if",
                                                    new Document("$eq",
                                                            Arrays.asList(
                                                                    new Document("$substrBytes", Arrays.asList("$_id", 0, 1)),
                                                                    "B"
                                                            )
                                                    )
                                            )
                                                    .append("then", "BOEING")
                                                    .append("else", "AIRBUS")
                                    )
                            )
                            .append("_id", "$$REMOVE")
            );

            //Run the pipeline
            collection.aggregate(
                    Arrays.asList(
                            matchStage,
                            groupStage,
                            projectStage
                    )
            ).forEach(doc -> System.out.println(doc.toJson()));



            //Now do the same thing using aggregation and expression builders

            var positionReports = current().<MqlDocument>getArray("positionReports");
            Bson bMatchStage = match(expr(
                    positionReports.any(positionReport -> {
                        var callsign = positionReport.getString("callsign");
                        var ts = positionReport.getDate("timestamp");
                        return callsign
                                .substrBytes(0,3)
                                .eq(of("UAL"))
                                .and(ts.gte(of(fromInstant)))
                                .and(ts.lt(of(toInstant)));
                    })
            ));

            Bson bGroupStage = group("$model",
                    addToSet("aircraftSet", current().getString("tailNum")));

            Bson bProjectStage = project(fields(
                    computed("airline", "United"),
                    computed("model", current().getString("_id")),
                    computed("count", current().<MqlDocument>getArray("aircraftSet").size()),
                    computed("manufacturer", current()
                            .getString("_id")
                            .substr(0, 1).eq(of("B"))
                            .cond(of("BOEING"), of("AIRBUS"))),
                    excludeId()
            ));


            collection.aggregate(
                Arrays.asList(
                        bMatchStage,
                        bGroupStage,
                        bProjectStage
                )
            ).forEach(doc -> System.out.println(doc.toJson()));
        }
    }
}