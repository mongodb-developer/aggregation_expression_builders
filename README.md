# aggregation_expression_builders
Source code to accompany blog post on using aggregation expression builder in the MongoDB Java Drivers

The code in this repository was built in using OpenJDK 64Bit by Jetbrains, version 17.0.4.1+7 aarch64 
on a Mac Powerbook M1 Pro. The code uses Gradle and should reference The MongoDB synchronous Java 
driver, version 4.9 or later, as a dependency.

The example aggregation pipleline used in the article is available in BSON document format in  file 
"aggregation.json". You can use the contents of this file to create an equivalent Pipeline in MongoDB 
Compass.

The data used in the example is availabe in 'aircraftData.json.zip'. After unzipping this file, you
can import the data to your own MongoDB instance using 
