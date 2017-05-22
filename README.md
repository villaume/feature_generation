# feature_generation
Prototying Feature Generation Tools for Spark.
(`lazy` work in progress to make a lame scala joke)

## Includes
• VIF Calculation to find and flag co-linear features in a Spark DataFrame
• Top-X One-Hot Encoding of String Array Features

### Environment
Uses Spark 2.1.1 
Scala 2.11


### To-Dos
• Finish integration test (see about getting smaller docker images)
• Add random forest feature selection
• Add forward/backward inclusion
• Add Information Value / workspace_id
• Add DF evalutation (fill-rates etc)
