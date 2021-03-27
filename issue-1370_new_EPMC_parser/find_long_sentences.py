import argparse
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def main():

    parser = argparse.ArgumentParser(description='This script finds long sentences in the ePMC datase.')
    parser.add_argument('--cooccurrenceFile', help='Partioned parquet file with the ePMC cooccurrences', type=str, required=True)
    parser.add_argument('--outputFile', help='Resulting file saved as compressed JSON.', type=str, required=True)
    parser.add_argument('--threshold', help='Lower threshold of character count of a sentence considered as long.', type=str, default=600, required=True)
    args = parser.parse_args()

    input_file = args.cooccurrenceFile
    output_file = args.outputFile
    threshold = args.threshold

    # Initialize spark:
    global spark

    sparkConf = (SparkConf()
         .set("spark.driver.maxResultSize", "0")
         .set("spark.debug.maxToStringFields", "2000")
         .set("spark.sql.execution.arrow.maxRecordsPerBatch", "500000")
         )
    spark = (
        SparkSession.builder
            .config(conf=sparkConf)
            .getOrCreate()
    )

    # Process data:
    (
        spark.read.parquet(input_file)
        .filter(
            (col('type') == "GP-DS") & # Filtering for gene/disease cooccurrence
            (col('isMapped') == True) & # Filtering for mapping
            (col('pmid') != "") &  # Filtering out missing pmids
            (col('label1') != "(") & # Filtering out this strange entity.
            (length(col('text')) > threshold )) # Sentence threshold is 500 
        .groupby([col('text'), col('pmid')])
        .agg(
            first(col("section")).alias('section'),
            length(col('text')).alias('textLenght'),
            collect_set(struct(
                col('keywordId1').alias('targetFromSourceId'),
                col("keywordId2").alias('diseaseFromSourceMappedId'),
            )).alias('associations'),
            collect_set(struct(
                col('label1').alias('targetLabel'),
                col("label2").alias('diseaseLabel'),
            )).alias('cooccurrences')
        )
        .withColumn('associationCount', size(col('associations')))
        .withColumn('cooccurrenceCount', size(col('cooccurrences')))

        .coalesce(1).write.format('json').mode('overwrite').option('compression', 'gzip')
        .save(output_file)
    )


if __name__ == '__main__':
    main()