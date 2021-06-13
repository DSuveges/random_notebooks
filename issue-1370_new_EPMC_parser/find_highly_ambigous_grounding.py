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

    # # Process data:
    # (
    #     spark.read.parquet(input_file)
    #     .filter(
    #         (col('type') == "GP-DS") & # Filtering for gene/disease cooccurrence
    #         (col('isMapped') == True) & # Filtering for mapping
    #         (col('pmid') != "")  # Filtering out missing pmids
    #     )
    #     .groupby([col('pmid'), col('label1')])
    #     .agg(
    #         collect_set(col('keywordId1')).alias('targetFromSourceId'),
    #     )
    #     .withColumn('targetCount', size(col('targetFromSourceId')))
    #     .filter(col('targetCount') > threshold)
    #     .orderBy(col('targetCount').desc())
    #     .coalesce(1).write.format('json').mode('overwrite').option('compression', 'gzip')
    #     .save(f'{output_file}_target.json.gz')
    # )

    # # Process data:
    # (
    #     spark.read.parquet(input_file)
    #     .filter(
    #         (col('type') == "GP-DS") & # Filtering for gene/disease cooccurrence
    #         (col('isMapped') == True) & # Filtering for mapping
    #         (col('pmid') != "")  # Filtering out missing pmids
    #     )
    #     .groupby([col('pmid'), col('label2')])
    #     .agg(
    #         collect_set(col('keywordId2')).alias('diseaseFromSourceMappedIds'),
    #     )
    #     .withColumn('diseaseCount', size(col('diseaseFromSourceMappedIds')))
    #     .filter(col('diseaseCount') > 3)
    #     .coalesce(1)
    #     .orderBy(col('diseaseCount').desc())
    #     .write.format('json').mode('overwrite').option('compression', 'gzip')
    #     .save(f'{output_file}_disease.json.gz')
    # )

    # (
    #     spark.read.parquet(input_file)
    #     .filter(
    #         (col('type') == "GP-DS") & # Filtering for gene/disease cooccurrence
    #         (col('isMapped') == True) & # Filtering for mapping
    #         (col('pmid') != "")  # Filtering out missing pmids
    #     )
    #     .groupby(col('label1'))
    #     .agg(
    #         collect_set(col('keywordId1')).alias('targetFromSourceId'),
    #     )
    #     .withColumn('targetCount', size(col('targetFromSourceId')))
    #     .filter(col('targetCount') > threshold)
    #     .write.format('json').mode('overwrite').option('compression', 'gzip')
    #     .save(f'{output_file}_target.json.gz')
    # )

    (
        spark.read.parquet(input_file)
        .filter(
            (col('type') == "GP-DS") & # Filtering for gene/disease cooccurrence
            (col('isMapped') == True) & # Filtering for mapping
            (col('pmid') != "")  & # Filtering out missing pmids
            (
                (lower(col('label1')) == 'tec') |
                (lower(col('label1')) == ')'  ) |
                (lower(col('label1')) == 'S-' )
            )
        )
        .write.format('parquet').mode('overwrite')
        .save(f'{output_file}_target.parquet')
    )

if __name__ == '__main__':
    main()