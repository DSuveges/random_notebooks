from datetime import datetime
import argparse
import os
import sys
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *

@udf(StringType())
def split_last(s):
    ''' Splits a string and returns the last value
    '''
    return s.split('/')[-1]

def main():
    evidence_file = 'gs://open-targets-data-releases/20.06/output/20.06_evidence_data.json.gz' # 20k sample out of 10M
    out_file = 'gs://open-targets-eu-dev-dataproc-test/20.06_literature_evidences_only/20.06_evidence_data.filtered.parquet' # Full dataset

    # Make spark session
    global spark
    spark = (pyspark.sql.SparkSession.builder.getOrCreate())
    print('Spark version: ', spark.version)

    # Load and select required cols
    data = (
        spark.read.json(evidence_file)
        .filter(col('type') == 'literature')
        .select(
            col('type').alias('evidence_type'),
            col('unique_association_fields.datasource').alias('data_source'),
            col('disease.efo_info.efo_id').alias('efo_code'),
            col('target.gene_info.geneid').alias('gene_id'),
            col('scores.association_score').alias('assoc_score'),
            col('unique_association_fields.publication_id').alias('pmid'),
        )
        # Clean efo_code
        .withColumn('efo_code', split_last(col('efo_code')))
        .withColumn('pmid', split_last(col('pmid')))
    )

    # Save data:
    (
        data
        .repartitionByRange('evidence_type', 'data_source', 'efo_code', 'gene_id')
        .write
        .parquet(
            out_file
        )
    )




if __name__ == '__main__':

    main()