import argparse

from pyspark.sql import SparkSession


def calculate_vote_count(data_source, output_uri):
    """
    Processes sample food establishment inspection data and queries the data to find the top 10 establishments
    with the most Red violations from 2006 to 2020.

    :param data_source:'s3://datasetdemosaplabs/food_establishment_data.csv'.
    :param output_uri: 's3://resultssaplabs/output'.
    """
    with SparkSession.builder.appName("Calculate genre Count").getOrCreate() as spark:
        # Load the restaurant violation CSV data
        if data_source is not None:
            movies_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        movies_df.createOrReplaceTempView("movie_desc")

        # Create a DataFrame of the top 10 restaurants with the most Red violations
        genre_count = spark.sql("""SELECT geners, count(*) AS genre_count
          FROM movie_desc 
          WHERE original_language = 'en' 
          GROUP BY genres
          ORDER BY genre_count DESC LIMIT 10""")

        # Write the results to the specified output URI
        genre_count.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="s3://moviebuck01/moviesdatasetoriginal.csv")
    parser.add_argument(
        '--output_uri', help="s3:/moviebuck03/result")
    args = parser.parse_args()

    calculate_genre_count(args.data_source, args.output_uri)
   
			