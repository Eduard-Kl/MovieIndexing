from pyspark.sql import SparkSession
import glob
from pyspark.sql.functions import col, collect_set


if __name__ == '__main__':

    files = glob.glob('data/movies_*.csv')
    if len(files) == 0:
        print('No input data found in data/ directory. Exiting')
        exit()

    spark = SparkSession.builder.appName('Movie indexing').getOrCreate()
    data = spark.read.format('csv') \
        .load(files, header='true', delimiter=',') \
        .select('cast', 'crew', col('id').alias('movie_id'))

    crew = spark.read.json(data.rdd.map(lambda x: x[0])).select('name', 'id')
    cast = spark.read.json(data.rdd.map(lambda x: x[1])).select('name', 'id')

    crew = crew \
        .filter(crew.name.isNotNull()) \
        .dropDuplicates(['name'])
    cast = cast \
        .filter(cast.name.isNotNull()) \
        .dropDuplicates(['name'])

    people = cast.union(crew)

    # Napraviti nešto korisno s people DataFrameom. Spremiti u bazu...
    print('Primjer osoba:id')
    people.show(10)

    joined = data \
        .join(people, data.cast.contains(people.name) | data.crew.contains(people.name), how='left') \
        .drop('cast', 'crew')

    starredIn = joined \
        .filter(col('name').isNotNull()) \
        .groupby(col('id')) \
        .agg(collect_set(col('movie_id')).alias('appeared_in')) \
        .drop('name')
    # Spremiti starredIn DataFrame
    print('Primjer osoba id:lista filmova')
    starredIn.show(10)
