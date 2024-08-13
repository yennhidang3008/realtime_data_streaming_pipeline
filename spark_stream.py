import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_address (
        id UUID PRIMARY KEY,
        uid TEXT,
        city TEXT,
        street_name TEXT,
        street_address TEXT,
        secondary_address TEXT,
        building_number TEXT,
        mail_box TEXT,
        community TEXT,
        zip_code TEXT,
        zip TEXT,
        postcode TEXT,
        time_zone TEXT,
        street_suffix TEXT,
        city_suffix TEXT,
        city_prefix TEXT,
        state TEXT,
        state_abbr TEXT,
        country TEXT,
        country_code TEXT,
        latitude TEXT,
        longitude TEXT,
        full_address TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")
    
    iD = kwargs.get('iD')
    uid = kwargs.get('uid')
    city = kwargs.get('city')
    street_name = kwargs.get('street_name')
    street_address = kwargs.get('street_address')
    secondary_address = kwargs.get('secondary_address')
    building_number = kwargs.get('building_number')
    mail_box = kwargs.get('mail_box')
    community = kwargs.get('community')
    zip_code = kwargs.get('zip_code')
    ziP = kwargs.get('ziP')
    postcode = kwargs.get('postcode')
    time_zone = kwargs.get('time_zone')
    street_suffix = kwargs.get('street_suffix')
    city_suffix = kwargs.get('city_suffix')
    city_prefix = kwargs.get('city_prefix')
    state = kwargs.get('state')
    state_abbr = kwargs.get('state_abbr')
    country = kwargs.get('country')
    country_code = kwargs.get('country_code')
    latitude = kwargs.get('latitude')
    longitude = kwargs.get('longitude')
    full_address = kwargs.get('full_address')


    try:
        session.execute("""
            INSERT INTO spark_streams.created_address(iD, uid, city, street_name, street_address, secondary_address, building_number, 
            mail_box, community, zip_code, zip, postcode, time_zone, street_suffix, city_suffix, city_prefix, state, state_abbr, country, country_code, latitude, longitude, full_address)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (iD, uid, city, street_name, street_address, secondary_address, building_number, 
            mail_box, community, zip_code, zip, postcode, time_zone, street_suffix, city_suffix, city_prefix, state, state_abbr, country, country_code, latitude, longitude, full_address))
        logging.info(f"Data inserted for id {iD}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([         
        StructField("iD", StringType(), False),
        StructField("uid", StringType(), False),
        StructField("city", StringType(), False),
        StructField("street_name", StringType(), False),
        StructField("street_address", StringType(), False),
        StructField("secondary_address", StringType(), False),
        StructField("building_number", StringType(), False),
        StructField("mail_box", StringType(), False),
        StructField("community", StringType(), False),
        StructField("zip_code", StringType(), False),
        StructField("ziP", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("time_zone", StringType(), False),
        StructField("street_suffix", StringType(), False),
        StructField("city_suffix", StringType(), False),
        StructField("city_prefix", StringType(), False),
        StructField("state", StringType(), False),
        StructField("state_abbr", StringType(), False),
        StructField("country", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("latitude", StringType(), False),
        StructField("longitude", StringType(), False),
        StructField("full_address", StringType(), False)
        ])


    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_address')
                               .start())

            streaming_query.awaitTermination()