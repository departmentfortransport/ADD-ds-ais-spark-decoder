import ais
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


schema = StructType([
    StructField("mmsi", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("messageType", IntegerType(), True),
    StructField("mType2", IntegerType(), True),
    StructField("mType3", IntegerType(), True),
    StructField("aisclassAorB", StringType(), True),
    StructField("aisMessage", StringType(), False),
    StructField("other", StringType(), True)])

batch =['gs://ds-ais/ANSData_RawData01Apr2016ML12713.dat',
        'gs://ds-ais/ANSData_RawData01Aug2016ML12835.dat',
        'gs://ds-ais/ANSData_RawData01Dec2016ML12957.dat',
        'gs://ds-ais/ANSData_RawData01Feb2016ML12653.dat',
        'gs://ds-ais/ANSData_RawData01Jan2016ML12622.dat',
        'gs://ds-ais/ANSData_RawData01Jul2016ML12804.dat',
        'gs://ds-ais/ANSData_RawData01Jun2016ML12774.dat',
        'gs://ds-ais/ANSData_RawData01Mar2016ML12682.dat',
        'gs://ds-ais/ANSData_RawData01May2016ML12743.dat',
        'gs://ds-ais/ANSData_RawData01Nov2016ML12927.dat',
        'gs://ds-ais/ANSData_RawData01Oct2016ML12896.dat',
        'gs://ds-ais/ANSData_RawData01Sep2016ML12866.dat']


for datafile in batch:
    fileday = datafile[27:29]
    filemonth = datafile[29:32]
    fileyear = datafile[32:36]

len(batch)
i=0

for datafile in batch:
# READ MULTIPLE CSV files
    df = spark.read.format("csv").schema(schema).load(datafile)
    i+=1
    # Function to decode the AIS messages
    def ais_to_dict(ais_string):
        try:
            ais_dict = ais.decode(ais_string, 0)
            return ais_dict
        except Exception:
            try:
                ais_dict = ais.decode(ais_string, 2)
                return ais_dict
            except:
                ais_dict ={}
                #n_fails += 1
                return ais_dict

    # Turn this into a spark user defined function, so that we can apply it to the message column
    decodeUDF = udf(ais_to_dict)
    df = df.withColumn("msgDecoded", decodeUDF(df['aisMessage']))

    # Extract decoded data from dictionary- specify variables we want (there are quite a lot of columns- see the tuples)
    variables_to_pull = ['cog','draught','destination','id','messageType','name','nav_status','other','sog','type_and_cargo','x','y']
    for var in variables_to_pull:
        extract_var = udf(lambda s:s.get(var,"NA"))
        df = df.withColumn(var,extract_var("msgDecoded"))

    extract_mmsi = udf(lambda s:s.get('mmsi',"NA"))
    df = df.withColumn('mmsi2',extract_mmsi("msgDecoded"))


    # Drop unrequired columns
    drop_list = ['aisMessage', 'msgDecoded', 'messageType', 'mType2', 'mType3']
    df =df.select([column for column in df.columns if column not in drop_list])

    print(i)
    fileday = datafile[27:29]
    filemonth = datafile[29:32]
    fileyear = datafile[32:36]
    filename = "gs://ds-ais-2/aisdecode" + str(fileday) + str(filemonth) + str(fileyear)+ ".csv"
    df.write.mode("overwrite").csv(filename)
    print('Complete')
