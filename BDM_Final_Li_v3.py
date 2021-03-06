from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.functions import col, when
import csv
from pyspark.sql.types import *
import sys


def writeToCSV(row):
    return ', '.join(str(item) for item in row)


def parseCSV(idx, part):
    years = ['2015', '2016', '2017', '2018', '2019']
    if idx==0:
        next(part)
    for p in csv.reader(part):
        if p[23].isalpha() or p[24] == '' or p[21] == '' or p[23] == '' or p[4][-4:] not in years:
            continue
        if '-' in p[23]:
            yield(p[23].split('-')[0], p[23].split('-')[1], p[24].lower(), p[21], p[4][-4:], p[0])
        else:
            yield(p[23], '', p[24].lower(), p[21], p[4][-4:], p[0])


def parseCL(idx, part):
    if idx==0:
        next(part)
    for p in csv.reader(part):
        LL_HN = p[2]
        LL_HNC = None
        LH_HN = p[3]
        LH_HNC = None
        if '-' in p[2] and '-' in p[3]:
            LL_HN = p[2].split('-')[0]
            LL_HNC = p[2].split('-')[1]
            LH_HN = p[3].split('-')[0]
            LH_HNC = p[3].split('-')[1]

        RL_HN = p[4]
        RL_HNC = ''
        RH_HN = p[5]
        RH_HNC = ''
        if '-' in p[4] and '-' in p[5]:
            RL_HN = p[4].split('-')[0]
            RL_HNC = p[4].split('-')[1]
            RH_HN = p[5].split('-')[0]
            RH_HNC = p[5].split('-')[1]
        yield(p[0], p[28].lower(), p[10].lower(), p[13], LL_HN, LL_HNC, LH_HN, LH_HNC, RL_HN, RL_HNC, RH_HN, RH_HNC)


def filter_udf(row):
    match = False
    HN = row[0]
    LL_HN = row[10]
    LH_HN = row[12]
    RL_HN = row[14]
    RH_HN = row[16]

    HNC = row[1]
    LL_HNC = row[11]
    LH_HNC = row[13]
    RL_HNC = row[15]
    RH_HNC = row[17]

    if HN != None:
        if HN%2==1:
            if LL_HN != None and LH_HN != None:
                if HN >= LL_HN and HN <= LH_HN:
                    if HNC != None:
                        if LL_HNC != None and LH_HNC != None:
                            if HNC >= LL_HNC and HN <= LH_HNC:
                                match = True
                    else:
                        match = True
        else:
            if RL_HN != None and RH_HN != None:
                if HN >= RL_HN and HN <= RH_HN:
                    if HNC != None:
                        if RL_HNC != None and RH_HNC != None:
                            if HNC >= RL_HNC and HN <= RH_HNC:
                                match = True
                    else:
                        match = True
    if match:
        return row
    return

def map_year(row):
    if row[1] == '2015':
        return (row[0], (1, 0, 0, 0, 0))
    elif row[1] == '2016':
        return (row[0], (0, 1, 0, 0, 0))
    elif row[1] == '2017':
        return (row[0], (0, 0, 1, 0, 0))
    elif row[1] == '2018':
        return (row[0], (0, 0, 0, 1, 0))
    elif row[1] == '2019':
        return (row[0], (0, 0, 0, 0, 1))
    return

def main(sc):
    spark = SparkSession(sc)
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    sqlContext = SQLContext(sc)
######################## Read Parking Violation Records #######################

    rows = sc.textFile('/data/share/bdm/nyc_parking_violation/*.csv', use_unicode=True).mapPartitionsWithIndex(parseCSV)
    #rows = sc.textFile('Parking_Violations_Issued_201[5-9]_simplified.csv').mapPartitionsWithIndex(parseCSV)

    df = sqlContext.createDataFrame(rows, ('House Number', 'HN Compound', 'Street Name', 'County', 'Date', 'SN'))

    map_NY = (col("County")=='NY')|\
            (col("County")=='MAN')|\
            (col("County")=='MH')|\
            (col("County")=='MN')|\
            (col("County")=='NEWY')|\
            (col("County")=='NEW Y')

    map_BX = (col("County")=='BRONX')|\
            (col("County")=='BX')

    map_BK = (col("County")=='BK')|\
            (col("County")=='K')|\
            (col("County")=='KING')|\
            (col("County")=='KINGS')

    map_QN = (col("County")=='Q')|\
            (col("County")=='QN')|\
            (col("County")=='QNS')|\
            (col("County")=='QU')|\
            (col("County")=='QUEEN')

    map_R = (col("County")=='R')|\
            (col("County")=='RICHMOND')

    df = df.withColumn("County", when(map_NY, '1')
                                .when(map_BX, '2')
                                .when(map_BK, '3')
                                .when(map_QN, '4')
                                .when(map_R, '5')
                                .otherwise('')).where(col('County')!='')
    df = df.withColumn("House Number", df["House Number"].cast('int'))
    df = df.withColumn("HN Compound", df["HN Compound"].cast('int'))

######################## Read NYC Street Data ########################

    #rows = sc.textFile('nyc_cscl.csv').mapPartitionsWithIndex(parseCL)

    rows = sc.textFile('/data/share/bdm/nyc_cscl.csv', use_unicode=True).mapPartitionsWithIndex(parseCL)
    centerline = sqlContext.createDataFrame(rows, ('ID', 'full street', 'st label', 'borocode', 'LL_HN', 'LL_HNC', 'LH_HN', 'LH_HNC', 'RL_HN', 'RL_HNC', 'RH_HN', 'RH_HNC'))
    centerline = centerline.withColumn("LL_HN", centerline["LL_HN"].cast('int'))
    centerline = centerline.withColumn("LH_HN", centerline["LH_HN"].cast('int'))
    centerline = centerline.withColumn("RL_HN", centerline["RL_HN"].cast('int'))
    centerline = centerline.withColumn("RH_HN", centerline["RH_HN"].cast('int'))
    centerline = centerline.withColumn("LL_HNC", centerline["LL_HNC"].cast('int'))
    centerline = centerline.withColumn("LH_HNC", centerline["LH_HNC"].cast('int'))
    centerline = centerline.withColumn("RL_HNC", centerline["RL_HNC"].cast('int'))
    centerline = centerline.withColumn("RH_HNC", centerline["RH_HNC"].cast('int'))
    print('Data loaded')

######################## Join Two Datasets ########################
    cond1 = (df['Street Name'] == centerline['full street'])
    cond2 = (df['Street Name'] == centerline['st label'])
    cond3 = (df['County'] == centerline['borocode'])
    joined = df.join(centerline, (cond3 & (cond1|cond2)), how="inner")

    '''def match(HN, HNC, LL_HN, LH_HN, RL_HN, RH_HN, LL_HNC, LH_HNC, RL_HNC, RH_HNC):
        ncond1 = HN != None
        ncond2 = HNC != None
        ncond3 = LL_HN != None
        ncond4 = LH_HN != None
        ncond5 = RL_HN != None
        ncond6 = RH_HN != None
        ncond7 = LL_HNC != None
        ncond8 = LH_HNC != None
        ncond9 = RL_HNC != None
        ncond10 = RH_HNC != None

        cond4 = (ncond1 and (HN % 2 == 1))
        cond5 = (ncond1 and ncond3 and ncond4 and (HN >= LL_HN) and (HN <= LH_HN))
        cond6 = (ncond1 and (HN % 2 == 0))
        cond7 = (ncond1 and ncond5 and ncond6 and (HN >= RL_HN) and (HN <= RH_HN))
        cond8 = cond4 and cond5
        cond9 = cond6 and cond7

        hnc_cond1 = (HNC != None)
        hnc_cond2 = (HNC == None)
        hnc_cond3 = (ncond2 and ncond7 and ncond8 and (HNC >= LL_HNC) and (HNC <= LH_HNC))
        hnc_cond4 = (ncond2 and ncond9 and ncond10 and (HNC >= RL_HNC) and (HNC <= RH_HNC))


        cond10 = (hnc_cond2 and (cond8|cond9))
        cond11 = (hnc_cond1 and (cond8|cond9) and (hnc_cond3|hnc_cond4))
        return (cond10|cond11)

    filter_udf = udf(match, returnType=BooleanType())

    filtered = joined.filter(filter_udf("House Number", "HN Compound", "LL_HN", "LH_HN", "RL_HN", "RH_HN", "LL_HNC", "LH_HNC", "RL_HNC", "RH_HNC"))
'''
######################## User Defined FIltering Function ########################



    filtered = joined.rdd.filter(filter_udf)
    filtered = filtered.map(lambda x: (x[5], x[6], x[4])).toDF(["SN","ID","Date"])

    filtered = filtered.dropDuplicates(['ID', 'SN']).drop('SN')

    count_rdd = filtered.rdd.map(map_year).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4]))

    count_rdd = count_rdd.map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4]))
    count_df = count_rdd.toDF(['PHYSICALID', 'COUNT_2015', 'COUNT_2016', 'COUNT_2017', 'COUNT_2018', 'COUNT_2019'])


    allID = centerline.select(col('ID').alias('PHYSICALID')).dropDuplicates()
    result = allID.join(count_df, on=['PHYSICALID'], how='outer').na.fill(0)

    marksColumns = [col('COUNT_2015'), col('COUNT_2016'), col('COUNT_2017'), col('COUNT_2018'), col('COUNT_2019')]
    diff_x = [-2, -1, 0, 1, 2]

    average_func = sum(x for x in marksColumns)/len(marksColumns)
    result = result.withColumn("avg", average_func)
    ols_func = sum(diff*(y - col('avg')) for diff, y in zip(diff_x, marksColumns))/10
    coef = result.withColumn("OLS_COEF", ols_func).drop('avg')
    #coef.show()
    coef.rdd.map(writeToCSV).saveAsTextFile(sys.argv[1])

if __name__=="__main__":
    sc = SparkContext()
    main(sc)
