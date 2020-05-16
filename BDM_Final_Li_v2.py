from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.functions import col, when
import csv

def writeToCSV(row):
    return ', '.join(str(item) for item in row)

def main(sc):
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    years = ['2015', '2016', '2017', '2018', '2019']
    def parseCSV(idx, part):
        if idx==0:
            next(part)
        for p in csv.reader(part):
            if p[23].isalpha() or p[24] == '' or p[21] == '' or p[23] == '' or p[4][-4:] not in years:
                continue
            if '-' in p[23]:
                yield(p[23].split('-')[0], p[23].split('-')[1], p[24].lower(), p[21], p[4][-4:])
            else:
                yield(p[23], '', p[24].lower(), p[21], p[4][-4:])

    rows = sc.textFile('/data/share/bdm/nyc_parking_violation/*.csv', use_unicode=True).mapPartitionsWithIndex(parseCSV)

    df = sqlContext.createDataFrame(rows, ('House Number', 'HN Compound', 'Street Name', 'County', 'Date'))

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

    def parseCL(idx, part):
        if idx==0:
            next(part)
        for p in csv.reader(part):
            LL_HN = p[2]
            LL_HNC = ''
            LH_HN = p[3]
            LH_HNC = ''
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
            yield(p[0], p[28].lower(), p[29].lower(), p[13], LL_HN, LL_HNC, LH_HN, LH_HNC, RL_HN, RL_HNC, RH_HN, RH_HNC)

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
    cond1 = (df['Street Name'] == centerline['full street'])
    cond2 = (df['Street Name'] == centerline['st label'])
    cond3 = (df['County'] == centerline['borocode'])
    cond4 = (df['House Number'] % 2 == 1)
    cond5 = (df['House Number'] >= centerline['LL_HN']) & (df['House Number'] <= centerline['LH_HN'])
    cond6 = (df['House Number'] % 2 == 0)
    cond7 = (df['House Number'] >= centerline['RL_HN']) & (df['House Number'] <= centerline['RH_HN'])
    cond8 = cond4 & cond5
    cond9 = cond6 & cond7

    hnc_cond1 = (df['HN Compound'].isNotNull())
    hnc_cond2 = (df['HN Compound'].isNull())
    hnc_cond3 = ((df['HN Compound'] >= centerline['LL_HNC']) & (df['HN Compound'] <= centerline['LH_HNC']))
    hnc_cond4 = ((df['HN Compound'] >= centerline['RL_HNC']) & (df['HN Compound'] <= centerline['RH_HNC']))


    cond10 = (hnc_cond2 & (cond8|cond9))
    cond11 = (hnc_cond1 & (cond8|cond9) & (hnc_cond3|hnc_cond4))

    joined = df.join(centerline, ((cond1|cond2) & cond3 & (cond10|cond11)), "inner")
    joined = joined.select(col('ID'), col('Date'))
    count_df = joined.groupBy(['ID', 'Date']).pivot('Date').count().drop('Date')
    print('Table pivoted')
    allID = centerline.select(col('ID')).dropDuplicates()
    result = allID.join(count_df, on=["ID"], how='outer').na.fill(0)

    marksColumns = [col('2015'), col('2016'), col('2017'), col('2018'), col('2019')]
    diff_x = [-2, -1, 0, 1, 2]

    average_func = sum(x for x in marksColumns)/len(marksColumns)
    result = result.withColumn("avg", average_func)
    ols_func = sum(diff*(y - col('avg')) for diff, y in zip(diff_x, marksColumns))/10
    coef = result.withColumn("OLS_COEF", ols_func).drop('avg')

    coef.rdd.map(writeToCSV).saveAsTextFile(sys.argv[1])

if __name__=="__main__":
    sc = SparkContext()
    main(sc)
