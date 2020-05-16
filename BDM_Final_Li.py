from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.functions import col, when
import csv
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler


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

    cond1 = (df['Street Name'] == centerline['full street'])
    cond2 = (df['Street Name'] == centerline['st label'])
    cond3 = (df['County'] == centerline['borocode'])

    joined = df.join(centerline, (cond1|cond2) & cond3, "inner")
    no_compound = joined.filter(joined['HN Compound'].isNull())
    compound = joined.filter(joined['HN Compound'].isNotNull())
    left = no_compound.filter(no_compound['House Number'] % 2 == 1)
    right = no_compound.filter(no_compound['House Number'] % 2 == 0)

    left_withc = compound.filter(compound['House Number'] % 2 == 1)
    right_withc = compound.filter(compound['House Number'] % 2 == 0)

    cond4 = (left['House Number'] >= left['LL_HN']) & (left['House Number'] <= left['LH_HN'])
    left = left.filter(cond4)
    left = left.select(col('ID'), col('Date'))

    cond5 = (right['House Number'] >= right['RL_HN']) & (right['House Number'] <= right['RH_HN'])
    right = right.filter(cond5)
    right = right.select(col('ID'), col('Date'))

    cond6 = (left_withc['HN Compound'] >= left_withc['LL_HNC']) & (left_withc['HN Compound'] <= left_withc['LH_HNC'])
    cond7 = (left_withc['House Number'] >= left_withc['LL_HN']) & (left_withc['House Number'] <= left_withc['LH_HN'])

    cond8 = (right_withc['HN Compound'] >= right_withc['RL_HNC']) & (right_withc['HN Compound'] <= right_withc['RH_HNC'])
    cond9 = (right_withc['House Number'] >= right_withc['RL_HN']) & (right_withc['House Number'] <= right_withc['RH_HN'])

    left_withc = left_withc.filter(cond6 & cond7)
    left_withc = left_withc.select(col('ID'), col('Date'))

    right_withc = right_withc.filter(cond8 & cond9)
    right_withc = right_withc.select(col('ID'), col('Date'))

    count_df = left.union(left_withc)
    count_df = count_df.union(right)
    count_df = count_df.union(right_withc)

    count_df = count_df.groupBy(['ID', 'Date']).pivot('Date').count().drop('Date')
    count_df = count_df.na.fill(0)

    marksColumns = [col('2015'), col('2016'), col('2017'), col('2018'), col('2019')]
    diff_x = [-2, -1, 0, 1, 2]

    average_func = sum(x for x in marksColumns)/len(marksColumns)

    coef = count_df.withColumn("avg", average_func)

    ols_func = sum(diff*(y - col('avg')) for diff, y in zip(diff_x, marksColumns))/10
    coef = coef.withColumn("OLS_COEF", ols_func).drop('avg')

    allID = centerline.select(col('ID'))
    result = allID.join(coef, on=["ID"], how='outer')
    result = result.dropDuplicates()
    result = result.na.fill(0)

    result.rdd.map(writeToCSV).saveAsTextFile(sys.argv[1])

if __name__=="__main__":
    sc = SparkContext()
    main(sc)
