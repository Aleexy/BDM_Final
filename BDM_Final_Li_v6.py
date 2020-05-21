from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.functions import col, when
import csv
from pyspark.sql.types import *
import sys
import statsmodels.api as sm



def writeToCSV(row):
    return ', '.join(str(item) for item in row)


NY  = ['MAN', 'MH', 'MN', 'NEWY', 'NEW Y', 'NY']
BX = ['BRONX', 'BX']
BK = ['BK', 'K', 'KING' 'KINGS']
QN = ['Q', 'QN', 'QNS', 'QU', 'QUEEN']
R = ['R', 'RICHMOND']
def to_borocode(boro):
    if boro in NY:
        return 1
    if boro in BX:
        return 2
    if boro in BK:
        return 3
    if boro in QN:
        return 4
    if boro in R:
        return 5
    return 6

def get_HN(s):
    try:
        HN = tuple(map(int, s.split('-')))
    except:
        return None
    return (HN[0], 0) if len(HN)==1 else HN

def parse_violation(idx, part):
# HN, SN, BC, YR, LEFT
    years = ['2015', '2016', '2017', '2018', '2019']
    if idx==0:
        next(part)
    reader= csv.reader(part)
    for p in reader:
        if p[23].isalpha() or p[24] == '' or p[21] == '' or p[23] == '' or p[4][-4:] not in years:
            continue
        HN = get_HN(p[23])
        if HN:
            yield(HN[0], HN[1], p[24].lower(), to_borocode(p[21]), p[4][-4:], HN[0]%2==1)
        continue

def parseCL_left(idx, part):
    if idx==0:
        next(part)
    for p in csv.reader(part):

        LL_HN = get_HN(p[2])
        LH_HN = get_HN(p[3])
        if LL_HN and LH_HN:
            yield(True, LL_HN[0], LH_HN[0], LL_HN[1], LH_HN[1], p[28].lower(), p[10].lower(), int(p[13]), p[0])

def parseCL_right(idx, part):
    if idx==0:
        next(part)
    for p in csv.reader(part):

        RL_HN = get_HN(p[4])
        RH_HN = get_HN(p[5])
        if RL_HN and RH_HN:
            yield(False, RL_HN[0], RH_HN[0], RL_HN[1], RH_HN[1], p[28].lower(), p[10].lower(), int(p[13]), p[0])

def map_year(row):
    if row[1] == '2015':
        return (row[0], (row[2], 0, 0, 0, 0))
    elif row[1] == '2016':
        return (row[0], (0, row[2], 0, 0, 0))
    elif row[1] == '2017':
        return (row[0], (0, 0, row[2], 0, 0))
    elif row[1] == '2018':
        return (row[0], (0, 0, 0, row[2], 0))
    elif row[1] == '2019':
        return (row[0], (0, 0, 0, 0, row[2]))
    return



def main(sc):
    sqlContext = SQLContext(sc)
    violations = sc.textFile('/data/share/bdm/nyc_parking_violation/*.csv', use_unicode=True).cache().mapPartitionsWithIndex(parse_violation)
    #violations = sc.textFile('Parking_Violations_Issued_2016_simplified.csv').mapPartitionsWithIndex(parse_violation)
    violations = sqlContext.createDataFrame(violations, ('HN', 'HNC', 'Street Name', 'County', 'Date', 'Left'))

    #cl_data = sc.textFile('nyc_cscl.csv')
    cl_data = sc.textFile('/data/share/bdm/nyc_cscl.csv', use_unicode=True)
    centerline_l = cl_data.mapPartitionsWithIndex(parseCL_left)
    centerline_l = sqlContext.createDataFrame(centerline_l, ('Left', 'L_HN', 'H_HN', 'L_HNC', 'H_HNC', 'full street', 'st label', 'borocode', 'ID'))
    centerline_r = cl_data.mapPartitionsWithIndex(parseCL_right)
    centerline_r = sqlContext.createDataFrame(centerline_r, ('Left', 'L_HN', 'H_HN', 'L_HNC', 'H_HNC', 'full street', 'st label', 'borocode', 'ID'))
    centerline = centerline_l.union(centerline_r)

    match = [violations['left'] == centerline['left'],
         violations['County'] == centerline['borocode'],
         violations['Street Name'] == centerline['full street'],
         (violations['HN'] >= centerline['L_HN'])&(violations['HN'] <= centerline['H_HN']),
         (violations['HNC'] >= centerline['L_HNC'])&(violations['HNC'] <= centerline['H_HNC'])]


    count = centerline.join(violations, match, "inner").groupby([centerline['ID'], violations['Date']]).count()

    result = count.rdd.map(map_year)

    allID = centerline_l.rdd.map(lambda x: (x[-1], (0, 0, 0, 0, 0))).union(result).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4]))

    diff_x = [-2, -1, 0, 1, 2]
    result = allID.map(lambda x: (x[0], (x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], round(sm.OLS([x[1][0], x[1][1], x[1][2], x[1][3], x[1][4]], diff_x).fit().params[0], 2)))).sortByKey()
    #result.take(5)
    return result.map(writeToCSV).saveAsTextFile(sys.argv[1])


if __name__=="__main__":
    sc = SparkContext()
    main(sc)
