from pyspark import SparkContext
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

def parse_violation(idx, part):
    years = ['2015', '2016', '2017', '2018', '2019']
    if idx==0:
        next(part)
    for p in csv.reader(part):
        if p[23].isalpha() or p[24] == '' or p[21] == '' or p[23] == '' or p[4][-4:] not in years:
            continue
        if '-' in p[23]:
            yield(p[23].split('-')[0], p[23].split('-')[1], p[24].lower(), to_borocode(p[21]), p[4][-4:], p[0])
        else:
            yield(p[23], '', p[24].lower(), to_borocode(p[21]), p[4][-4:], p[0])


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
        yield(p[0], p[28].lower(), p[10].lower(), int(p[13]), LL_HN, LL_HNC, LH_HN, LH_HNC, RL_HN, RL_HNC, RH_HN, RH_HNC)

def filter_left(x):
    return (x[1][1].isdecimal() and x[1][3].isdecimal())

def filter_left_HN(x):
    HN = int(x[0][1])
    L = int(x[1][1])
    H = int(x[1][3])
    return (HN>=L and HN<=H)

def filter_right(x):
    return (x[1][5].isdecimal() and x[1][7].isdecimal())

def filter_right_HN(x):
    HN = int(x[0][1])
    L = int(x[1][5])
    H = int(x[1][7])
    return (HN>=L and HN<=H)

def filter_c(x):
    if x[1][0].isdecimal():
        if x[1][1].isdecimal() and x[1][2].isdecimal():
            C = int(x[1][0])
            L = int(x[1][1])
            H = int(x[1][2])
            return (C>=L and C<=H)
        return False
    return True

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
    #violations = sc.textFile('Parking_Violations_Issued_2020_simplified.csv').mapPartitionsWithIndex(parse_violation)
    violations = sc.textFile('/data/share/bdm/nyc_parking_violation/*.csv', use_unicode=True).mapPartitionsWithIndex(parse_violation)
    violations = violations.map(lambda x: (x[3], (x[0], x[1], x[2], x[4], x[5])))

    #centerline = sc.textFile('nyc_cscl.csv').mapPartitionsWithIndex(parseCL)
    centerline = sc.textFile('/data/share/bdm/nyc_cscl.csv', use_unicode=True).mapPartitionsWithIndex(parseCL)
    centerline = centerline.map(lambda x: (x[3], (x[0], x[1], x[2], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11])))

    joined = centerline.join(violations).filter(lambda x: (x[1][1][0]!='' and (x[1][0][1] == x[1][1][2] or x[1][0][2] == x[1][1][2])))
    joined = joined.map(lambda x: ((x[1][0][0], x[1][1][0]),
                               (x[1][1][1], x[1][0][3], x[1][0][4], x[1][0][5],
                                x[1][0][6], x[1][0][7], x[1][0][8], x[1][0][9],
                                x[1][0][10], x[1][1][3], x[1][1][4])))

    left = joined.filter(lambda x: x[0][1].isdecimal() and int(x[0][1])%2==1)
    right = joined.filter(lambda x: x[0][1].isdecimal() and int(x[0][1])%2==0)

    filtered_l = left.filter(filter_left).filter(filter_left_HN)
    filtered_l = filtered_l.map(lambda x: ((x[0][0], x[1][10], x[1][9]), (x[1][0], x[1][2], x[1][4])))

    filtered_r = right.filter(filter_right).filter(filter_right_HN)
    filtered_r = filtered_r.map(lambda x: ((x[0][0], x[1][10], x[1][9]), (x[1][0], x[1][6], x[1][8])))

    data_l = filtered_l.filter(filter_c).map(lambda x: ((x[0][0], x[0][1]), x[0][2])).distinct().map(lambda x: ((x[0][0]),(x[1])))
    data_r = filtered_r.filter(filter_c).map(lambda x: ((x[0][0], x[0][1]), x[0][2])).distinct().map(lambda x: ((x[0][0]),(x[1])))
    data = data_l.union(data_r)

    count = data.map(map_year)
    allID = centerline.map(lambda x: (x[1][0], (0, 0, 0, 0, 0))).union(count).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4]))

    diff_x = [-2, -1, 0, 1, 2]

    result = allID.map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], round(sm.OLS([x[1][0], x[1][1], x[1][2], x[1][3], x[1][4]], diff_x).fit().params[0], 2)))
    #result.take(5)
    result.map(writeToCSV).saveAsTextFile(sys.argv[1])

if __name__=="__main__":
    sc = SparkContext()
    main(sc)
