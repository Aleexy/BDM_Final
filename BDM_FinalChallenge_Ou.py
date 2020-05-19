## Imports
from pyspark import SparkContext
import csv
import sys
import statsmodels.api as sm

def toCSV(data):
    return ','.join(str(d) for d in data)

## Main functionality
def main(sc):
    outputpath = sys.argv[1]

    MANHATTAN  = ['MAN','MH','MN','NEWY','NEW Y', 'NY']
    BRONX = ['BRONX', 'BX']
    BROOKLYN = ['BK','K','KING,KINGS']
    QUEENS = ['Q', 'QN', 'QNS', 'QU', 'QUEEN']
    STATEN_ISLAND = ['R', 'RICHMOND']
    Violation = ['MAN','MH','MN','NEWY','NEW Y', 'NY', 'BRONX', 'BX', 'BK','K','KING,KINGS','Q', 'QN', 'QNS', 'QU', 'QUEEN', 'R', 'RICHMOND']

    def extractStreet(partId, records):
        if partId==0:
            next(records)
        reader = csv.reader(records)
        for row in reader:
            (PHYSICALID, FULL_STREE, ST_LABEL, BOROCODE, 
            L_LOW_HN, L_HIGH_HN, R_LOW_HN, R_HIGH_HN) = (row[0], row[28], row[10], int(row[13]), row[2], row[3], row[4],row[5])
   
            if PHYSICALID != '' and BOROCODE != '':
                yield (PHYSICALID, FULL_STREE, ST_LABEL, BOROCODE, 
                    L_LOW_HN.split('-')[0] if '-' in L_LOW_HN else L_LOW_HN,
                    L_LOW_HN.split('-')[1] if '-' in L_LOW_HN else '',
                    L_HIGH_HN.split('-')[0] if '-' in L_HIGH_HN else L_HIGH_HN,
                    L_HIGH_HN.split('-')[1] if '-' in L_HIGH_HN else '',
                    R_LOW_HN.split('-')[0] if '-' in R_LOW_HN else R_LOW_HN,
                    R_LOW_HN.split('-')[1] if '-' in R_LOW_HN else '',
                    R_HIGH_HN.split('-')[0] if '-' in R_HIGH_HN else R_HIGH_HN,
                    R_HIGH_HN.split('-')[1] if '-' in R_HIGH_HN else '')

    def extractParking_2015(partId, records):
        if partId==0:
            next(records)
        reader = csv.reader(records)
        for row in reader:   
            (House_Number, Street_Name, Violation_County, Issue_Date) = (row[23], row[24], row[21], row[4][-4:])
            if '2015' in Issue_Date and Violation_County in Violation and House_Number != '' and Street_Name != '' :
                yield  (House_Number.split('-')[0] if '-' in House_Number else House_Number,
                        House_Number.split('-')[1] if '-' in House_Number else '',
                        Street_Name, 1 if  Violation_County in MANHATTAN else
                        (2 if Violation_County in BRONX else
                        (3 if Violation_County in BROOKLYN else
                        (4 if Violation_County in QUEENS else 
                        5 ))))

    def extractParking_2016(partId, records):
        if partId==0:
            next(records)
        reader = csv.reader(records)
        for row in reader:
            (House_Number, Street_Name, Violation_County, Issue_Date) = (row[23], row[24], row[21], row[4][-4:])
            if '2016' in Issue_Date and Violation_County in Violation and House_Number != '' and Street_Name != '' :
                yield  (House_Number.split('-')[0] if '-' in House_Number else House_Number,
                        House_Number.split('-')[1] if '-' in House_Number else '',
                        Street_Name, 
                        1 if  Violation_County in MANHATTAN else
                        (2 if Violation_County in BRONX else
                        (3 if Violation_County in BROOKLYN else
                        (4 if Violation_County in QUEENS else 
                        5 ))))

    def extractParking_2017(partId, records):
        if partId==0:
            next(records)
        reader = csv.reader(records)
        for row in reader:  
            (House_Number, Street_Name, Violation_County, Issue_Date) = (row[23], row[24], row[21], row[4][-4:])
            if '2017' in Issue_Date and Violation_County in Violation and House_Number != '' and Street_Name != '' :
                yield  (House_Number.split('-')[0] if '-' in House_Number else House_Number,
                        House_Number.split('-')[1] if '-' in House_Number else '',
                        Street_Name, 
                        1 if  Violation_County in MANHATTAN else
                        (2 if Violation_County in BRONX else
                        (3 if Violation_County in BROOKLYN else
                        (4 if Violation_County in QUEENS else 
                         5 ))))

    def extractParking_2018(partId, records):
        if partId==0:
            next(records)
        reader = csv.reader(records)
        for row in reader:   
            (House_Number, Street_Name, Violation_County, Issue_Date) = (row[23], row[24], row[21], row[4][-4:])
            if '2018' in Issue_Date and Violation_County in Violation and House_Number != '' and Street_Name != '' :
                yield  (House_Number.split('-')[0] if '-' in House_Number else House_Number,
                        House_Number.split('-')[1] if '-' in House_Number else '',
                        Street_Name, 
                        1 if  Violation_County in MANHATTAN else
                        (2 if Violation_County in BRONX else
                        (3 if Violation_County in BROOKLYN else
                        (4 if Violation_County in QUEENS else 
                            5 ))))
    
    def extractParking_2019(partId, records):
        if partId==0:
            next(records)
        reader = csv.reader(records)
        for row in reader:
            (House_Number, Street_Name, Violation_County, Issue_Date) = (row[23], row[24], row[21], row[4][-4:])
            if '2019' in Issue_Date and Violation_County in Violation and House_Number != '' and Street_Name != '' :
                yield  (House_Number.split('-')[0] if '-' in House_Number else House_Number,
                        House_Number.split('-')[1] if '-' in House_Number else '',
                        Street_Name, 
                        1 if  Violation_County in MANHATTAN else
                        (2 if Violation_County in BRONX else
                        (3 if Violation_County in BROOKLYN else
                        (4 if Violation_County in QUEENS else 
                        5 ))))
    

    nyc = sc.textFile('/data/share/bdm/nyc_cscl.csv', use_unicode=True)
    nyc_df = nyc.mapPartitionsWithIndex(extractStreet).map(lambda x: ((x[1] if (x[1] == x[2]) else (x[1] if (x[1] != '') else x[2])
            , x[3]), (x[0], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11])))

    data_2015 = sc.textFile('/data/share/bdm/nyc_parking_violation/2015.csv', use_unicode=True)
    df_2015 = data_2015.mapPartitionsWithIndex(extractParking_2015).map(lambda x: ((x[2], x[3]), (x[0],x[1])))

    nyc_2015 = df_2015.join(nyc_df).map(lambda x: (x[1][1][0], x[1][0][0], x[1][0][1], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4],
                                 x[1][1][5], x[1][1][6], x[1][1][7], x[1][1][8]))

    nyc_2015_withExtend_odd = nyc_2015.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and  (x[2].isdecimal() == True)  and (x[3] != '') and (x[4] != '') 
                                    and (x[3].isdecimal() == True) and (x[4].isdecimal() == True) and  (x[5] != '') and (x[5].isdecimal() == True) and (x[6] != '') and (x[6].isdecimal() == True)).map(
                                    lambda x: (x[0], int(x[1]), int(x[2]), int(x[3]),
                                    int(x[4]), int(x[5]), int(x[6]))).filter(lambda x: (x[2] %2 ==1) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2015_withExtend_even = nyc_2015.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and (x[2].isdecimal() == True) 
                                    and (x[7] != '') and (x[7].isdecimal() == True) and (x[8] != '') and (x[8].isdecimal() == True) and (x[9] != '') 
                                    and (x[9].isdecimal() == True) and (x[10].isdecimal() == True) and (x[10] != '')).map(lambda x: (x[0], int(x[1]), int(x[2]), int(x[7]),
                                    int(x[8]), int(x[9]), int(x[10]))).filter(lambda x: (x[2] %2 ==0) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2015_outExtend_odd = nyc_2015.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[3] != '') and (x[3].isdecimal() == True)
                                         and (x[5] != '') and (x[5].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[3]), int(x[5]))).filter(lambda x: 
                                         (x[1] % 2 == 1) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])

    nyc_2015_outExtend_even = nyc_2015.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[7] != '') 
                                        and (x[7].isdecimal() == True) and (x[9] != '') and (x[9].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[7]), int(x[9]))).filter(lambda x: 
                                         (x[1] % 2 == 0) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])

    total_2015 = nyc_2015_withExtend_odd.union(nyc_2015_withExtend_even).union(nyc_2015_outExtend_odd).union(
            nyc_2015_outExtend_even).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).map(
            lambda x: (x[0], (x[1], 0, 0, 0, 0)))

    data_2016 = sc.textFile('/data/share/bdm/nyc_parking_violation/2016.csv', use_unicode=True)
    df_2016 = data_2016.mapPartitionsWithIndex(extractParking_2016).map(lambda x: ((x[2], x[3]), (x[0],x[1])))

    nyc_2016 = df_2016.join(nyc_df).map(lambda x: (x[1][1][0], x[1][0][0], x[1][0][1], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4],
                                 x[1][1][5], x[1][1][6], x[1][1][7], x[1][1][8]))

    nyc_2016_withExtend_odd = nyc_2016.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and  (x[2].isdecimal() == True)  and (x[3] != '') and (x[4] != '') 
                                    and (x[3].isdecimal() == True) and (x[4].isdecimal() == True) and  (x[5] != '') and (x[5].isdecimal() == True) and (x[6] != '') and (x[6].isdecimal() == True)).map(
                                    lambda x: (x[0], int(x[1]), int(x[2]), int(x[3]),
                                    int(x[4]), int(x[5]), int(x[6]))).filter(lambda x: (x[2] %2 ==1) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2016_withExtend_even = nyc_2016.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and (x[2].isdecimal() == True) 
                                    and (x[7] != '') and (x[7].isdecimal() == True) and (x[8] != '') and (x[8].isdecimal() == True) and (x[9] != '') 
                                    and (x[9].isdecimal() == True) and (x[10].isdecimal() == True) and (x[10] != '')).map(lambda x: (x[0], int(x[1]), int(x[2]), int(x[7]),
                                    int(x[8]), int(x[9]), int(x[10]))).filter(lambda x: (x[2] %2 ==0) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2016_outExtend_odd = nyc_2016.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[3] != '') and (x[3].isdecimal() == True)
                                         and (x[5] != '') and (x[5].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[3]), int(x[5]))).filter(lambda x: 
                                         (x[1] % 2 == 1) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])

    nyc_2016_outExtend_even = nyc_2016.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[7] != '') 
                                        and (x[7].isdecimal() == True) and (x[9] != '') and (x[9].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[7]), int(x[9]))).filter(lambda x: 
                                         (x[1] % 2 == 0) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])

    total_2016 = nyc_2016_withExtend_odd.union(nyc_2016_withExtend_even).union(nyc_2016_outExtend_odd).union(
            nyc_2016_outExtend_even).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).map(
            lambda x: (x[0], (0, x[1], 0, 0, 0)))

    data_2017 = sc.textFile('/data/share/bdm/nyc_parking_violation/2017.csv', use_unicode=True)
    df_2017 = data_2017.mapPartitionsWithIndex(extractParking_2017).map(lambda x: ((x[2], x[3]), (x[0],x[1])))

    nyc_2017 = df_2017.join(nyc_df).map(lambda x: (x[1][1][0], x[1][0][0], x[1][0][1], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4],
                                 x[1][1][5], x[1][1][6], x[1][1][7], x[1][1][8]))

    nyc_2017_withExtend_odd = nyc_2017.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and  (x[2].isdecimal() == True)  and (x[3] != '') and (x[4] != '') 
                                    and (x[3].isdecimal() == True) and (x[4].isdecimal() == True) and  (x[5] != '') and (x[5].isdecimal() == True) and (x[6] != '') and (x[6].isdecimal() == True)).map(
                                    lambda x: (x[0], int(x[1]), int(x[2]), int(x[3]),
                                    int(x[4]), int(x[5]), int(x[6]))).filter(lambda x: (x[2] %2 ==1) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2017_withExtend_even = nyc_2017.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and (x[2].isdecimal() == True) 
                                    and (x[7] != '') and (x[7].isdecimal() == True) and (x[8] != '') and (x[8].isdecimal() == True) and (x[9] != '') 
                                    and (x[9].isdecimal() == True) and (x[10].isdecimal() == True) and (x[10] != '')).map(lambda x: (x[0], int(x[1]), int(x[2]), int(x[7]),
                                    int(x[8]), int(x[9]), int(x[10]))).filter(lambda x: (x[2] %2 ==0) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2017_outExtend_odd = nyc_2017.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[3] != '') and (x[3].isdecimal() == True)
                                         and (x[5] != '') and (x[5].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[3]), int(x[5]))).filter(lambda x: 
                                         (x[1] % 2 == 1) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])

    nyc_2017_outExtend_even = nyc_2017.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[7] != '') 
                                        and (x[7].isdecimal() == True) and (x[9] != '') and (x[9].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[7]), int(x[9]))).filter(lambda x: 
                                         (x[1] % 2 == 0) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])
    
    total_2017 = nyc_2017_withExtend_odd.union(nyc_2017_withExtend_even).union(nyc_2017_outExtend_odd).union(
            nyc_2017_outExtend_even).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).map(
            lambda x: (x[0], (0, 0, x[1], 0, 0)))

    data_2018 = sc.textFile('/data/share/bdm/nyc_parking_violation/2018.csv', use_unicode=True)
    df_2018 = data_2018.mapPartitionsWithIndex(extractParking_2018).map(lambda x: ((x[2], x[3]), (x[0],x[1])))

    nyc_2018 = df_2018.join(nyc_df).map(lambda x: (x[1][1][0], x[1][0][0], x[1][0][1], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4],
                                 x[1][1][5], x[1][1][6], x[1][1][7], x[1][1][8]))

    nyc_2018_withExtend_odd = nyc_2018.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and  (x[2].isdecimal() == True)  and (x[3] != '') and (x[4] != '') 
                                    and (x[3].isdecimal() == True) and (x[4].isdecimal() == True) and  (x[5] != '') and (x[5].isdecimal() == True) and (x[6] != '') and (x[6].isdecimal() == True)).map(
                                    lambda x: (x[0], int(x[1]), int(x[2]), int(x[3]),
                                    int(x[4]), int(x[5]), int(x[6]))).filter(lambda x: (x[2] %2 ==1) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2018_withExtend_even = nyc_2018.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and (x[2].isdecimal() == True) 
                                    and (x[7] != '') and (x[7].isdecimal() == True) and (x[8] != '') and (x[8].isdecimal() == True) and (x[9] != '') 
                                    and (x[9].isdecimal() == True) and (x[10].isdecimal() == True) and (x[10] != '')).map(lambda x: (x[0], int(x[1]), int(x[2]), int(x[7]),
                                    int(x[8]), int(x[9]), int(x[10]))).filter(lambda x: (x[2] %2 ==0) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2018_outExtend_odd = nyc_2018.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[3] != '') and (x[3].isdecimal() == True)
                                         and (x[5] != '') and (x[5].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[3]), int(x[5]))).filter(lambda x: 
                                         (x[1] % 2 == 1) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])

    nyc_2018_outExtend_even = nyc_2018.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[7] != '') 
                                        and (x[7].isdecimal() == True) and (x[9] != '') and (x[9].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[7]), int(x[9]))).filter(lambda x: 
                                         (x[1] % 2 == 0) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])
    
    total_2018 = nyc_2018_withExtend_odd.union(nyc_2018_withExtend_even).union(nyc_2018_outExtend_odd).union(
                nyc_2018_outExtend_even).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], (0, 0, 0, x[1], 0)))

    data_2019 = sc.textFile('/data/share/bdm/nyc_parking_violation/2019.csv', use_unicode=True)
    df_2019 = data_2019.mapPartitionsWithIndex(extractParking_2019).map(lambda x: ((x[2], x[3]), (x[0],x[1])))

    nyc_2019 = df_2019.join(nyc_df).map(lambda x: (x[1][1][0], x[1][0][0], x[1][0][1], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4],
                                 x[1][1][5], x[1][1][6], x[1][1][7], x[1][1][8]))

    nyc_2019_withExtend_odd = nyc_2019.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and  (x[2].isdecimal() == True)  and (x[3] != '') and (x[4] != '') 
                                    and (x[3].isdecimal() == True) and (x[4].isdecimal() == True) and  (x[5] != '') and (x[5].isdecimal() == True) and (x[6] != '') and (x[6].isdecimal() == True)).map(
                                    lambda x: (x[0], int(x[1]), int(x[2]), int(x[3]),
                                    int(x[4]), int(x[5]), int(x[6]))).filter(lambda x: (x[2] %2 ==1) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2019_withExtend_even = nyc_2019.filter(lambda x : (x[1] != '') and (x[1].isdecimal() == True) and (x[2] != '') and (x[2].isdecimal() == True) 
                                    and (x[7] != '') and (x[7].isdecimal() == True) and (x[8] != '') and (x[8].isdecimal() == True) and (x[9] != '') 
                                    and (x[9].isdecimal() == True) and (x[10].isdecimal() == True) and (x[10] != '')).map(lambda x: (x[0], int(x[1]), int(x[2]), int(x[7]),
                                    int(x[8]), int(x[9]), int(x[10]))).filter(lambda x: (x[2] %2 ==0) and 
                                    (x[3] <= x[1]) and (x[1] <= x[5]) and (x[4] <= x[2]) and (x[2] <= x[6])).map(
                                    lambda x: x[0])

    nyc_2019_outExtend_odd = nyc_2019.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[3] != '') and (x[3].isdecimal() == True)
                                         and (x[5] != '') and (x[5].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[3]), int(x[5]))).filter(lambda x: 
                                         (x[1] % 2 == 1) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])

    nyc_2019_outExtend_even = nyc_2019.filter(lambda x: (x[1] != '') and (x[1].isdecimal() == True) and (x[7] != '') 
                                        and (x[7].isdecimal() == True) and (x[9] != '') and (x[9].isdecimal() == True)).map(
                                        lambda x: (x[0], int(x[1]), int(x[7]), int(x[9]))).filter(lambda x: 
                                         (x[1] % 2 == 0) and (x[2] <= x[1]) and (x[1] <= x[3])).map(lambda x: x[0])

    
    total_2019 = nyc_2019_withExtend_odd.union(nyc_2019_withExtend_even).union(nyc_2019_outExtend_odd).union(nyc_2019_outExtend_even).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).map(
            lambda x: (x[0], (0, 0, 0, 0, x[1])))

    cscl = nyc_df.map(lambda x: (x[1][0], 0)).reduceByKey(lambda x,y : x + y).map(lambda x: (x[0], (0, 0, 0, 0, 0)))

    X = [-2, -1, 0, 1, 2]

    report = cscl.union(total_2015).union(total_2016).union(total_2017).union(total_2018).union(
        total_2019).reduceByKey(lambda x, y: (x[0] + y[0] , x[1]+y[1], x[2] + y[2], x[3]+y[3], x[4]+y[4])).map(
        lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], 
        round(sm.OLS([x[1][0], x[1][1], x[1][2], x[1][3], x[1][4]], X).fit().params[0]), 2)).map(
        lambda x: (x[0], (x[1], x[2], x[3], x[4], x[5], x[6]))).sortByKey().map(
        lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))

    lines = report.map(toCSV)\
        .saveAsTextFile(outputpath)
    
    return lines

if __name__ == "__main__":
    sc = SparkContext()
    # Execute Main functionality 
    main(sc)