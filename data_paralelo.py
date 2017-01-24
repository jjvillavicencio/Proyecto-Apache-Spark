#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Procesar DataSet en paralelo"""

from time import time
from pyspark import SparkContext, SparkConf

__tiempo_inicial__ = time()
conf = SparkConf().setAppName('ProcesamientoData')
sc = SparkContext(conf=conf)

userRDD = sc.textFile("u.user")

def parse_N_calculate_age(data):
    userid, age, gender, occupation, zip = data.split("|")
    return  userid, age_group(int(age)), gender, occupation, zip, int(age)

def  age_group(age):
    if age < 10:
        return '0-10'
    elif age < 20:
        return '10-20'
    elif age < 30:
        return '20-30'
    elif age < 40:
        return '30-40'
    elif age < 50:
        return '40-50'
    elif age < 60:
        return '50-60'
    elif age < 70:
        return '60-70'
    elif age < 80:
        return '70-80'
    else:
        return '80+'

data_with_age_bucket = userRDD.map(parse_N_calculate_age)

RDD_20_30 = data_with_age_bucket.filter(lambda line: '20-30' in line)

freq = RDD_20_30.map(lambda line: line[3]).countByValue()

print "El total de usuarios es: ", userRDD.count()

print "Total de usuarios por profesión ", dict(freq)

Under_age = sc.accumulator(0)
Over_age = sc.accumulator(0)

def outliers(data):
    global Over_age, Under_age
    age_grp = data[1]
    if age_grp == "70-80":
        Over_age += 1
    if age_grp == "0-10":
        Under_age += 1
    return data

df = data_with_age_bucket.map(outliers).collect()

print "Usuarios entre 0-10: ", Under_age
print "Usuarios entre 70-80: ", Over_age

__tiempo_final__ = time()
__tiempo_ejecucion__ = __tiempo_final__ - __tiempo_inicial__

print 'El tiempo de ejecucion fue:', __tiempo_ejecucion__ #En segundos
