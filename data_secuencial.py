#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Procesar DataSet en secuencial"""

from time import time
from collections import Counter

__tiempo_inicial__ = time()

__dataSet__ = open('u.user', 'r')
__lineas__ = __dataSet__.readlines()

def  intervalo_edad(edad):
    """Intervalos de edad"""
    if edad < 10:
        return '0-10'
    elif edad < 20:
        return '10-20'
    elif edad < 30:
        return '20-30'
    elif edad < 40:
        return '30-40'
    elif edad < 50:
        return '40-50'
    elif edad < 60:
        return '50-60'
    elif edad < 70:
        return '60-70'
    elif edad < 80:
        return '70-80'
    else:
        return '80+'

def conversor_edad(data):
    """pasar edad a un intervalo de edad"""
    datos_conv = []
    for linea in data:
        datos_conv.append(linea.split("|"))

    for usuario in datos_conv:
        usuario[1] = intervalo_edad(int(usuario[1]))
    return datos_conv

def usuarios_coinciden(intervalo, data):
    """Cantidad e usuarios que coinciden con el intervalo de edad"""
    coincidencias = []
    for usuario in data:
        if usuario[1] == intervalo:
            coincidencias.append(usuario)
    return coincidencias

def usuarios_profesion(data):
    """Clasificar usuarios por profesion"""
    profesiones = []
    for usuario in data:
        profesiones.append(usuario[3])
    print Counter(profesiones)
    return Counter(profesiones)

def extremos(data):
    """Obtener cantidad de edades marginales"""
    extremos_dic = {}
    extremos_dic['pasados_edad'] = 0
    extremos_dic['menor_edad'] = 0
    for usuario in data:
        if usuario[1] == "70-80":
            extremos_dic['pasados_edad'] += 1
        if usuario[1] == "0-10":
            extremos_dic['menor_edad'] += 1
    return extremos_dic

__cambiar_edades__ = conversor_edad(__lineas__)
__usuario_filtrados__ = usuarios_coinciden("20-30", __cambiar_edades__)
__edade_extremos__ = extremos(__cambiar_edades__)

print "El total de usuarios es: ", len(__lineas__)
print "Total de usuarios por profesión: ", usuarios_profesion(__usuario_filtrados__)
print "Usuarios entre 0-10: ", __edade_extremos__['menor_edad']
print "Usuarios entre 70-80: ", __edade_extremos__['pasados_edad']

__tiempo_final__ = time()
__tiempo_ejecucion__ = __tiempo_final__ - __tiempo_inicial__

print 'El tiempo de ejecucion fue:', __tiempo_ejecucion__ #En segundos
