import pandas as pd
import numpy as np
from IPython.display import display
import matplotlib.pyplot as plt
import seaborn as sns
import funciones_estadistica as estadistica
import pymysql
from sqlalchemy import create_engine, text
import ast

import time
import mysql.connector

#CREACIÓN BASE DE DATOS

# Conectar a MySQL usando pymysql

def conexion(df, anfitrion, usuario, contraseña, basedatos, archivo):
    #Conexión sin BD para crear la base de datos
    root_cnx = mysql.connector.connect(
        host=anfitrion,
        user=usuario,
        password=contraseña
    )
    root_cur = root_cnx.cursor()
    root_cur.execute(f"CREATE DATABASE IF NOT EXISTS {basedatos}")
    root_cur.close()
    root_cnx.close()

    #Conexión YA apuntando a la BD
    cnx = mysql.connector.connect(
        host=anfitrion,
        user=usuario,
        password=contraseña,
        database=basedatos    
    )

    # Engine de SQLAlchemy (también con la BD)
    engine = create_engine(
        f"mysql+pymysql://{usuario}:{contraseña}@{anfitrion}/{basedatos}"
    )

    df.to_csv(archivo, index=False)
    return engine, cnx



