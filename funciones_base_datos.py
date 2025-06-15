import pandas as pd
import numpy as np
from IPython.display import display
import matplotlib.pyplot as plt
import seaborn as sns
import funciones_estadistica as estadistica
import pymysql
from sqlalchemy import create_engine, text
import ast
from word2number import w2n
from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.impute import KNNImputer
import funciones_limpieza as limp
import funciones_visualizacion as visual
import funciones_nulos as limp_nulo
import time

#CREACIÓN BASE DE DATOS

# Conectar a MySQL usando pymysql

def conexion(df, anfitrion, usuario, contraseña, basedatos, archivo):
    connection = pymysql.connect(
        host= anfitrion,
        user= usuario,
        password= contraseña
    )
    # Crear un cursor
    cursor = connection.cursor()
    # Crear una base de datos
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {basedatos}")
    print("Base de Datos creada exitosamente.")
    time.sleep(3)
    #transformamos el df a csv
    df.to_csv(archivo, index=False)
    # Cargar el archivo CSV
    df = pd.read_csv(archivo)
    engine = create_engine(f'mysql+pymysql://{usuario}:{contraseña}@{anfitrion}/{basedatos}')
    return engine, connection



