#IMPORTACIÓN DE LIBRERÍAS

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
import funciones_base_datos as base_datos
import time

pd.set_option('display.max_columns', None)

#LECTURA CSV
print("Leyendo csv...")
df = pd.read_csv("hr_raw_data.csv")

#EDA

print("Visualizando nulos...")
    #Visualización Nulos
#visual.exploracion_basica(df)
#nulos = df.isnull().sum()/df.shape[0]*100
#nulos.sort_values(ascending=False)

print("Eliminamos de columnas no útiles...")
    #Eliminación de columnas no útiles
df.drop(columns=["monthlyincome", "employeecount", "hourlyrate", "monthlyrate", "dailyrate", "datebirth", "numberchildren","yearsincurrentrole", "sameasmonthlyincome","over18","Unnamed: 0"], inplace=True)

print("Visualizamos la distribución de las categorías...")
    #Visualizacion de distribución de las categorías
visual.distribucion(df, 'number')
visual.distribucion(df, 'object')

print("Analizamos valores atípicos...")
    #Análisis de valores atípicos:
visual.atipicos(df)
    
    #Análisis boxplot:
visual.boxplot(df)


#LIMPIEZA

print("Limpiamos los datos...")

visual.extract_data(df)

df.drop_duplicates(['employeenumber'], inplace = True)

limp.letras_a_numeros(df, 'age')

divisas = ['salary']
for divi in divisas:
    limp.limpieza_divisas (df, divi, float)

numeros_limp = ['totalworkingyears', 'worklifebalance']
for num in numeros_limp:
    limp.limpieza_numeros(df, num, float)

limp.cambio_tipo(df, 'remotework', object)
limp.limpieza_boleanos (df, 'remotework', object)

limp.cambio_tipo(df, 'education', int)

limp.minusculas_titulos(df)

limp.minusculas(df)

limp.quitar_espacios(df)

limp.limpieza_genero (df, 'gender', str)

limp.limpieza_maritalstatus (df, 'maritalstatus')

cambio_a_objeto = ['relationshipsatisfaction', 'jobinvolvement', 'joblevel', 'jobsatisfaction', 'stockoptionlevel', 'worklifebalance', 'education', 'jobrole', 'environmentsatisfaction']
for col in cambio_a_objeto:
    limp.cambio_tipo(df, col, object)

enteros = ['worklifebalance']
for num_ent in enteros:
    limp.cambiar_a_entero(df, num_ent)

limp.primer_digito(df, 'environmentsatisfaction')


#GESTIÓN DE NULOS
print("Gestionamos los nulos...")

lista_moda = ['overtime', 'maritalstatus', 'standardhours', 'worklifebalance', 'businesstravel', 'performancerating']
for col in lista_moda:
    limp.objeto_moda(df, col)

limp_nulo.negativos_a_nulos (df, 'distancefromhome')

imp_negativos = ['distancefromhome', 'salary', 'totalworkingyears']
for col in imp_negativos:
    limp_nulo.impu_KNNImputer (df, col)
    print(df[imp_negativos].dtypes)
    print(f"Imputación terminada para {col}")

    #visualizacion nulos
print("Visualizamos los nulos una vez hecha la limpieza:")
limp_nulo.ver_nulos(df)

print("Visualizamos todos los datos una vez hecha la limpieza:")
visual.extract_data(df)

#VISUALIZACIÓN

print("Visualizamos los gráficos elegidos")

estadistica.matriz_correlacion (df)
estadistica.grafico_pastel(df, 'attrition', 'Dimisiones')
estadistica.grafico_comparacion(df, 'yearsatcompany', 'attrition', "Años en la empresa", "Nº Empleados")

estadistica.clasificar_veterania(df, 'yearsatcompany', 'veterania')

time.sleep(3)

estadistica.comparacion_con_porcentajes(df, 'veterania', 'attrition', "Veteranía", "Total Empleados", "Distribución de empleados por veteranía y dimisiones")

estadistica.grafico_pastel(df, 'gender', 'Género')  
estadistica.grafico_comparacion(df, 'gender', 'attrition', "Género", "Total Empleados")


#CREACIÓN BASE DE DATOS


engine, connection = base_datos.conexion(df, 'localhost', 'root', 'AlumnaAdalab', 'bd_talento_ABC_Company', 'bd_talento_abc_company.csv')
cursor = connection.cursor()

print("Creamos las tablas en SQL")

time.sleep(3)

    # Borrar datos previos para asegurar que no hay problemas al ejecutar de nuevo
with engine.begin() as conn:
    tablas = [
        "employee_info", "employee_career", "employee_rating", "employee_training",
        "employee_satisfaction", "employee_salary", "employee_conditions", "employee_company",
        "attrition"
    ]
    for tabla in tablas:
        conn.execute(text(f"DELETE FROM {tabla}"))
    print("Datos borrados de las tablas.")

    #Creación de tablas SQL
tablas_sql = {
    "employee_info": """
        CREATE TABLE IF NOT EXISTS employee_info (
            id_employee INT PRIMARY KEY,
            age INT,
            gender VARCHAR(100),
            maritalstatus VARCHAR(100),
            education VARCHAR(100),
            distancefromhome FLOAT
        )
    """,
    "employee_career": """
        CREATE TABLE IF NOT EXISTS employee_career (
            id_employee INT PRIMARY KEY,
            numcompaniesworked  INT,
            totalworkingyears FLOAT
        )
    """,
    "employee_rating": """
        CREATE TABLE IF NOT EXISTS employee_rating (
            id_employee INT PRIMARY KEY,
            jobinvolvement  VARCHAR(100),
            performancerating VARCHAR(100)
        )
    """,

       "employee_training": """
        CREATE TABLE IF NOT EXISTS employee_training (
            id_employee INT PRIMARY KEY,
            trainingtimeslastyear INT
        )
    """,

       "employee_satisfaction": """
        CREATE TABLE IF NOT EXISTS employee_satisfaction (
            id_employee INT PRIMARY KEY,
            environmentsatisfaction VARCHAR(100),
            jobsatisfaction VARCHAR(100),
            relationshipsatisfaction VARCHAR(100),
            worklifebalance VARCHAR(100)
        )
    """,
        "employee_salary": """
        CREATE TABLE IF NOT EXISTS employee_salary (
            id_employee INT PRIMARY KEY,
            salary FLOAT,
            percentsalaryhike INT
        )
    """,
        "employee_conditions": """
        CREATE TABLE IF NOT EXISTS employee_conditions (
            id_employee INT PRIMARY KEY,
            businesstravel VARCHAR(100),
            stockoptionlevel INT,
            standardhours VARCHAR(100),
            overtime VARCHAR(100), 
            remotework VARCHAR(100)
        )
    """,

        "employee_company": """
        CREATE TABLE IF NOT EXISTS employee_company (
            id_employee INT PRIMARY KEY,
            department VARCHAR(500),
            roledepartament VARCHAR(500),
            joblevel INT,
            jobrole VARCHAR(500), 
            yearsatcompany INT, 
            yearssincelastpromotion INT,
            yearswithcurrmanager INT
        )
    """,

        "attrition": """
        CREATE TABLE IF NOT EXISTS attrition (
            id_employee INT PRIMARY KEY,
            attrition VARCHAR(100)
        )
    """,

    
}

for tabla, sql in tablas_sql.items():
    with engine.begin() as conn:
        conn.execute(text(sql))
        print(f"Tabla {tabla} creada exitosamente.")




#IMPORTACIÓN DATOS EN BASE DE DATOS

print("Rellenamos las tablas en SQL")
# Tabla de employee_info:
df_employee_info = df[['employeenumber', 'age', 'gender', 'maritalstatus', 'education', 'distancefromhome']]
df_employee_info.columns = ['id_employee', 'age', 'gender', 'maritalstatus', 'education', 'distancefromhome']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_employee_info.empty:
    df_employee_info.to_sql('employee_info', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_employee_info)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'artists'")



# Tabla de employee_career:
df_employee_career = df[['employeenumber', 'numcompaniesworked', 'totalworkingyears']]
df_employee_career.columns = ['id_employee', 'numcompaniesworked', 'totalworkingyears']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_employee_career.empty:
    df_employee_career.to_sql('employee_career', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_employee_career)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'df_employee_career'")


# Tabla de employee_rating:
df_employee_rating = df[['employeenumber', 'jobinvolvement', 'performancerating']]
df_employee_rating.columns = ['id_employee', 'jobinvolvement', 'performancerating']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_employee_rating.empty:
    df_employee_rating.to_sql('employee_rating', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_employee_rating)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'employee_rating'")



# Tabla de employee_training:
df_employee_training = df[['employeenumber', 'trainingtimeslastyear']]
df_employee_training.columns = ['id_employee', 'trainingtimeslastyear']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_employee_training.empty:
    df_employee_training.to_sql('employee_training', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_employee_training)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'employee_training'")


# Tabla de employee_satisfaction:
df_employee_satisfaction = df[['employeenumber', 'environmentsatisfaction', 'jobsatisfaction', 'relationshipsatisfaction', 'worklifebalance']]
df_employee_satisfaction.columns = ['id_employee', 'environmentsatisfaction', 'jobsatisfaction', 'relationshipsatisfaction', 'worklifebalance']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_employee_satisfaction.empty:
    df_employee_satisfaction.to_sql('employee_satisfaction', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_employee_satisfaction)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'employee_satisfaction'")


# Tabla de employee_salary:
df_employee_salary = df[['employeenumber', 'salary', 'percentsalaryhike']]
df_employee_salary.columns = ['id_employee', 'salary', 'percentsalaryhike']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_employee_salary.empty:
    df_employee_salary.to_sql('employee_salary', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_employee_salary)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'employee_salary'")


# Tabla de employee_conditions:
df_employee_conditions = df[['employeenumber', 'businesstravel', 'stockoptionlevel', 'standardhours', 'overtime', 'remotework']]
df_employee_conditions.columns = ['id_employee', 'businesstravel', 'stockoptionlevel', 'standardhours', 'overtime', 'remotework']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_employee_conditions.empty:
    df_employee_conditions.to_sql('employee_conditions', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_employee_conditions)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'employee_conditions'")


# Tabla de employee_company:
df_employee_company = df[['employeenumber', 'department', 'roledepartament', 'joblevel', 'jobrole', 'yearsatcompany', 'yearssincelastpromotion', 'yearswithcurrmanager']]
df_employee_company.columns = ['id_employee', 'department', 'roledepartament', 'joblevel', 'jobrole', 'yearsatcompany', 'yearssincelastpromotion', 'yearswithcurrmanager']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_employee_company.empty:
    df_employee_company.to_sql('employee_company', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_employee_company)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'employee_company'")



# Tabla de attrition:
df_attrition = df[['employeenumber', 'attrition']]
df_attrition.columns = ['id_employee', 'attrition']

id_employee_existentes = pd.read_sql("SELECT id_employee FROM employee_info", con=engine)

df_employee_info = df_employee_info[~df_employee_info['id_employee'].isin(id_employee_existentes['id_employee'])]

if not df_attrition.empty:
    df_attrition.to_sql('attrition', con=engine, if_exists='append', index=False)
    print(f"Insertados {len(df_attrition)} empleados nuevos.")
else:
    print("No hay empleados nuevos por insertar.")

print("Datos de empleados insertados en la tabla 'attrition'")


connection.commit()
cursor.close()
connection.close()

print("FIN")