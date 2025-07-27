#IMPORTACIÓN DE LIBRERÍAS

# Tratamiento de datos
import pandas as pd 
import pymysql
import mysql.connector
import numpy as np

# Visualización
import matplotlib.pyplot as plt
import seaborn as sns
pd.set_option('display.max_columns', None) # para poder visualizar todas las columnas de los DataFrames

# Evaluar linealidad de las relaciones entre las variables
import scipy.stats as stats
from scipy.stats import shapiro, poisson, chisquare, expon, kstest

#Bases de datos
from sqlalchemy import create_engine, text
from sqlalchemy import inspect

# Gestión de los warnings
import warnings
warnings.filterwarnings("ignore")


import ast  # Para convertir texto en listas de Python

from word2number import w2n

from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.impute import KNNImputer

import funciones_limpieza as limp
import funciones_visualizacion as visual
import funciones_base_datos as bbdd
import funciones_estadistica as estadistica
import funciones_nulos as limp_nulo

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


columnas_nulos = ['roledepartament', 'department', 'educationfield']

for col in columnas_nulos:
    limp.objeto_categoria (df, col)


#visualizacion nulos

print("Visualizamos todos los datos una vez hecha la limpieza:")
visual.extract_data(df)


#CREACIÓN BASE DE DATOS


from dotenv import load_dotenv
import os

load_dotenv()
password = os.getenv("DB_PASSWORD")

engine, connection = bbdd.conexion(df, 'localhost', 'root', password , 'bd_talento_ABC_Company', 'tabla_general.csv')
cursor = connection.cursor()


# Borrar datos previos en todas las tablas (en orden para evitar errores de relaciones)
with engine.begin() as conn:
    inspector = inspect(conn)
    tablas_existentes = inspector.get_table_names()
    tablas = [
        "attrition", "employee_company", "employee_conditions", "employee_salary", "employee_satisfaction", "employee_training", "employee_career", 
        "employee_rating", "employee_info", "standardhours", "departments", "jobroles", "roledepartaments", "businesstravels", "maritalstatus"
    ]
    for tabla in tablas:
        if tabla in tablas_existentes:
            conn.execute(text(f"DROP TABLE {tabla}"))
            print(f"Tabla borrada: {tabla}")
        else:
            print(f"La tabla '{tabla}' no existe. No se pudo eliminar.")

tablas_sql = {
    "maritalstatus": """
        CREATE TABLE IF NOT EXISTS maritalstatus (
            id_maritalstatus INT NOT NULL AUTO_INCREMENT,
            maritalstatus VARCHAR(500) UNIQUE,
            PRIMARY KEY (id_maritalstatus)        
        )
    """,
    "businesstravels": """
        CREATE TABLE IF NOT EXISTS businesstravels (
            id_businesstravel INT NOT NULL AUTO_INCREMENT,
            businesstravel VARCHAR(500) UNIQUE,
            PRIMARY KEY (id_businesstravel)       
        )
    """,
    "roledepartaments": """
        CREATE TABLE IF NOT EXISTS roledepartaments (
            id_roledepartament INT NOT NULL AUTO_INCREMENT,
            roledepartament VARCHAR(500) UNIQUE,
            PRIMARY KEY (id_roledepartament)        
        )
    """,
    "jobroles": """
        CREATE TABLE IF NOT EXISTS jobroles (
            id_jobrole INT NOT NULL AUTO_INCREMENT,
            jobrole VARCHAR(500) UNIQUE,
            PRIMARY KEY (id_jobrole)        
        )
    """,
    "departments": """
        CREATE TABLE IF NOT EXISTS departments (
            id_department INT NOT NULL AUTO_INCREMENT,
            department VARCHAR(500) UNIQUE,
            PRIMARY KEY (id_department)        
        )
    """,
    "standardhours": """
        CREATE TABLE IF NOT EXISTS standardhours (
            id_standardhour INT NOT NULL AUTO_INCREMENT,
            standardhour VARCHAR(500) UNIQUE,
            PRIMARY KEY (id_standardhour)        
        )
    """,
    "employee_info": """
        CREATE TABLE IF NOT EXISTS employee_info (
            id_employee INT NOT NULL,
            age INT,
            gender VARCHAR(100),
            id_maritalstatus INT,
            education VARCHAR(100),
            distancefromhome FLOAT,
            PRIMARY KEY (id_employee),
            CONSTRAINT fk_employee_info_maritalstatus
                FOREIGN KEY (id_maritalstatus)
                REFERENCES maritalstatus(id_maritalstatus)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        )

    """,
    "employee_rating": """
        CREATE TABLE IF NOT EXISTS employee_rating (
            id_employee INT NOT NULL,
            jobinvolvement  VARCHAR(100),
            performancerating VARCHAR(100),
            CONSTRAINT fk_employee_rating_employee_info
                FOREIGN KEY (id_employee)
                REFERENCES employee_info(id_employee)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        )
    """,
    "employee_career": """
        CREATE TABLE IF NOT EXISTS employee_career (
            id_employee INT NOT NULL,
            numcompaniesworked  INT,
            totalworkingyears FLOAT,
            CONSTRAINT fk_employee_career_employee_info
                FOREIGN KEY (id_employee)
                REFERENCES employee_info(id_employee)
                ON DELETE CASCADE
                ON UPDATE CASCADE 
        )
    """,

    "employee_training": """
        CREATE TABLE IF NOT EXISTS employee_training (
            id_employee INT NOT NULL,
            trainingtimeslastyear INT,
            CONSTRAINT fk_employee_training_employee_info
                FOREIGN KEY (id_employee)
                REFERENCES employee_info(id_employee)
                ON DELETE CASCADE
                ON UPDATE CASCADE 
        )
    """,

    "employee_satisfaction": """
        CREATE TABLE IF NOT EXISTS employee_satisfaction (
            id_employee INT NOT NULL,
            environmentsatisfaction VARCHAR(100),
            jobsatisfaction VARCHAR(100),
            relationshipsatisfaction VARCHAR(100),
            worklifebalance INT,
            CONSTRAINT fk_employee_satisfaction_employee_info
                FOREIGN KEY (id_employee)
                REFERENCES employee_info(id_employee)
                ON DELETE CASCADE
                ON UPDATE CASCADE 
        )
    """,
    "employee_salary": """
        CREATE TABLE IF NOT EXISTS employee_salary (
            id_employee INT NOT NULL,
            salary FLOAT,
            percentsalaryhike INT,
            CONSTRAINT fk_employee_salary_employee_info
                FOREIGN KEY (id_employee)
                REFERENCES employee_info(id_employee)
                ON DELETE CASCADE
                ON UPDATE CASCADE 
        )
    """,
    "employee_conditions": """
        CREATE TABLE IF NOT EXISTS employee_conditions (
            id_employee INT NOT NULL,
            id_businesstravel INT,
            stockoptionlevel INT,
            id_standardhour INT,
            overtime VARCHAR(100), 
            remotework VARCHAR(100),
            CONSTRAINT fk_employee_conditions_employee_info
                FOREIGN KEY (id_employee)
                REFERENCES employee_info(id_employee)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT fk_employee_conditions_businesstravels
                FOREIGN KEY (id_businesstravel)
                REFERENCES businesstravels(id_businesstravel)
                ON DELETE CASCADE
                ON UPDATE CASCADE, 
            CONSTRAINT fk_employee_conditions_standardhours
                FOREIGN KEY (id_standardhour)
                REFERENCES standardhours(id_standardhour)
                ON DELETE CASCADE
                ON UPDATE CASCADE   
        )
    """,

    "employee_company": """
        CREATE TABLE IF NOT EXISTS employee_company (
            id_employee INT NOT NULL,
            id_department INT NOT NULL,
            id_roledepartament INT NOT NULL,
            joblevel INT,
            id_jobrole INT NOT NULL, 
            yearsatcompany INT, 
            yearssincelastpromotion INT,
            yearswithcurrmanager INT,
            CONSTRAINT fk_employee_company_employee_info
                FOREIGN KEY (id_employee)
                REFERENCES employee_info(id_employee)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT fk_employee_company_departments
                FOREIGN KEY (id_department)
                REFERENCES departments(id_department)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT fk_employee_company_roledepartaments
                FOREIGN KEY (id_roledepartament)
                REFERENCES roledepartaments(id_roledepartament)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT fk_employee_company_jobroles
                FOREIGN KEY (id_jobrole)
                REFERENCES jobroles(id_jobrole)
                ON DELETE CASCADE
                ON UPDATE CASCADE

        )
    """,

    "attrition": """
        CREATE TABLE IF NOT EXISTS attrition (
            id_employee INT NOT NULL,
            attrition VARCHAR(100),
            PRIMARY KEY (id_employee)
        )
    """,

    
}

for tabla, sql in tablas_sql.items():
    with engine.begin() as conn:
        conn.execute(text(sql))
        print(f"Tabla {tabla} creada exitosamente.")


#Tabla maritalstatus


valores_tabla_maritalstatus = []

df_maritalstatus = df[['maritalstatus']].copy()
df_maritalstatus.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_maritalstatus.append((row.maritalstatus,))

query_insercion_maritalstatus = "INSERT IGNORE INTO maritalstatus (maritalstatus) VALUES (%s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_maritalstatus, valores_tabla_maritalstatus)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


#Tabla businesstravels


valores_tabla_businesstravels = []

df_businesstravels = df[['businesstravel']].copy()
df_businesstravels.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_businesstravels.append((row.businesstravel,))

query_insercion_businesstravels = "INSERT IGNORE INTO businesstravels (businesstravel) VALUES (%s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_businesstravels, valores_tabla_businesstravels)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")



#Tabla roledepartaments


valores_tabla_roledepartaments = []

df_roledepartaments = df[['roledepartament']].copy()
df_roledepartaments.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_roledepartaments.append((row.roledepartament,))

query_insercion_roledepartaments = "INSERT IGNORE INTO roledepartaments (roledepartament) VALUES (%s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_roledepartaments, valores_tabla_roledepartaments)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


#Tabla jobroles


valores_tabla_jobroles = []

df_jobroles = df[['jobrole']].copy()
df_jobroles.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_jobroles.append((row.jobrole,))

query_insercion_jobroles = "INSERT IGNORE INTO jobroles (jobrole) VALUES (%s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_jobroles, valores_tabla_jobroles)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


#Tabla departments


valores_tabla_departments = []

df_departments = df[['department']].copy()
df_departments.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_departments.append((row.department,))

query_insercion_departments = "INSERT IGNORE INTO departments (department) VALUES (%s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_departments, valores_tabla_departments)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


#Tabla standardhours


valores_tabla_standardhours = []

df_standardhours = df[['standardhours']].copy()
df_standardhours.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_standardhours.append((row.standardhours,))

query_insercion_standardhours = "INSERT IGNORE INTO standardhours (standardhour) VALUES (%s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_standardhours, valores_tabla_standardhours)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


# Tabla employee_info

df_employee_info_sql = pd.read_sql("SELECT id_maritalstatus, maritalstatus FROM maritalstatus", con=engine)

df_employee_info = df[['employeenumber', 'age', 'gender', 'education', 'maritalstatus', 'distancefromhome']].copy()
df_employee_info.columns = ['id_employee', 'age', 'gender', 'education', 'maritalstatus', 'distancefromhome']

df_employee_info = df_employee_info.merge(df_employee_info_sql, on='maritalstatus', how='inner')

valores_tabla_employee_info = df_employee_info[[
    'id_employee', 'age', 'gender', 'id_maritalstatus', 'education', 'distancefromhome'
]].drop_duplicates().values.tolist()

query_insercion_employee_info = """
    INSERT INTO employee_info (
        id_employee, age, gender, id_maritalstatus, education, distancefromhome
    ) VALUES (%s, %s, %s, %s, %s, %s)
"""

try:
    cursor = connection.cursor()
    cursor.executemany(query_insercion_employee_info, valores_tabla_employee_info)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")



#Tabla employee_rating


valores_tabla_employee_rating = []

df_employee_rating = df[['employeenumber', 'jobinvolvement', 'performancerating']].copy()
df_employee_rating.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_employee_rating.append((row.employeenumber, row.jobinvolvement,row.performancerating))

query_insercion_employee_rating = "INSERT IGNORE INTO employee_rating (id_employee, jobinvolvement, performancerating) VALUES (%s, %s, %s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_employee_rating, valores_tabla_employee_rating)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


#Tabla employee_career


valores_tabla_employee_career = []

df_employee_career = df[['employeenumber', 'numcompaniesworked', 'totalworkingyears']].copy()
df_employee_career.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_employee_career.append((row.employeenumber, row.numcompaniesworked,row.totalworkingyears))

query_insercion_employee_career = "INSERT IGNORE INTO employee_career (id_employee, numcompaniesworked, totalworkingyears) VALUES (%s, %s, %s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_employee_career, valores_tabla_employee_career)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


#Tabla employee_training


valores_tabla_employee_training = []

df_employee_training = df[['employeenumber', 'trainingtimeslastyear']].copy()
df_employee_training.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_employee_training.append((row.employeenumber, row.trainingtimeslastyear))

query_insercion_employee_training = "INSERT IGNORE INTO employee_training (id_employee, trainingtimeslastyear) VALUES (%s, %s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_employee_training, valores_tabla_employee_training)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


df['worklifebalance'] = df['worklifebalance'].astype(int)

#Tabla employee_satisfaction


valores_tabla_employee_satisfaction = []

df_employee_satisfaction = df[['employeenumber', 'environmentsatisfaction', 'jobsatisfaction', 'relationshipsatisfaction', 'worklifebalance']].copy()
df_employee_satisfaction.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_employee_satisfaction.append((row.employeenumber, row.environmentsatisfaction, row.jobsatisfaction, row.relationshipsatisfaction, row.worklifebalance))

query_insercion_employee_satisfaction = "INSERT IGNORE INTO employee_satisfaction (id_employee, environmentsatisfaction, jobsatisfaction, relationshipsatisfaction, worklifebalance) VALUES (%s, %s, %s, %s, %s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_employee_satisfaction, valores_tabla_employee_satisfaction)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


#Tabla employee_salary


valores_tabla_employee_salary = []

df_employee_salary = df[['employeenumber', 'salary', 'percentsalaryhike']].copy()
df_employee_salary.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_employee_salary.append((row.employeenumber, row.salary, row.percentsalaryhike))

query_insercion_employee_salary = "INSERT IGNORE INTO employee_salary (id_employee, salary, percentsalaryhike) VALUES (%s, %s, %s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_employee_salary, valores_tabla_employee_salary)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


# Tabla employee_conditions

df_conditions_businesstravels_sql = pd.read_sql("SELECT id_businesstravel, businesstravel FROM businesstravels", con=engine)
df_conditions_standardhours_sql = pd.read_sql("SELECT id_standardhour, standardhour FROM standardhours", con=engine)

df_employee_conditions = df[['employeenumber', 'businesstravel', 'stockoptionlevel', 'standardhours', 'overtime', 'remotework']].copy()
df_employee_conditions.columns = ['id_employee', 'businesstravel', 'stockoptionlevel', 'standardhour', 'overtime', 'remotework']

df_employee_conditions = df_employee_conditions.merge(df_conditions_businesstravels_sql, on='businesstravel', how='inner')
df_employee_conditions = df_employee_conditions.merge(df_conditions_standardhours_sql, on='standardhour', how='inner')

valores_tabla_employee_conditions = df_employee_conditions[[
    'id_employee', 'id_businesstravel', 'stockoptionlevel', 'id_standardhour', 'overtime', 'remotework'
]].drop_duplicates().values.tolist()

query_insercion_employee_conditions = """
    INSERT INTO employee_conditions (
        id_employee, id_businesstravel, stockoptionlevel, id_standardhour, overtime, remotework
    ) VALUES (%s, %s, %s, %s, %s, %s)
"""

try:
    cursor = connection.cursor()
    cursor.executemany(query_insercion_employee_conditions, valores_tabla_employee_conditions)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


# Tabla employee_company

df_conditions_departments_sql = pd.read_sql("SELECT id_department, department FROM departments", con=engine)
df_conditions_roledepartaments_sql = pd.read_sql("SELECT id_roledepartament, roledepartament FROM roledepartaments", con=engine)
df_conditions_jobroles_sql = pd.read_sql("SELECT id_jobrole, jobrole FROM jobroles", con=engine)

df_employee_company = df[[
    'employeenumber', 'department', 'roledepartament', 'joblevel',
    'jobrole', 'yearsatcompany', 'yearssincelastpromotion', 'yearswithcurrmanager'
]].copy()

df_employee_company.columns = [
    'id_employee', 'department', 'roledepartament', 'joblevel',
    'jobrole', 'yearsatcompany', 'yearssincelastpromotion', 'yearswithcurrmanager'
]

df_employee_company = df_employee_company.merge(df_conditions_departments_sql, on='department', how='inner')
df_employee_company = df_employee_company.merge(df_conditions_roledepartaments_sql, on='roledepartament', how='inner')
df_employee_company = df_employee_company.merge(df_conditions_jobroles_sql, on='jobrole', how='inner')

valores_tabla_employee_company = df_employee_company[[
    'id_employee', 'id_department', 'id_roledepartament', 'joblevel',
    'id_jobrole', 'yearsatcompany', 'yearssincelastpromotion', 'yearswithcurrmanager'
]].drop_duplicates().values.tolist()

query_insercion_employee_company = """
    INSERT INTO employee_company (
        id_employee, id_department, id_roledepartament, joblevel,
        id_jobrole, yearsatcompany, yearssincelastpromotion, yearswithcurrmanager
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

try:
    cursor = connection.cursor()
    cursor.executemany(query_insercion_employee_company, valores_tabla_employee_company)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")



#Tabla attrition


valores_tabla_attrition = []

df_attrition = df[['employeenumber', 'attrition']].copy()
df_attrition.drop_duplicates(inplace=True)

for row in df.itertuples():
    valores_tabla_attrition.append((row.employeenumber, row.attrition))

query_insercion_attrition = "INSERT IGNORE INTO attrition (id_employee, attrition) VALUES (%s, %s)"

try:
    cursor = connection.cursor() 
    cursor.executemany(query_insercion_attrition, valores_tabla_attrition)
    connection.commit()
    print(cursor.rowcount, "registros insertados correctamente 🎉")
except mysql.connector.Error as err:
    print("Error al insertar datos:")
    print(f"Error code {err.errno}")
    print(f"SQLSTATE {err.sqlstate}")
    print(f"MESSAGE {err.msg}")


connection.commit()
cursor.close()
connection.close()




