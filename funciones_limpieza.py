import pandas as pd
import numpy as np
from word2number import w2n

def letras_a_numeros(df, col):
    def convertir(valor):
        try:
            return int(valor)  #cambiar de str a int si ya es un número
        except:
            try:
                return w2n.word_to_num(valor)  # si es texto
            except:
                return np.nan  # si no se puede convertir

    df[col] = df[col].apply(convertir) #recorre cada elemento de la columna y lo covierte en "valor"
    return df[col].unique()

def limpieza_divisas (df, col, tipo):
    df[col] = df[col].str.replace('$', "")
    df[col] = df[col].str.replace(',', ".")
    df[col] = df[col].astype(tipo)

def limpieza_numeros (df, col, tipo):
    df[col] = df[col].str.replace(',', ".")
    df[col] = df[col].astype(tipo)

def limpieza_boleanos (df, col, tipo):
    df[col] = df[col].str.replace('Yes', "True")
    df[col] = df[col].str.replace('1', "True")
    df[col] = df[col].str.replace('0', "False")
    df[col] = df[col].astype(tipo)

def limpieza_genero (df, col, tipo):
    df[col] = df[col].astype(tipo) 
    df[col] = df[col].str.replace('1', "Female")
    df[col] = df[col].str.replace('0', "Male")

def primer_digito(df, col):
    df[col] = df[col].astype(str).str[0]  # toma solo el primer carácter

def limpieza_maritalstatus (df, col):
    df[col] = df[col].str.replace(r'^marr.*', 'married', regex=True)
    df[col] = df[col].str.replace(r'^div.*', 'divorced', regex=True)
    df[col] = df[col].str.replace(r'^sin.*', 'single', regex=True)

def minusculas(df):
    for col in df.columns:
        if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].str.lower()


def minusculas_titulos(df):
    nuevas_columnas = {}
    for col in df.columns:
        nuevas_columnas[col] = col.lower()

    df.rename(columns = nuevas_columnas, inplace = True)
    df.columns    

def cambio_tipo(df, col, tipo):
    df[col] = df[col].astype(tipo) 

def quitar_espacios(df):
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()


def cambiar_a_entero(df, col):
    df[col] = pd.to_numeric(df[col], errors='coerce')  # Convierte a numérico (float si hay decimales o NaN)
    df[col] = df[col].astype('Int64')  #convierte a entero permitiendo NaNs (Int64 de pandas, no int normal)

   

def cambiar_objeto(df, col):
    df[col] = df[col].astype(str)


def meses(df, col):
    diccionario_mapa = {1: "enero", 2: "febrero", 3: "marzo",  4: "abril",  5: "mayo",  6: "junio",  7: "julio",  8: "agosto",  9: "septiembre",  10: "octubre",  11: "noviembre",  12: "diciembre",} 
    for item in col:
        df[item] = df[item].map(diccionario_mapa)



#NULOS

# Cambiar nulo por nueva categoría
def objeto_categoria (df, col):
    df[col] = df[col].fillna('Unknown')
# Cambiar nulo por moda
def objeto_moda(df, col):
    moda= df[col].mode()[0] 
    df[col] = df[col].fillna(moda)
    return moda
# Cambiar nulo por mediana
def mediana_num(df, col):
    mediana= df[col].median()
    df[col] = df[col].fillna(mediana)   
    nulos= df.isnull().sum().sort_values(ascending= False)
    return nulos





 