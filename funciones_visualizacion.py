import pandas as pd 
import numpy as np
from sqlalchemy import create_engine
import pymysql
from IPython.display import display
import matplotlib.pyplot as plt
import seaborn as sns


pd.set_option('display.max_columns', None)


def ver_unicos(df):
    print('  ')
    print('VISUALIZACIÓN DE ÚNICOS')
    print('  ')
    for col in df.select_dtypes(include='object'):
        print(col)
        print('-----------------------------')
        print(df[col].unique())    


def extract_data(df):
    print('INFORMACIÓN SOBRE COLUMNAS')
    print('  ')
    print(df.info())
    print('  ')
    print('--------------------------------------------------')
    print('  ')
    print('VISUALIZACIÓN DE NULOS')
    print('  ')
    print(df.isnull().sum())
    ver_unicos(df)

    return df.head()



def porcentaje_nulos(df):
    nulos = df.isnull().sum()/df.shape[0]*100
    nulos = nulos[nulos > 0]
    nulos.sort_values(ascending=False)
    nulos = nulos.to_frame(name='perc_nulos').reset_index()
    return nulos


def exploracion_basica(df):
    print("Primeras filas:")
    print(df.head())
    
    print("\nDimensiones del DataFrame:")
    print(df.shape)
    
    print("\nTipos de datos:")
    print(df.dtypes)
    
    print("\nValores únicos por columna:")
    print(df.nunique())

    print("\nValores Nulos")
    print(df.isnull().sum())

    print("\nValores Duplicados")
    print(df.duplicated().sum())
    
def distribucion(df, tipo):
    for col in df.select_dtypes(include= tipo):
        print('-----------------------------')
        print(f"La distribución de las categorías para la columna {col.upper()}")
        print(df[col].nunique())
        print(df[col].value_counts(normalize=True))  


def atipicos(df):
    num_bins = 40
    df.hist(bins=num_bins, figsize=(25,25))
    plt.savefig("histogram_plots")
    plt.show()     


def boxplot(df):
    numeric_columns = df.select_dtypes(include=['number']).columns
    rows = (len(numeric_columns) // 3) + (len(numeric_columns) % 3 > 0)  # Calcula el número de filas necesarias
    cols = 3

    # Definir tamaño del gráfico
    fig, axes = plt.subplots(nrows=rows, ncols=cols, figsize=(15, rows*3))
    if rows > 1:
        axes = axes.flatten()
    # Iterar sobre cada columna numérica y graficar un boxplot
    for i, col in enumerate(numeric_columns):
        sns.boxplot(data=df[col], ax=axes[i])
        axes[i].set_title(f"Boxplot de {col}")

    # Ajustar el layout para evitar solapamientos
    plt.tight_layout()
    plt.show()         


