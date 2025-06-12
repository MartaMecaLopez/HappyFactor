import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


# elegir tamaño de la imagen de los gráficos
plt.figure(figsize=(15, 20))

# Histograma
def histograma(df,col, titulo, eje_x, eje_y):
    plt.subplot(3, 2, 1) # plt.subplot(n_filas, n_columnas, índice)
    sns.histplot(df[col].dropna(), bins=20, kde=False)
    plt.title(titulo)
    plt.xlabel(eje_x)
    plt.ylabel(eje_y)
    plt.tight_layout()
    plt.show()

# Gráfico de barras 
def grafico_barras(df, col, titulo, eje_x, eje_y):
    orden = df[col].value_counts().index
    sns.countplot(x=col, data=df, order=orden)
    plt.xticks(rotation=45)
    plt.title(titulo)
    plt.xlabel(eje_x)
    plt.ylabel(eje_y)
    plt.tight_layout()
    plt.show()

# Gráfico de pastel
def grafico_pastel(df, col, titulo):
    plt.figure(figsize=(5, 5))
    counts = df[col].value_counts()
    plt.pie(counts, labels=counts.index, autopct='%1.1f%%', startangle=90)
    plt.title(titulo)
    plt.tight_layout()
    plt.show()

# Boxplot de colx por coly (ej: Boxplot de tarifa por clase)
def boxplot(df, colx, coly, titulo, eje_x, eje_y):
    plt.figure(figsize=(5, 5))
    sns.boxplot(x=colx, y=coly, data=df)
    plt.title(titulo)
    plt.xlabel(eje_x)
    plt.ylabel(eje_y)
    plt.tight_layout()
    plt.show()

# Gráfico de dispersión colx vs coly (EJ: Gráfico de dispersión Edad vs Tarifa)

def grafico_dispersion(df, colx, coly, titulo, eje_x, eje_y):
    plt.subplot(3, 2, 5)  # plt.subplot(n_filas, n_columnas, índice)
    sns.scatterplot(x=colx, y=coly, data=df)
    plt.title(titulo)
    plt.xlabel(eje_x)
    plt.ylabel(eje_y)

    plt.tight_layout()
    plt.show()