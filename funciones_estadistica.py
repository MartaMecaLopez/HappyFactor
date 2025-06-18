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
    colores = sns.color_palette("coolwarm", len(counts))
    plt.pie(counts, labels=counts.index, autopct='%1.1f%%', startangle=90, colors=colores)
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



# Matriz de correlación
def matriz_correlacion (df):
 
    num_vars = df.select_dtypes(include=np.number).columns.tolist()
    correlation_matrix = df[num_vars].corr()

    plt.figure(figsize=(15, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=.5)
    # Añade título
    plt.title('Matriz de Correlación entre Variables')
    # Muestra el gráfico
    plt.show()   



def grafico_comparacion(df, colX, matiz, nombreX, nombreY):
    sns.countplot(x=colX, data=df, color = 'c', hue = matiz)
    plt.xticks(rotation=45)
    plt.xlabel(nombreX)
    plt.ylabel(nombreY)
    plt.show() 



def comparacion_con_porcentajes(df, colX, matiz, nombreX, nombreY, titulo):
    grafico = sns.countplot(x=colX, data= df, color = 'c', hue = matiz)
    plt.xticks(rotation=45)

    plt.xlabel(nombreX)
    plt.ylabel(nombreY)

    total = len(df)
    for p in grafico.patches:
        height = p.get_height()
        percentage = f'{100 * height / total:.1f}%'
        grafico.text(
            p.get_x() + p.get_width() / 2, 
            height + 1,  # Ajuste para que no tape la barra
            percentage, 
            ha='center'
        )
    plt.title(titulo)
    plt.tight_layout()
    plt.show()     


def clasificar_veterania(df, col, nueva_col):
    df[nueva_col] = df[col].apply(lambda x: '+ de 10 años' if x > 10 else '-= de 10 años')
    return df   