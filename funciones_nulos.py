import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.impute import KNNImputer

#Nulos

def negativos_a_nulos (df, col):
    df.loc[df[col] < 0, col] = np.nan


#Métoco KNNImputer

def impu_KNNImputer (df, col):
    imputer_iter = KNNImputer(n_neighbors=5)
    df[col] = imputer_iter.fit_transform(df[[col]])    


#Visualizacion nulos
def ver_nulos(df):
    nulos = df.isnull().sum()/df.shape[0]*100
    nulos = nulos.sort_values(ascending=False)
    print("\nPorcentaje de valores nulos por columna:\n")
    print(nulos)


