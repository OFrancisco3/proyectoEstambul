import pandas as pd

# Cargar el primer conjunto de datos y eliminar la columna 'gender'
df1 = pd.read_csv('D:\\descargar666\\posibledataset\\customer_shopping_data.csv')
df1 = df1.drop('gender', axis=1)
print("df1 cargado y columna 'gender' eliminada")
# Cargar el segundo conjunto de datos
df2 = pd.read_csv('D:\\descargar666\\posibledataset\\combinado_csv.csv')
print("df2 cargado")
# Unir los dos conjuntos de datos por una columna común (puedes ajustar el nombre de la columna)
resultado = pd.concat([df1, df2], axis=1)
print("Concatenación realizada")
# Mostrar el resultado
#print(resultado)

# Guardar el resultado en un nuevo archivo CSV si es necesario
resultado.to_csv('datasetproyectov2.csv', index=False) #falta especificar donde lo guarda
print("Archivo guardado")