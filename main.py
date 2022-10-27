import pandas as pd
from sqlalchemy import create_engine

#APERTURA Y CONVERSION DE ARCHIVOS


#ARCHIVO 2

def Abrir():
    

    psemana413=pd.read_csv(r'precios_semana_20200413.csv',encoding='utf-16')
    psemana413.to_csv(r'csvfiles\psemana413csv',index=False)

    producto=pd.read_parquet(r'producto.parquet',engine='pyarrow')
    producto.to_csv(r'csvfiles\productocsv',index=False)



    psemana426=pd.read_excel(r"precios_semanas_20200419_20200426.xlsx",sheet_name=None)
    psemana426df=  pd.concat(psemana426, ignore_index=True)
    psemana426df.to_csv(r'csvfiles\psemana426csv',index=False)


    psemana503=pd.read_json(r"precios_semana_20200503.json")
    psemana503.to_csv(r'csvfiles\psemana503csv',index=False)

    psemana508=pd.read_csv(r"precios_semana_20200518.txt",encoding='utf-8',sep='|')
    psemana508.to_csv(r'csvfiles\psemana518csv',index=False)

    sucursal=pd.read_csv(r"sucursal.csv")
    sucursal.to_csv(r'csvfiles\sucursalcsv',index=False)

    
    
        
    return 'Convertido'
Abrir()

#ABRIMOS ARCHIVOS YA CONVERTIDOS
archivo1=pd.read_csv(r"csvfiles\psemana413csv")
archivo2=pd.read_csv(r'csvfiles\productocsv')
archivo3=pd.read_csv(r"csvfiles\psemana426csv")
archivo4=pd.read_csv(r"csvfiles\psemana503csv")
archivo5=pd.read_csv(r'csvfiles\psemana518csv')
archivo6=pd.read_csv(r'csvfiles\sucursalcsv')






#Limpiar y transformar
def ModificaValoresDeColumna(df,col,remplazar,por):
    
    if remplazar == None:
        inreplace = None
        if type(por==str):
            por=str(por)
            return df[col].replace(to_replace=[remplazar], value=por, inplace=True)


#ETL 1

archivo1['precio']=archivo1['precio'].round(2)


archivo1.to_csv(r'archivoslistos\archivo1listo', sep=',',index=False)


#ETL 2
ModificaValoresDeColumna(archivo2,'categoria1',None,'SIN CATEGORIA1')
ModificaValoresDeColumna(archivo2,'categoria2',None,'SIN CATEGORIA2')
ModificaValoresDeColumna(archivo2,'categoria3',None,'SIN CATEGORIA3')

archivo2.to_csv(r'archivoslistos\archivo2listo', sep=',',index=False)

#ETL 3
archivo3=archivo3.fillna(0)
archivo3['producto_id']=archivo3['producto_id'].astype('string').str.rstrip('.0')
archivo3['producto_id']=archivo3['producto_id'].str.zfill(13)
archivo3['sucursal_id']=archivo3['sucursal_id'].astype('string').str.replace('/','-')
archivo3['sucursal_id']=archivo3['sucursal_id'].str.rstrip('00:00:00')
archivo3.reindex(columns = ['precio', 'producto_id', 'sucursal_id'])

archivo3.to_csv(r'archivoslistos\archivo3listo', sep=',',index=False)

#ETL 4

archivo4['precio']=archivo4['precio'].round(2)
archivo4.to_csv(r'archivoslistos\archivo4listo', sep=',',index=False)
#ETL 5

archivo5['precio']=archivo5['precio'].round(2)
archivo5.to_csv(r'archivoslistos\archivo5listo', sep=',',index=False)

#ETL 6
archivo6.to_csv(r'archivoslistos\archivo6listo', sep=',',index=False)




#CONECTAMOS SQL
cadena_conexion= 'mysql+pymysql://root:123456789@localhost:3306/dataengine'

conexion= create_engine(cadena_conexion)

#ABRIMOS LOS ARCHIVOS LIMPIOS PARA SUBIRLO A SQL
listo1=pd.read_csv(r'archivoslistos\archivo1listo')
listo3=pd.read_csv(r"archivoslistos\archivo3listo")
listo4=pd.read_csv(r'archivoslistos\archivo4listo')
listo5=pd.read_csv(r'archivoslistos\archivo5listo')
listo6=pd.read_csv(r'archivoslistos\archivo6listo')


#USAMOS ESTA FUNCION QUE CARGA LA TABLA A MYSQL


def SqlAlchemy(archivo,nombre):  
    
    
    archivo.to_sql(name=nombre, con=conexion)

SqlAlchemy(listo1,'Precios_Semana_0413')
#SqlAlchemy(listo_2,'Producto')
SqlAlchemy(listo3,'Precios_Semana_426')
SqlAlchemy(listo4,'Precios_Semana_503')
SqlAlchemy(listo5,'Precios_Semana_508')
SqlAlchemy(listo6,'Sucursal')


#FUNCION QUE UNE LAS TABLAS

def Pipeline():
    final=pd.concat([listo1,listo3,listo4,listo5])
    final.to_csv(r'archivoslistos\Precios_Final')
    SqlAlchemy(final,'Precios_Final')


Pipeline()


