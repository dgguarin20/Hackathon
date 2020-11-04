import numpy as np
import sqlite3



#FUNCION CON OBJETIVO DE CREAR LAS TABLAS.

def create_tables():
    #CREA LA BASE DE DATO SI NO EXISTE. SI EXISTE SOLO ENTRA.
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    #CREA LAS TABLAS EN LA BASE DE DATOS
    c.execute("CREATE TABLE IF NOT EXISTS CampoCrudo(id INTEGER PRIMARY KEY, campo TEXT, year INTEGER, departamentoID INTEGER, municipioID INTEGER, operadoraID INTEGER, cuencaID INTEGER, contradoID INTEGER, datosID INTEGER )")
    c.execute("CREATE TABLE IF NOT EXISTS Departamento(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Municipio(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Empresa(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Cuenca(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Contrato(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Data(id INTEGER PRIMARY KEY, enero REAL, febrero REAL, marzo REAL, abril REAL, mayo REAL, junio REAL, julio REAL, agosto REAL, septiembre REAL, octubre REAL, noviembre REAL, diciembre REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS CampoGas(id INTEGER PRIMARY KEY, campo TEXT, year INTEGER, departamentoID INTEGER, municipioID INTEGER, operadoraID INTEGER, cuencaID INTEGER, contradoID INTEGER, datosID INTEGER, datosMesID INTEGER, mes TEXT)")
    conn.commit()

def quitarAcento(valor):
    #Acentos
    v = valor.replace("á","a")
    v2 = v.replace("é","e")
    v3 = v2.replace("í","i")
    v4 = v3.replace("ó", "o")
    v5 = v4.replace("ú", "u")
    return v5

def limpiar(valor):

    v = valor.replace("/"," ")
    v2 = v.replace("-"," ")
    v3 = v2.replace("  ", " ")
    v5 = v3.replace("  ", " ")
    v6 = v5.replace("  ", " ")
    v7 = v6.replace(".", "")
    
    v4 = ""
    #Quitar el espacio alfinal
    if len(v7)!=0:
        if " " == v7[len(v7)-1]:
            for i in range(0, len(v7)-1):
                v4+=v7[i]
        else:
            v4 = v7
    else:
        v4 = v7
    
    return v4

#Insertar Departamento parametro es el departamento a insertar y regresa un id
def insertarDepartamento(departamento):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    #Verificar que departamentos hay y si ya existe
    c.execute("SELECT * FROM Departamento")
    existe = False
    idD = 0
    #Limpiar datos
    departamento = quitarAcento(departamento)
    departamento = limpiar(departamento)
    for row in c.fetchall():
        if departamento == row[1]:
            existe =True
            idD = row[0]
            break
        idD = row[0]+1
    #Si no existe se agrega
    if existe == False:
        c.execute("INSERT INTO Departamento(id, nombre) VALUES (?, ?)", (idD, departamento))
        conn.commit()
    return idD
#Insertar Municipio parametro es el municipio a insertar y regresa un id.
def insertarMunicipio(municipio):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Municipio")
    existe = False
    idM = 0
    #Limpiar dato
    municipio = quitarAcento(municipio)
    municipio = limpiar(municipio)
    #Verificar si ya existe
    for row in c.fetchall():
        if municipio == row[1]:
            existe =True
            idM = row[0]
            break
        idM = row[0]+1
    if existe == False:
        c.execute("INSERT INTO Municipio(id, nombre) VALUES (?, ?)", (idM, municipio))
        conn.commit()
    return idM

#Insertar Empresa con parametro de empresa a inesrtar y regresa id.
def insertarEmpresa(empresa):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Empresa")
    existe = False
    idE = 0
    #Limpia datos
    empresa = quitarAcento(empresa)
    empresa = limpiar(empresa)
    #chekea si existe o no
    for row in c.fetchall():
        if empresa == row[1]:
            existe =True
            idE = row[0]
            break
        idE = row[0]+1
    #Si no existe
    if existe == False:
        c.execute("INSERT INTO Empresa(id, nombre) VALUES (?, ?)", (idE, empresa))
        conn.commit()
    return idE

#Inserta Cuenca. parametro cuenca a insertar y regresa el id donde lo guardo
def insertarCuenca(cuenca):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Cuenca")
    existe = False
    idC = 0
    #Limpiar datos
    cuenca = quitarAcento(cuenca)
    cuenca = limpiar(cuenca)
    #Veirfica si ya existe
    for row in c.fetchall():
        if cuenca == row[1]:
            existe =True
            idC = row[0]
            break
        idC = row[0]+1
        #Sino existe lo agrega
    if existe == False:
        c.execute("INSERT INTO Cuenca(id, nombre) VALUES (?, ?)", (idC, cuenca))
        conn.commit()
    return idC
#Insertar Contrato, parametro es el contrato a insertar regresa un id que se agrego
def insertarContrato(contrato):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Contrato")
    existe = False
    idCo = 0
    #Limpia datos
    contrato = quitarAcento(contrato)
    contrato = limpiar(contrato)
    #Verifica si ya existe
    for row in c.fetchall():
        if contrato == row[1]:
            existe =True
            idCo = row[0]
            break
        idCo = row[0]+1
        #Si no existe inserta
    if existe == False:
        c.execute("INSERT INTO Contrato(id, nombre) VALUES (?, ?)", (idCo, contrato))
        conn.commit()
    return idCo
#Insertar datos, parametro son los meses con sus datos regresa el id que identifica donde lo guardo
def insertarData(arregloMes):
    
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Data")
    rows = c.fetchall()
    idDa = len(rows)+1
    #Agrega los datos
    c.execute("INSERT INTO Data(id, enero, febrero, marzo, abril, mayo, junio, julio, agosto, septiembre, octubre, noviembre, diciembre) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (idDa, arregloMes[0], arregloMes[1], arregloMes[2], arregloMes[3], arregloMes[4], arregloMes[5], arregloMes[6], arregloMes[7], arregloMes[8], arregloMes[9], arregloMes[10], arregloMes[11]))
    conn.commit()
    return idDa
#Inserta campoCrudo, parametros son los id retornados anteriormente y pues, esta el campo a insertar y el year. 
def insertarCampoCrudo(idD, idM, idE, idC, idCo, idDa, campo, year):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Data")
    rows = c.fetchall()
    #Para encontrar el id
    idCamp = len(rows)
    campo = quitarAcento(campo)
    campo = limpiar(campo)
    #Inserta los datos
    c.execute("INSERT INTO CampoCrudo(id, campo, year, departamentoID, municipioID, operadoraID, cuencaID, contradoID, datosID ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (idCamp, campo, year, idD, idM, idE, idC, idCo, idDa))
    conn.commit()
#Borra todas las tablas
def deleteTable():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("DROP TABLE Data")
    c.execute("DROP TABLE CampoCrudo")
    c.execute("DROP TABLE Departamento")
    c.execute("DROP TABLE Municipio")
    c.execute("DROP TABLE Empresa")
    c.execute("DROP TABLE Cuenca")
    c.execute("DROP TABLE Contrato")
    conn.commit

#Funciones que buscan toda la informacion de la selectiva tabla
def getCampoCrudo():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo")
    rows = c.fetchall()
    return rows
def getData():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Data")
    rows = c.fetchall()
    return rows
def getContrato():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Contrato")
    rows = c.fetchall()
    return rows
def getCuenca():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Cuenca")
    rows = c.fetchall()
    return rows
def getEmpresa():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Empresa")
    rows = c.fetchall()
    return rows
def getMunicipio():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Municipio")
    rows = c.fetchall()
    return rows
def getDepartamento():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Departamento")
    rows = c.fetchall()
    return rows
#Funcion para escribir el borde en la base de datos
def encontrarIDBorde():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE year = -1")
    rows = c.fetchall()
    return rows
#Funciones con el objetivo de regresar unos valores que cumplan lo que se busca.Esto se dan en los parametros la columna y el valor
def getCampoCrudoByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    mensaje = "SELECT * FROM CampoCrudo WHERE {} = {}".format(columna, valor)
    c.execute(mensaje)
    rows = c.fetchall()
    return rows
              
def getDataByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    mensaje = "SELECT * FROM Data WHERE {} = {}".format(columna, valor)
    c.execute(mensaje)
    rows = c.fetchall()
    return rows
              
def getContratoByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    mensaje = "SELECT * FROM Contrato WHERE {} = {}".format(columna, valor)
    c.execute(mensaje)
    rows = c.fetchall()
    return rows
def getCuencaByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    mensaje = "SELECT * FROM Cuenca WHERE {} = {}".format(columna, valor)
    c.execute(mensaje)
    rows = c.fetchall()
    return rows
              
def getEmpresaByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    mensaje = "SELECT * FROM Empresa WHERE {} = {}".format(columna, valor)
    c.execute(mensaje)
    rows = c.fetchall()
    return rows
              
def getMunicipioByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    mensaje = "SELECT * FROM Municipio WHERE {} = {}".format(columna, valor)
    c.execute(mensaje)
    rows = c.fetchall()
    return rows
              
def getDepartamentoByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    mensaje = "SELECT * FROM Departamento WHERE {} = {}".format(columna, valor)
    c.execute(mensaje)
    rows = c.fetchall()
    return rows
#Cuando son varios valores a buscar en vez de uno
def getCampoCrudeByManyValue(lista):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    mensaje = "SELECT * FROM Departamento WHERE"
    #Se hace el recorrido para ver todas las condiciones
    for i in range(0, len(lista)):
        columna = lista[i][0]
        valor = lista[i][1]
        m = "{} = {}".format(columna, valor)
        mensaje= mensaje + m
        if i < len(lista)-1:
            mensaje = mensaje + "AND"
    c.execute(mensaje)
    rows = c.fetchall()
    return rows