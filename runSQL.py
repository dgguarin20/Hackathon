import numpy as np
import sqlite3





def create_tables():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS CampoCrudo(id INTEGER PRIMARY KEY, campo TEXT, year INTEGER, departamentoID INTEGER, municipioID INTEGER, operadoraID INTEGER, cuencaID INTEGER, contradoID INTEGER, datosID INTEGER )")
    c.execute("CREATE TABLE IF NOT EXISTS Departamento(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Municipio(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Empresa(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Cuenca(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Contrato(id INTEGER PRIMARY KEY, nombre TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS Data(id INTEGER PRIMARY KEY, enero REAL, febrero REAL, marzo REAL, abril REAL, mayo REAL, junio REAL, julio REAL, agosto REAL, septiembre REAL, octubre REAL, noviembre REAL, diciembre REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS CampoGas(id INTEGER PRIMARY KEY, campo TEXT, year INTEGER, departamentoID INTEGER, municipioID INTEGER, operadoraID INTEGER, cuencaID INTEGER, contradoID INTEGER, datosID INTEGER, datosMesID INTEGER, mes TEXT)")
    conn.commit()
    
def insertarTablaCrudo(valor, encabezado, year):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    campo = ""
    departamento = ""
    municipio = ""
    operadora = ""
    cuenca = ""
    contrato = ""
    arregloMes = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    termino = False
    for i in range(0,len(encabezado)):
        if valor[i]=="":
            value = "0"
        else:
            value = valor[i]
            
          
        if "departamento" == encabezado[i].lower():
            departamento = value.lower()
        elif "campo" == encabezado[i].lower():
            campo = value.lower()
        elif "municipio" == encabezado[i].lower():
            municipio = value.lower()
        elif "operadora" == encabezado[i].lower() or "empresa" == encabezado[i].lower():
            operadora = value.lower()           
        elif "cuenca" == encabezado[i].lower():
            cuenca = value.lower()
        elif "contrato" == encabezado[i].lower():
            contrato = value.lower()
        elif "enero"== encabezado[i].lower() or "ene" in encabezado[i].lower():
            arregloMes[0] = float(value)
        elif "febrero"== encabezado[i].lower() or "feb" in encabezado[i].lower():
            arregloMes[1] = float(value) 
        elif "marzo"== encabezado[i].lower():
            arregloMes[2] = float(value)
        elif "abril"== encabezado[i].lower():
            arregloMes[3] = float(value)
        elif "mayo"== encabezado[i].lower():
            arregloMes[4] = float(value)
        elif "junio"== encabezado[i].lower():
            arregloMes[5] = float(value)
        elif "julio"== encabezado[i].lower():
            arregloMes[6] = float(value)
        elif "agosto"== encabezado[i].lower():
            arregloMes[7] = float(value)
        elif "septiembre"== encabezado[i].lower():
            arregloMes[8] = float(value)
        elif "octubre"== encabezado[i].lower():
            arregloMes[9] = float(value)
        elif "noviembre"== encabezado[i].lower():
            arregloMes[10] = float(value)
        elif "diciembre"== encabezado[i].lower():
            arregloMes[11] = float(value)
        elif "concatenar" == encabezado[i].lower():
            print("concatenar")
        else: 
            print("no")
            print(encabezado[i].lower())
            print(valor[i])
    
    if termino == True:
        print(termino) 
    else:
        conn = sqlite3.connect("hackathon.db")
        c = conn.cursor()
        
        idD = insertarDepartamento(departamento)
        idM = insertarMunicipio(municipio)
        idE = insertarEmpresa(operadora)
        idC = insertarCuenca(cuenca)
        idCo = insertarContrato(contrato)
        idDa = insertarData(arregloMes)
        insertarCampoCrudo(idD, idM, idE, idC, idCo, idDa, campo, year)
        
def insertarBordeANH():
    campo = "-1"
    departamento = "-1"
    municipio = "-1"
    operadora = "-1"
    cuenca = "-1"
    contrato = "-1"
    arregloMes = [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1]
    year = "-1"
    idD = insertarDepartamento(departamento)
    idM = insertarMunicipio(municipio)
    idE = insertarEmpresa(operadora)
    idC = insertarCuenca(cuenca)
    idCo = insertarContrato(contrato)
    idDa = insertarData(arregloMes)
    insertarCampoCrudo(idD, idM, idE, idC, idCo, idDa, campo, year)

    
def quitarAcento(valor):
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
    if len(v7)!=0:
        if " " == v7[len(v7)-1]:
            for i in range(0, len(v7)-1):
                v4+=v7[i]
        else:
            v4 = v7
    else:
        v4 = v7
    
    return v4

def insertarDepartamento(departamento):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Departamento")
    existe = False
    idD = 0
    departamento = quitarAcento(departamento)
    departamento = limpiar(departamento)
    for row in c.fetchall():
        if departamento == row[1]:
            existe =True
            idD = row[0]
            break
        idD = row[0]+1
    if existe == False:
        c.execute("INSERT INTO Departamento(id, nombre) VALUES (?, ?)", (idD, departamento))
        conn.commit()
    return idD

def insertarMunicipio(municipio):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Municipio")
    existe = False
    idM = 0
    municipio = quitarAcento(municipio)
    municipio = limpiar(municipio)
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

def insertarEmpresa(empresa):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Empresa")
    existe = False
    idE = 0
    empresa = quitarAcento(empresa)
    empresa = limpiar(empresa)
    for row in c.fetchall():
        if empresa == row[1]:
            existe =True
            idE = row[0]
            break
        idE = row[0]+1
    if existe == False:
        c.execute("INSERT INTO Empresa(id, nombre) VALUES (?, ?)", (idE, empresa))
        conn.commit()
    return idE

def insertarCuenca(cuenca):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Cuenca")
    existe = False
    idC = 0
    cuenca = quitarAcento(cuenca)
    cuenca = limpiar(cuenca)
    for row in c.fetchall():
        if cuenca == row[1]:
            existe =True
            idC = row[0]
            break
        idC = row[0]+1
    if existe == False:
        c.execute("INSERT INTO Cuenca(id, nombre) VALUES (?, ?)", (idC, cuenca))
        conn.commit()
    return idC

def insertarContrato(contrato):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Contrato")
    existe = False
    idCo = 0
    contrato = quitarAcento(contrato)
    contrato = limpiar(contrato)
    for row in c.fetchall():
        if contrato == row[1]:
            existe =True
            idCo = row[0]
            break
        idCo = row[0]+1
    if existe == False:
        c.execute("INSERT INTO Contrato(id, nombre) VALUES (?, ?)", (idCo, contrato))
        conn.commit()
    return idCo

def insertarData(arregloMes):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Data")
    rows = c.fetchall()
    idDa = len(rows)+1
    c.execute("INSERT INTO Data(id, enero, febrero, marzo, abril, mayo, junio, julio, agosto, septiembre, octubre, noviembre, diciembre) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (idDa, arregloMes[0], arregloMes[1], arregloMes[2], arregloMes[3], arregloMes[4], arregloMes[5], arregloMes[6], arregloMes[7], arregloMes[8], arregloMes[9], arregloMes[10], arregloMes[11]))
    conn.commit()
    return idDa

def insertarCampoCrudo(idD, idM, idE, idC, idCo, idDa, campo, year):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM Data")
    rows = c.fetchall()
    idCamp = len(rows)
    campo = quitarAcento(campo)
    campo = limpiar(campo)
    c.execute("INSERT INTO CampoCrudo(id, campo, year, departamentoID, municipioID, operadoraID, cuencaID, contradoID, datosID ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (idCamp, campo, year, idD, idM, idE, idC, idCo, idDa))
    conn.commit()
    
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

def encontrarIDBorde():
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE campo = -1", (columna, valor))
    rows = c.fetchall()
    return rows

def getCampoCrudoByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE ? = ?", (columna, valor))
    rows = c.fetchall()
    return rows
              
def getDataByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE ? = ?", (columna, valor))
    rows = c.fetchall()
    return rows
              
def getContratoByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE ? = ?", (columna, valor))
    rows = c.fetchall()
    return rows
def getCuencaByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE ? = ?", (columna, valor))
    rows = c.fetchall()
    return rows
              
def getEmpresaByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE ? = ?", (columna, valor))
    rows = c.fetchall()
    return rows
              
def getMunicipioByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE ? = ?", (columna, valor))
    rows = c.fetchall()
    return rows
              
def getDepartamentoByValue(columna, valor):
    conn = sqlite3.connect("hackathon.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CampoCrudo WHERE ? = ?", (columna, valor))
    rows = c.fetchall()
    return rows