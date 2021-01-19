#!/usr/bin/python
#*****************************************************************************************#
# Nombre      : callcicd.sh                                                               #
#                                                                                         #
# Descripcion : Genera un archivo csv de las solicitudes de llamadas desde viajes.        #
#                                                                                         #
# Autores     : Christian Calderon / Cristian Quintana / Felipe Cruz                      #
# Empresa     : TCS                                                                       #
# Sintaxis    : ./callcicd.sh ruta/proyecto nombre.jar                                    #
# Fecha       : 19/01/2021                                                                #
#*****************************************************************************************#

#*****************************************************************************************#
# Definicion de variables
#*****************************************************************************************#
import os;
import sys;
from subprocess import call;
import logging;
import time;
start = time.time()
log = logging.getLogger("logging_tryout2")

#*****************************************************************************************#
# Logger Config
#*****************************************************************************************#
def logger_config():
   
   log.setLevel(logging.DEBUG)
   
   # create console handler and set level to debug
   ch = logging.StreamHandler()
   ch.setLevel(logging.DEBUG)
   
   # create formatter
   formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
   
   # add formatter to ch
   ch.setFormatter(formatter)
   
   # add ch to logger
   log.addHandler(ch)

#*****************************************************************************************#
# Ejecutar Repocesar
#*****************************************************************************************#
def call_compile(args):
    #Se ubica en la carpeta para procesar
    log.info("Compilando el proyecto")
    ruta = args[0]
    cmd = "cd "+ruta + " && mvn clean package -DskipTests"
    #Compila el proyecto
    try:
        execution = call(cmd, shell=True)
        log.info("Compile execution: "+str(execution))
    except OSError as e:
        log.error(e)
    return execution
#*****************************************************************************************#
# Spark submit activos 1
#*****************************************************************************************#
def call_install(args):
    #Proceso para instalacion
    #Copia el proyecto compilado a hadoop
    try:
        cmd = "hdfs dfs -put -f "+args[0]+"/target/"+args[1]+" hdfs://10.128.0.3/bancochile/gdd/jar/"+args[1]
        execution = call(cmd, shell=True)
    except OSError as e:
        log.error(e)
    log.info("Install execution: "+str(execution))
#*****************************************************************************************#
#  Funciones de apoyo (Definiciones globales a utilizar )
#*****************************************************************************************#
# def printlog():
#      echo "$@" >> ${DIRLOG}/${FTPLOG}

#*****************************************************************************************#
# Programa principal
#*****************************************************************************************#
def ayudallamada():
      print("Para llamar al programa se debe utilizar parametros")
      print("Asi paramametro 1 : ruta de compilacion, debe ser la carpeta donde se ejecuta mvn")
      print("parametro 2: jar a compilar")
      print("Ejemplo:")
      print(" > callcicd /PoC-ChC-repo/pocbch/cl.bancochile.fdd.pocmod.master.cmf fdd.master.cmf.pocmod-1.0.0.jar")
      print(" ---> en este caso se compila el proyecto y se copia el jar a hdfs")
#*****************************************************************************************#
# Programa principal
#*****************************************************************************************#
if __name__ == "__main__":
     logger_config()
     log.info("Inicio Proceso CI / CD")
     args = sys.argv[1:]
     # Si no hay dos parametros se muestra la ayuda y se termina la ejecucion
     contpass=1
     if len(args) < 2 :
         print "params:", args, "-"
         ayudallamada()
     else:
         if len(args[0]) > 5:
             contpass = call_compile(args)
         if len(args[1]) > 5 and contpass==0:
             call_install(args)
     log.info("Fin Proceso")
     ready = time.time()
     elapsed = ready - start
     log.info("Time elapsed: "+time.strftime('%H:%M:%S', time.gmtime(elapsed)))
