#!/usr/bin/python
#*****************************************************************************************#
# Nombre      : callprocess.sh                                                            #
#                                                                                         #
# Descripcion : Genera un archivo csv de las solicitudes de llamadas desde viajes.        #
#                                                                                         #
# Autores     : Christian Calderon / Cristian Quintana / Felipe Cruz                      #
# Empresa     : TCS                                                                       #
# Sintaxis    : sh callprocess.sh                                                          #
# Fecha       : 16/01/2021                                                                #
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
def call_python():
    #Limpia temporales generados en procesos anteriores
    log.info("Limpiando temporales")
    cmd = "rm -r /home/tcs_bancochile/PoC-ChC-repo/pocbch/shell/outputInterfaces/*"
    call(cmd, shell=True)
    #Procesa Archivos excell
    try:
        log.info("Generando archivos temporales")
        call("./callpy.sh")
    except OSError as e:
        log.error(e)
    
    #Copia archivos generados a HDFS
    cmd = "hdfs dfs -rm -r hdfs://10.128.0.3/bancochile/gdd/data/raw/cmf"
    call(cmd, shell=True)
    cmd = "hdfs dfs -mkdir /bancochile/gdd/data/raw/cmf"
    call(cmd, shell=True)
    try:
        cmd = "hdfs dfs -put -f /home/tcs_bancochile/PoC-ChC-repo/pocbch/shell/outputInterfaces/* hdfs://10.128.0.3/bancochile/gdd/data/raw/cmf"
        log.info("Copiando archivos temporales a HDFS")
        call(cmd, shell=True)
    except OSError as e:
        log.error(e)

#*****************************************************************************************#
# Spark submit activos 1
#*****************************************************************************************#
def call_activos1():
    log.info("Procesando activos 1")
    cmd = "spark-submit --jars hdfs://10.128.0.3/bancochile/gdd/jar/huemul-bigdatagovernance-2.6.2.jar,hdfs://10.128.0.3/bancochile/gdd/jar/huemul-sql-decode-1.0.jar,hdfs://10.128.0.3/bancochile/gdd/jar/fdd.todos.settings-1.4.0.jar,hdfs://10.128.0.3/bancochile/gdd/jar/ojdbc7-12.1.0.1.jar,hdfs://10.128.0.3/bancochile/gdd/jar/fdd.todos.cipher-1.0.1.jar,hdfs://10.128.0.3/bancochile/gdd/jar/hive-contrib-2.3.5.jar,hdfs://10.128.0.3/bancochile/gdd/jar/hive-warehouse-connector_2.11-1.0.0.3.1.0.0-78.jar --class cl.bancochile.fdd.master.cmf.process.process_activos1_mes hdfs://10.128.0.3/bancochile/gdd/jar/fdd.master.cmf-1.0.0.jar Environment=production,RegisterInControl=false,TestPlanMode=false,year=2019,month=09,day=01,num_registros_min=1,num_registros_max=10000000,params='',krb5_env=false --deploy-mode cluster --master yarn-cluster --executor-memory 2GB --driver-memory 2GB --num-executors 2 --total-executor-cores 4 --files /opt/hive/conf/hive-site.xml"
    try:
        call(cmd, shell=True)
    except OSError as e:
        log.error(e)

#*****************************************************************************************#
# Spark submit activos 2
#*****************************************************************************************#


def call_activos2():
    log.info("Procesando activos 2")
    cmd = "spark-submit --jars hdfs://10.128.0.3/bancochile/gdd/jar/huemul-bigdatagovernance-2.6.2.jar,hdfs://10.128.0.3/bancochile/gdd/jar/huemul-sql-decode-1.0.jar,hdfs://10.128.0.3/bancochile/gdd/jar/fdd.todos.settings-1.4.0.jar,hdfs://10.128.0.3/bancochile/gdd/jar/ojdbc7-12.1.0.1.jar,hdfs://10.128.0.3/bancochile/gdd/jar/fdd.todos.cipher-1.0.1.jar,hdfs://10.128.0.3/bancochile/gdd/jar/hive-contrib-2.3.5.jar,hdfs://10.128.0.3/bancochile/gdd/jar/hive-warehouse-connector_2.11-1.0.0.3.1.0.0-78.jar --class cl.bancochile.fdd.master.cmf.process.process_activos2_mes  hdfs://10.128.0.3/bancochile/gdd/jar/fdd.master.cmf-1.0.0.jar Environment=production,RegisterInControl=false,TestPlanMode=false,year=2019,month=09,day=01,num_registros_min=1,num_registros_max=10000000,params='',krb5_env=false --deploy-mode cluster --master yarn-cluster --executor-memory 2GB --driver-memory 2GB --num-executors 2 --total-executor-cores 4 --files /opt/hive/conf/hive-site.xml"
    try:
        call(cmd, shell=True)
    except OSError as e:
        log.error(e)


#*****************************************************************************************#
# Spark submit cmf operational reporting
#*****************************************************************************************#

def call_op_repo():
    log.info("Procesando Reporte")
    cmd = "spark-submit --jars hdfs://10.128.0.3/bancochile/gdd/jar/huemul-bigdatagovernance-2.6.2.jar,hdfs://10.128.0.3/bancochile/gdd/jar/huemul-sql-decode-1.0.jar,hdfs://10.128.0.3/bancochile/gdd/jar/fdd.todos.settings-1.4.0.jar,hdfs://10.128.0.3/bancochile/gdd/jar/ojdbc7-12.1.0.1.jar,hdfs://10.128.0.3/bancochile/gdd/jar/fdd.todos.cipher-1.0.1.jar,hdfs://10.128.0.3/bancochile/gdd/jar/hive-contrib-2.3.5.jar,hdfs://10.128.0.3/bancochile/gdd/jar/hive-warehouse-connector_2.11-1.0.0.3.1.0.0-78.jar --class cl.bancochile.fdd.dim.sgt.process.process_cmf_operational_reporting  hdfs://10.128.0.3/bancochile/gdd/jar/fdd.dim.sgt-1.0.0.jar Environment=production,RegisterInControl=false,TestPlanMode=false,year=2019,month=09,day=01,num_registros_min=1,num_registros_max=10000000,params='',krb5_env=false --deploy-mode cluster --master yarn-cluster --executor-memory 2GB --driver-memory 2GB --num-executors 2 --total-executor-cores 4 --files /opt/hive/conf/hive-site.xml"
    try:
        call(cmd, shell=True)
    except OSError as e:
        log.error(e)

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
      print("Asi paramametro 1 : indica si se debe reporocesar con una s")
      print("parametro 2: indica cual de las llamadas a sparc submit se deben realizar")
      print("Ejemplo:")
      print(" > callprocess s 1")
      print(" ---> en este caso se reprocesa y se invoca al primer script")

#*****************************************************************************************#
# Programa principal
#*****************************************************************************************#
if __name__ == "__main__":
     logger_config()
     log.info("Inicio Proceso")
     args = sys.argv[1:]
     # Si no hay dos parametros se muestra la ayuda y se termina la ejecucion
     if len(args) < 2 :
         print "params:", args, "-"
         ayudallamada()
     else:
         # Si el primer parametro es una s llamar a Python para reprocesar
         if args[0] == "s":
             call_python()
             # Si el segundo parametro es un 1 llamar a Activos 1
         if args[1] == "1":
             call_activos1()
             call_op_repo()
         # Si el segundo parametro es un 2 llamar a Activos 2
         elif args[1] == "2":
             call_activos2()
             call_op_repo()
         # Si el segundo parametro es un 3 llamar a Activos 1 y 2
         elif args[1] == "3":
             call_activos1()
             call_activos2()
             call_op_repo()
     log.info("Fin Proceso")
     done = time.time()
     elapsed = done - start
     log.info("Time elapsed: "+time.strftime('%H:%M:%S', time.gmtime(elapsed)))
