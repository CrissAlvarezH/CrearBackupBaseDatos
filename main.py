from os import listdir, remove, popen
from datetime import datetime
from config import credenciales, bases_de_datos, space
import logging
import subprocess
import boto3

logging.basicConfig(
    filename='logs/logs.txt',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s > %(levelname)s : %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)

def enviarBackupsComprimidosConBoto3():
    logging.info('[INICIA] ENVIO DE LOS BACKUPS COMPRIMIDOS')

    listaComprimidos = listdir('backups_comprimidos/')

    for archivo in listaComprimidos:
        sessionS3 = boto3.session.Session()

        s3Client = sessionS3.client(
            service_name='s3',
            region_name= space['region_name'],
            aws_access_key_id= space['aws_access_key_id'],
            aws_secret_access_key= space['aws_secret_access_key'],
            endpoint_url= space['endpoint_url']
        )

        try:

            logging.info('Enviamos archivo:    %s' % archivo)

            with open('backups_comprimidos/'+archivo, 'rb') as data:
                s3Client.upload_fileobj(
                    data,  # Archivo
                    'copiasdeseguridad',  # Nombre del Space
                    archivo #  Nombre del archivo en el Space cuando suba
                )

            logging.info('Borramos archivo:    %s' % archivo)

            remove('backups_comprimidos/' + archivo)  # Borramos el archivo una vez enviado

        except Exception as e:
            logging.error('Error al enviar el archivo %s' % archivo)

    logging.info('[FIN] ENVIO DE LOS BACKUPS COMPRIMIDOS')

def comprimirBackups():
    logging.info('[INICIA] COMPRESIÓN DE LOS ARCHIVOS DE BACKUPS')

    listaBackups = listdir('backups/')

    logging.info('Backups a comprimir: %s' % listaBackups)

    # Recorremos los archivos dentro de la carpeta backups
    for backup in listaBackups:
        nombreBackupComprimido = '%s.tar.bz2' % backup

        #cmd = 'tar zcf %s %s' % ('backups_comprimidos/'+nombreBackupComprimido, 'backups/'+backup)
        cmd = ' tar -c %s | bzip2 > %s' % ('backups/'+backup, 'backups_comprimidos/'+nombreBackupComprimido) # Compresion mas fuerte

        logging.info('Comprimimos:   %s  >  %s' % (backup, nombreBackupComprimido) )

        try:
            proceso = subprocess.Popen(cmd, shell=True)
            proceso.wait() # Esperamos a que el proceso termine antes de pasar a la siguiente linea
        except Exception as e:
            logging.error('Al ejecutar comando para comprimir, Error: %s' % e)

        logging.info('Borramos:      %s' % backup)

        remove('backups/'+backup) # Borramos el archivo original despues de estar comprimido


    logging.info('[TERMINA] COMPRESIÓN DE LOS ARCHIVOS DE BACKUPS')
    logging.info('')


def crearBackup(db):
    logging.info('[INICIA] BACKUP DE LA BASE DE DATOS %s' % db)

    rutaBackup = 'backups/backup_%s_%s.sql' % (db, datetime.now().strftime('%Y_%m_%d-%H_%M_%S'))

    # Creamos el comando para hacer el backup con las credenciales y la base de datos respectiva
    cmd = 'mysqldump --user=%s --password=%s %s --lock-tables=false > %s' % \
          (credenciales['usuario'], credenciales['pass'], db, rutaBackup)

    try:
        proceso = subprocess.Popen(cmd, shell=True) # Ejecutamos el comando
        proceso.wait() # Esperamos a que el proceso termine antes de pasar a la siguiente linea
    except Exception as e:
        logging.error('Al momento de ejecutar comando de mysqldump, Error: %s' % e)

    logging.info('[TERMINA] BACKUP DE LA BASE DE DATOS %s' % db)
    logging.info('')

def main():
    logging.info(' ===========================================================================')
    logging.info(' --------------------------- PROCESO INICIADO -----------------------------')

    # Recorremos las bases de datos para hacer el backup de cada una
    for db in bases_de_datos:
        crearBackup(db)

    comprimirBackups()

    enviarBackupsComprimidosConBoto3()

    logging.info(' --------------------------- PROCESO TERMINADO -----------------------------')
    logging.info(' ===========================================================================')
    logging.info('') # Dos saltos de linea para dejar espacio
    logging.info('')

if __name__ == '__main__':
    main()
