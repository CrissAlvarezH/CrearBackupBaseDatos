from os import listdir, remove, popen
from datetime import datetime
from config import credenciales, bases_de_datos
import logging
import subprocess

logging.basicConfig(
    filename='logs/logs.txt',
    filemode='a',
    level=logging.DEBUG,
    format='%(asctime)s > %(levelname)s : %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def comprimirBackups():
    logging.info('[INICIA] COMPRESIÓN DE LOS ARCHIVOS DE BACKUPS')

    listaBackups = listdir('backups/')

    logging.info('Backups a comprimir: %s' % listaBackups)

    # Recorremos los archivos dentro de la carpeta backups
    for backup in listaBackups:
        nombreBackupComprimido = '%s.tar.bz2' % backup

        #cmd = 'tar zcf %s %s' % ('backups_comprimidos/'+nombreBackupComprimido, 'backups/'+backup)
        cmd = ' tar -c %s | bzip2 > %s' % ('backups/'+backup, 'backups_comprimidos/'+nombreBackupComprimido) # Compresion mas fuerte

        logging.info('Comprimimos: %s  >  %s' % (backup, nombreBackupComprimido) )

        try:
            proceso = subprocess.Popen(cmd, shell=True)
            proceso.wait() # Esperamos a que el proceso termine antes de pasar a la siguiente linea
        except Exception as e:
            logging.error('Al ejecutar comando para comprimir, Error: %s' % e)

        logging.info('Borramos: %s' % backup)

        remove('backups/'+backup) # Borramos el archivo original despues de estar comprimido


    logging.info('[TERMINA] COMPRESIÓN DE LOS ARCHIVOS DE BACKUPS')


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

    logging.info(' --------------------------- PROCESO TERMINADO -----------------------------')
    logging.info(' ===========================================================================')
    logging.info('') # Dos saltos de linea para dejar espacio
    logging.info('')

if __name__ == '__main__':
    main()
