from os import listdir, remove, popen
from datetime import datetime
from config import credenciales, bases_de_datos

def comprimirBackups():
    # Recorremos los archivos dentro de la carpeta backups
    for backup in listdir('backups/'):
        print('Backups: %s' % backup)
        nombreBackupComprimido = '%s.tar.bz2' % backup

        #cmd = 'tar zcf %s %s' % ('backups_comprimidos/'+nombreBackupComprimido, 'backups/'+backup)
        cmd = '	tar -c %s | bzip2 > %s' % ('backups/'+backup, 'backups_comprimidos/'+nombreBackupComprimido) # Compresion mas fuerte

        resCmd = popen(cmd).read()

        print('Respuesta al comando de comprimir: %s' % resCmd)


def crearBackup(db):
    rutaBackup = 'backups/backup_%s_%s.sql' % (db, datetime.now().strftime('%Y_%m_%d-%H_%M_%S'))

    # Creamos el comando para hacer el backup con las credenciales y la base de datos respectiva
    cmd = 'mysqldump --user=%s --password=%s %s --lock-tables=false > %s' % \
          (credenciales['usuario'], credenciales['pass'], db, rutaBackup)

    resCmd = popen(cmd).read()  # Ejecutamos el comando y leemos la respuesta

    print('Respuest al comando de crear backup: %s' % resCmd)

def main():
    # Recorremos las bases de datos para hacer el backup de cada una
    for db in bases_de_datos:
        crearBackup(db)

    comprimirBackups()

if __name__ == '__main__':
    main()
