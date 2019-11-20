import os
from datetime import datetime
from config import credenciales, bases_de_datos

def crearBackup(db):
    rutaBackup = 'backups/backup_%s_%s.sql' % (db, datetime.now().strftime('%Y_%m_%d-%H_%M_%S'))

    # Creamos el comando para hacer el backup con las credenciales y la base de datos respectiva
    cmd = 'mysqldump --user=%s --password=%s %s --lock-tables=false > %s' % \
          (credenciales['usuario'], credenciales['pass'], db, rutaBackup)

    resCmd = os.popen(cmd).read()  # Ejecutamos el comando y leemos la respuesta

    print('Respuest al comando: %s' % resCmd)

def main():
    # Recorremos las bases de datos para hacer el backup de cada una
    for db in bases_de_datos:
        crearBackup(db)


if __name__ == '__main__':
    main()
