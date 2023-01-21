import psycopg2 as pg
import yaml

class Working_with_postgresql():

    def __init__(self):

        with open('db_access.yml', 'r', encoding='utf-8') as file: 
            self.settings = yaml.load(file, Loader=yaml.FullLoader)

    def create_db_structure(self):

        try:
            conn = self.db_connect()

            with conn.cursor() as cursor:
                cursor.execute("""
                            CREATE TABLE IF NOT EXISTS clients (
                                id SERIAL PRIMARY KEY,
                                first_name VARCHAR(50) NOT NULL,
                                last_name VARCHAR(50) NOT NULL,
                                email VARCHAR(255) NOT NULL
                            );""")
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS phones (
                                    id SERIAL PRIMARY KEY,
                                    phone VARCHAR(20) NOT NULL,
                                    client_id INTEGER NOT NULL,
                                    FOREIGN KEY (client_id) REFERENCES clients (id)
                                );""")

            conn.commit()

        except Exception as _ex:
            print("Error while working with PostgreSQL", _ex)
        finally:
            if conn:
                conn.close()
                print("PostgreSQL connection closed")

    def add_client(self, first_name, last_name, email):

        try:
            conn = self.db_connect()
            
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO clients (first_name, last_name, email) VALUES ('{first_name}', '{last_name}', '{email}');")

            conn.commit()
        
        except Exception as _ex:
            print("Error while working with PostgreSQL", _ex)
        finally:
            if conn:
                conn.close()
                print("PostgreSQL connection closed")

    def add_phone(self, client_id, phone):
        
        try:
            conn = self.db_connect()
            
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO phones (client_id, phone) VALUES ({client_id}, '{phone}');")
            
            conn.commit()
        
        except Exception as _ex:
            print("Error while working with PostgreSQL", _ex)
        finally:
            if conn:
                conn.close()
                print("PostgreSQL connection closed")

    def update_client(self, id, first_name, last_name, email):

        try:
            conn = self.db_connect()

            with conn.cursor() as cursor:
                cursor.execute(f"UPDATE clients SET first_name = '{first_name}', last_name = '{last_name}',\
                                email = '{email}' WHERE id = {id};")

            conn.commit()
        
        except Exception as _ex:
            print("Error while working with PostgreSQL", _ex)
        finally:
            if conn:
                conn.close()
                print("PostgreSQL connection closed")

    def delete_phone(self, client_id, phone):
        
        try:
            conn = self.db_connect()
            
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM phones WHERE client_id = {client_id} AND phone = '{phone}';")
            
            conn.commit()
        
        except Exception as _ex:
            print("Error while working with PostgreSQL", _ex)
        finally:
            if conn:
                conn.close()
                print("PostgreSQL connection closed")

    def delete_client(self, client_id):
        
        try:
            conn = self.db_connect()

            with conn.cursor() as cursor:
                cursor.execute(f'DELETE FROM phones WHERE client_id = {client_id}')
                cursor.execute(f"DELETE FROM clients WHERE id = {client_id};")
            
            conn.commit()
        
        except Exception as _ex:
            print("Error while working with PostgreSQL", _ex)
        finally:
            if conn:
                conn.close()
                print("PostgreSQL connection closed")

    def find_client(self, query):
        try:
            conn = self.db_connect()

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM clients \
                        LEFT JOIN phones ON clients.id = phones.client_id\
                        WHERE first_name ILIKE '{query}' \
                        OR last_name ILIKE '{query}' \
                        OR email ILIKE '{query}' \
                        OR phone ILIKE '{query}';")
                result = cursor.fetchall()
        
        except Exception as _ex:
            print("Error while working with PostgreSQL", _ex)
        finally:
            if conn:
                conn.close()
                print("PostgreSQL connection closed")

        return result

    def db_connect(self):
        connect = pg.connect(
            host=self.settings['host'],
            port=self.settings['port'],
            user=self.settings['user'],
            password=self.settings['password'],
            dbname=self.settings['db_name']
        )

        return connect


if __name__ == "__main__":
    work_with_db = Working_with_postgresql()
    while True:
        print('\n1 - Создать базы данных клиентов и их номеров(если они уже соданы, то новая не создасться)\n\
2 - Добавить клиента\n\
3 - Добавить номер телефона клиента\n\
4 - Обновить информацию о клиенте\n\
5 - Удалить номер телефона из базы данных\n\
6 - Удалить клиента базы данных\n\
7 - Найти информацию о клиенте в базе данных\n\
0 - Завершить работу программы')
        action = input('Выберите действие которое необходимо выполнить: ')

        if action == '1':
            work_with_db.create_db_structure()

        elif action == '2':
            first_name = input('Введите имя клиента: ')
            last_name = input('Введите фамилию пользователя: ')
            email = input('Введите email клиента: ')
            work_with_db.add_client(first_name, last_name, email)

        elif action == '3':
            client_id = input('Введите id клиента: ')
            phone = input('Введите номер телефона: ')
            work_with_db.add_phone(client_id, phone)

        elif action == '4':
            client_id = input('Введите id клиента: ')
            first_name = input('Введите имя клиента: ')
            last_name = input('Введите фамилию пользователя: ')
            email = input('Введите email клиента: ')
            work_with_db.update_client(client_id, first_name, last_name, email)

        elif action == '5':
            client_id = input('Введите id клиента: ')
            phone = input('Введите номер телефона: ')
            work_with_db.delete_phone(client_id, phone)

        elif action == '6':
            client_id = input('Введите id клиента: ')
            work_with_db.delete_client(client_id)

        elif action == '7':
            query = input('Введите имя/фамилию/email/номер телефона клиента: ')
            result = work_with_db.find_client(query)
            print(result)

        elif action == '0':
            exit()