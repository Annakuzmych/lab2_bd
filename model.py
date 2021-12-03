import time

from config import *
import psycopg2

cursor = None
connection = None


def connect():
    try:
        global connection, cursor
        connection = psycopg2.connect(user=user, password=password, host='localhost', port="5432", database=database)

        cursor = connection.cursor()

    except (Exception, psycopg2.Error) as error:
        print("Can't connect to DB", error)
        disconnect()


def disconnect():
    if connection:
        cursor.close()
        connection.close()
        print("Successfully disconnected from DB")
    else:
        print("Can't disconnect")


def insert(num: int, col: list) -> bool:
    if (cursor is None) and (connection is None):
        return False
    try:
        match num:
            case 1:
                cursor.execute("""INSERT INTO PUBLIC."Direction" (number_med, id_doctor, id_specialist, data) \
                          VALUES (%s, %s, %s, %s)""", col)
            case 2:
                cursor.execute("""INSERT INTO PUBLIC."Doctor" (id_specialist, name_doc, phone_num) \
                          VALUES (%s, %s, %s)""", col)
            case 3:
                cursor.execute("""INSERT INTO PUBLIC."Hospital" (name, address, phone) \
                          VALUES (%s, %s, %s)""", col)
            case 4:
                cursor.execute("""INSERT INTO PUBLIC."Hospital_Doctor" (id_doctor, id) \
                          VALUES (%s, %s)""", col)
            case 5:
                cursor.execute("""INSERT INTO PUBLIC."Patient" (name) \
                          VALUES (%s)""", col)
            case 6:
                cursor.execute("""INSERT INTO PUBLIC."Specialist" (cabinet, specialization) \
                                      VALUES (%s, %s)""", col)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Can't insert into table", error)
        cursor.execute('rollback')
        return False
    return True


def select(num: int, quantity: int = 100, offset: int = 0, id: str = "") -> list:
    if (cursor is None) and (connection is None):
        return []
    try:
        match num:
            case 1:
                if id:
                    cursor.execute("""SELECT * FROM public."Direction" WHERE number=%s""", id)
                else:
                    cursor.execute("""SELECT * FROM public."Direction" ORDER BY number ASC limit %s offset %s""",
                                   (quantity, offset,))
            case 2:
                if id:
                    cursor.execute("""SELECT * FROM public."Doctor" WHERE id_doctor=%s""", id)
                else:
                    cursor.execute(
                        """SELECT * FROM public."Doctor" ORDER BY id_doctor ASC limit %s offset %s""",
                        (quantity, offset,))
            case 3:
                if id:
                    cursor.execute("""SELECT * FROM public."Hospital" WHERE id=%s""", id)
                else:
                    cursor.execute("""SELECT * FROM public."Hospital" ORDER BY id ASC limit %s offset %s""",
                                   (quantity, offset,))
            case 4:
                if id:
                    cursor.execute("""SELECT * FROM public."Hospital_Doctor" WHERE id_tab=%s""", id)
                else:
                    cursor.execute(
                        """SELECT * FROM public."Hospital_Doctor" ORDER BY id_tab ASC limit %s offset %s""",
                        (quantity, offset,))
            case 5:
                if id:
                    cursor.execute("""SELECT * FROM public."Patient" WHERE number_med=%s""", id)
                else:
                    cursor.execute("""SELECT * FROM public."Patient" ORDER BY number_med ASC limit %s offset %s""",
                                   (quantity, offset,))
            case 6:
                if id:
                    cursor.execute("""SELECT * FROM public."Specialist" WHERE id_specialist=%s""", id)
                else:
                    cursor.execute("""SELECT * FROM public."Specialist" ORDER BY id_specialist ASC limit %s offset %s""",
                                   (quantity, offset,))
        return cursor.fetchall()
    except(Exception, psycopg2.Error) as error:
        print(f"Can't select from a table {num}, with {id=}\n", error)
        return []


def delete(num: int, id: str) -> bool:
    if (cursor is None) and (connection is None):
        return False
    try:
        match num:
            case 1:
                cursor.execute("""DELETE FROM public."Direction" WHERE number=%s""", (id,))
            case 2:
                cursor.execute("""DELETE FROM public."Doctor" WHERE id_doctor like %s""", (id,))
            case 3:
                cursor.execute("""DELETE FROM public."Hospital" WHERE id like %s""", (id,))
            case 4:
                cursor.execute("""DELETE FROM public."Hospital_Doctor" WHERE id_tab = %s""", (id,))
            case 5:
                cursor.execute("""DELETE FROM public."Patient" WHERE number_med = %s""", (id,))
            case 6:
                cursor.execute("""DELETE FROM public."Specialist" WHERE id_specialist = %s""", (id,))
        connection.commit()
        return True
    except(Exception, psycopg2.Error) as error:
        print(f"Can't delete from a table {num}, {id=}\n", error)
        cursor.execute('rollback')
        return False


def update(num: int, col: list, id: int) -> bool:
    if (cursor is None) and (connection is None):
        return False
    try:
        match num:
            case 1:
                cursor.execute(
                    """UPDATE PUBLIC."Direction" SET id_doctor = %s, id_specialist = %s, number_med = %s, data = %s WHERE number=%s;""",
                    (*col, id,))
            case 2:
                cursor.execute(
                    """UPDATE PUBLIC."Doctor" SET id_specialist = %s, name_doc = %s, phone_num = %s WHERE id_doctor = %s;""",
                    (*col, id,))
            case 3:
                cursor.execute(
                    """UPDATE PUBLIC."Hospital" SET name = %s, address = %s, phone = %s  WHERE id = %s;""",
                    (*col, id,))
            case 4:
                cursor.execute(
                    """UPDATE PUBLIC."Hospital_Doctor" SET id_doctor = %s, id = %s WHERE id_tab = %s;""",
                    (*col, id,))
            case 5:
                cursor.execute(
                    """UPDATE PUBLIC."Patient" SET name = %s WHERE number_med = %s;""",
                    (*col, id,))
            case 6:
                cursor.execute(
                    """UPDATE PUBLIC."Specialist" SET cabinet = %s, specialization = %s WHERE id_specialist = %s;""",
                    (*col, id,))
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print(f"Can't update the table row = {id}, {col}\n", error)
        cursor.execute('rollback')
        return False
    return True


def generate(num: int, quant: int):
    if (cursor is None) and (connection is None):
        return False
    try:
        for i in range(quant):
            match num:
                case 1:
                    cursor.execute(
                        """INSERT INTO public."Direction"(data, number_med, id_doctor, id_specialist) Select random_string(10), number_med, id_doctor, public."Specialist".id_specialist From public."Doctor" cross join public."Specialist" cross join public."Patient" order by random() limit 1;""")
                case 2:
                    cursor.execute(
                        """INSERT INTO public."Doctor"(name_doc, phone_num, id_specialist) Select random_string(8), random_between(1000,100000), id_specialist From public."Specialist" order by random() limit 1;  """)
                case 3:
                    cursor.execute(
                        """INSERT INTO public."Hospital"(name, address, phone) VALUES (random_string(10), random_string(10), random_between(1000,100000));""")
                case 4:
                    cursor.execute(
                        """INSERT INTO public."Hospital_Doctor"(id_doctor, id) Select id_doctor, id From public."Hospital" cross join public."Doctor" order by random() limit 1;""")
                case 5:
                    cursor.execute(
                        """INSERT INTO public."Patient"(name) Select random_string(12);""")
                case 6:
                    cursor.execute(
                        """INSERT INTO public."Specialist"(cabinet, specialization) Select random_between(10,1000), random_string(10);""")
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print(f"Can't insert in the table row = {num}\n", error)
        cursor.execute('rollback')
        return False
    return True
5


def search(tables: list[int], key: str, expression: str):
    sql_query = "select * from "
    match tables[0]:
        case 1:
            sql_query += 'public."Direction" as first'
        case 2:
            sql_query += 'public."Doctor" as first'
        case 3:
            sql_query += 'public."Hospital" as first'
        case 4:
            sql_query += 'public."Hospital_Doctor" as first'
        case 5:
            sql_query += 'public."Patient" as first'
        case 6:
            sql_query += 'public."Specialist" as first'

    sql_query += ' inner join '
    match tables[1]:
        case 1:
            sql_query += 'public."Direction" as second'
        case 2:
            sql_query += 'public."Doctor" as second'
        case 3:
            sql_query += 'public."Hospital" as second'
        case 4:
            sql_query += 'public."Hospital_Doctor" as second'
        case 5:
            sql_query += 'public."Patient" as second'
        case 6:
            sql_query += 'public."Specialist" as second'
    sql_query += f' on first.{key} = second.{key} Where {expression}'
    print('SQL QUERY =>', sql_query)
    global cursor
    try:
        timer = time.time_ns()
        cursor.execute(sql_query)
    except (Exception, psycopg2.Error) as error:
        print("Can't execute search\n", error)
        return []
    rows = cursor.fetchall()
    timer = time.time_ns() - timer
    return rows, timer
