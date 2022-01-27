from functools import wraps
from time import time
from datetime import datetime

import pymysql
import argparse
import json
from json2xml import json2xml
from json2xml.utils import readfromstring

from config import local_host, user, password, db_name


def recreate_table_if_needed(cursor):
    """ Create tables """

    cursor.execute("DROP TABLE IF EXISTS students")
    cursor.execute("DROP TABLE IF EXISTS rooms")

    create_table_rooms = """CREATE TABLE `rooms`
                        (room_id INT AUTO_INCREMENT PRIMARY KEY,
                        name varchar(32));"""
    cursor.execute(create_table_rooms)

    create_table_students = """CREATE TABLE `students`
                            (student_id INT AUTO_INCREMENT PRIMARY KEY,
                            birthday DATETIME, 
                            name varchar(32), 
                            sex varchar(32), 
                            room_id INT, 
                            FOREIGN KEY (room_id) REFERENCES rooms(room_id));"""
    cursor.execute(create_table_students)

    index_create_str = "ALTER TABLE students ADD INDEX birthday (birthday)"
    cursor.execute(index_create_str)


def get_rooms(name_of_file):
    with open(name_of_file, "r", encoding="utf8") as read_file:
        rooms = json.load(read_file)

    return rooms


def load_rooms_into_table_from_file(file_rooms, cursor, connection):
    """ Insert values into 'rooms' table """

    rooms = get_rooms(file_rooms)

    for room in rooms:
        query_string = f"INSERT INTO rooms(name) VALUES ('{room['name']}')"

        cursor.execute(query_string)
    connection.commit()

    return rooms


def get_students(name_of_file):
    with open(name_of_file, "r", encoding="utf8") as read_file:
        students = json.load(read_file)

    return students


def load_students_into_table_from_file(file_students, cursor, connection):
    """ Insert values into 'students' table """

    students = get_students(file_students)

    for student in students:
        birthday = datetime.strptime(student['birthday'], "%Y-%m-%dT%H:%M:%S.%f")
        query_string = f"""INSERT INTO students (birthday, name, sex, room_id) 
                       VALUES ('{birthday}', '{student['name']}', '{student['sex']}', {student['room'] + 1})"""

        cursor.execute(query_string)
    connection.commit()

    return students


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print(f'{(te - ts) * 1000.0} millisec')
        return result

    return wrap


def execute_query(sql_query, cursor):
    cursor.execute(sql_query)
    res = cursor.fetchall()

    return res


@timing
def get_rooms_students_count(cursor):
    """ Select rooms list and count students in room """

    query_string = """SELECT rooms.name as 'The room number', count(rooms.room_id) as 'number of students' 
                   FROM rooms INNER JOIN students ON rooms.room_id = students.room_id 
                   GROUP BY rooms.name, rooms.room_id"""

    return execute_query(query_string, cursor)


@timing
def get_five_smallest_age_rooms(cursor):
    """ Select top 5 rooms with the smallest average age of students """

    query_string = """SELECT rooms.name as 'The room number', CAST(AVG(YEAR(NOW()) - YEAR(students.birthday)) as float)
                   as 'Average age of students' 
                   FROM rooms INNER JOIN students ON rooms.room_id = students.room_id 
                   GROUP BY rooms.name 
                   ORDER BY AVG(YEAR(NOW()) - YEAR(students.birthday))
                   LIMIT 5"""

    return execute_query(query_string, cursor)


@timing
def get_five_biggest_rooms_with_age_difference(cursor):
    """ Select top 5 rooms with the biggest age difference among students """

    query_string = """SELECT rooms.name as 'The room number', CAST((MAX(YEAR(NOW()) - YEAR(students.birthday)))
                   - (MIN(YEAR(NOW()) - YEAR(students.birthday))) as float) as 'The biggest age difference' 
                   FROM rooms INNER JOIN students ON rooms.room_id = students.room_id 
                   GROUP BY rooms.name 
                   ORDER BY (MAX(YEAR(NOW()) - YEAR(students.birthday))) - 
                   (MIN(YEAR(NOW()) - YEAR(students.birthday))) DESC 
                   LIMIT 5"""

    return execute_query(query_string, cursor)


@timing
def get_rooms_with_different_sex_of_students(cursor):
    """ Select list of rooms where students of different sexes live"""

    query_string = """SELECT room_id
                   FROM STUDENTS 
                   GROUP BY room_id 
                   HAVING COUNT(DISTINCT sex) > 1 """

    res_query_4 = execute_query(query_string, cursor)

    res_4 = []

    for room in res_query_4:
        res_4.append("Room " + str(room['room_id'] - 1))
    return res_4


def do_task_work(file_students, file_rooms, format):
    try:
        connection = pymysql.connect(
            host=local_host,
            port=3306,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("successfully connected...")

        try:
            with connection.cursor() as cursor:
                recreate_table_if_needed(cursor)

                rooms = load_rooms_into_table_from_file(file_rooms, cursor, connection)
                students = load_students_into_table_from_file(file_students, cursor, connection)

                # Getting tasks results
                task_query_result1 = get_rooms_students_count(cursor)
                task_query_result2 = get_five_smallest_age_rooms(cursor)
                task_query_result3 = get_five_biggest_rooms_with_age_difference(cursor)
                task_query_result4 = get_rooms_with_different_sex_of_students(cursor)

                # adding tasks results to JSON
                json_result = {}
                json_result['task1_result'] = task_query_result1
                json_result['task2_result'] = task_query_result2
                json_result['task3_result'] = task_query_result3
                json_result['task4_result'] = task_query_result4

                # Determining file format and the data that we need to write to file
                results_file_name = ""
                data_to_write = None
                data_in_json = json.dumps(json_result, indent=4)

                format_lower = format.lower()
                if format_lower == "json":
                    # if format is JSON - then just write it
                    results_file_name = "results.json"
                    data_to_write = data_in_json
                elif format_lower == "xml":
                    # if format is XML then convert JSON result to XML
                    data_in_xml = readfromstring(data_in_json)
                    data_to_write = json2xml.Json2xml(data_in_xml).to_xml()
                    results_file_name = "results.xml"

                # Writing results to JSON or XML file
                with open(results_file_name, "w", encoding="utf8") as write_file:
                    write_file.write(data_to_write)

            print("successfully")

        finally:
            connection.close()

    except Exception as e:
        print("Connection refused")
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert json to json or xml")
    parser.add_argument("-r", help="Path to JSON file with rooms")
    parser.add_argument("-s", help="Path to JSON file with students")
    parser.add_argument("-f", help="Format for the output file JSON or XML")
    args = parser.parse_args()

    do_task_work(args.s, args.r, args.f)
