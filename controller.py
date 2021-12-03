import model as db


def select_table(insert=False) -> int:
    if insert:
        print('Select the table to insert data, from 0-6')
    else:
        print('Select table, from 0-6')
    print('1. Direction\n2. Doctor\n3. Hospital\n4. Hospital_Doctor\n5. Patient\n6. Specialist\n0. Return to main menu')
    num = int(input())
    if num > 6:
        print('Incorrect number')
        select_table()
    return num


def input_values(num: int) -> list[str]:
    print("Insert value seperated by comma")
    values = ""
    match num:
        case 1:
            values = input('Input: number_med->integer, id_doctor->integer, id_specialist->integer, data->char[50]\n')
        case 2:
            values = input('Input: id_specialist->integer, name_doc->char[50], phone_num->integer\n')
        case 3:
            values = input('Input: name->char[50], address->char[50], phone->integer\n')
        case 4:
            values = input('Input: id_doctor->integer, id->integer\n')
        case 5:
            values = input('Input: name->char[50]\n')
        case 6:
            values = input('Input: cabinet->integer, specialization->char[50]\n')

    return values.split(',')


def insert_option(num: int):
    if num == 0:
        return
    # if num == 4:
    #     print("Can't insert in this table")
    #     return
    columns = [column.strip() for column in input_values(num)]
    if db.insert(num, columns):
        print("Inserted successfully")
    else:
        print("Can't insert")


def pretty_print(nums: list[int], rows):
    d = []
    for num in nums:
        match num:
            case 1:
                d += ['number', 'number_med', 'id_doctor', 'id_specialist', 'data']
            case 2:
                d += ['id_doctor', 'id_specialist', 'name_doc', 'phone_num']
            case 3:
                d += ['id', 'name', 'address', 'phone']
            case 4:
                d += ['id_tab', 'id', 'id_doctor']
            case 5:
                d += ['number_med', 'name']
            case 6:
                d += ['id_specialist', 'cabinet', 'specialization']

    names = []
    lengths = []
    rules = []
    rls = []
    for dd in d:
        names.append(dd)
        lengths.append(len(dd))
    for col in range(len(lengths)):
        for row in rows:
            rls.append(3 if type(row[col]) is not str else len(row[col]))
        lengths[col] = max([lengths[col]] + rls)
        rules.append("-" * lengths[col])
    format = " ".join(["%%-%ss" % l for l in lengths])
    result = [format % tuple(names), format % tuple(rules)]
    for row in rows:
        result.append(format % row)
    return "\n".join(result) + '\n'


def print_option(num: int, id: str = "", quantity: int = 0, offset: int = 0) -> str:
    if num == 0:
        return ""
    if not id:
        if not quantity:
            quantity = int(input('Input quantity of rows to print: '))
        rows = db.select(num, quantity, offset)
    else:
        rows = db.select(num, id=id)
    return pretty_print([num], rows)


def delete_option(num: int):
    if num == 0:
        return
    id = input("Enter id of row you want to delete\n'p' -> print rows\n'r' -> return to menu\n")
    if id == 'r':
        return
    elif id == 'p':
        offset = 0
        while True:
            print(print_option(num, quantity=10, offset=offset))
            id = input(
                "Enter id of row you want to delete\n'n' -> next 10 rows\n'b' -> previous 10 rows\n'r' -> return to "
                "menu\n")
            if id == 'r':
                return
            elif id == 'n':
                offset += 10
            elif id == 'b':
                offset -= 10
            else:
                break
    # id = inpt
    if db.delete(num, id):
        print('Deleted successfully')
    else:
        print("Can't delete")


def edit_option(num: int):
    if num == 0:
        return
    id = input("Enter id of row you want to change\n'p' -> print rows\n'r' -> return to menu\n")
    if id == 'r':
        return
    elif id == 'p':
        offset = 0
        while True:
            print(print_option(num, quantity=10, offset=offset))
            id = input(
                "Enter id of row you want to change\n'n' -> next 10 rows\n'b' -> previous 10 rows\n'r' -> return to menu\n")
            if id == 'r':
                return
            elif id == 'n':
                offset += 10
            elif id == 'b':
                offset -= 10
            else:
                break
    print_option(num, id)
    print("If you don't want to change column -> write as it was")
    columns = input_values(num)
    print('Updated successfully') if db.update(num, columns, int(id)) else print("Can't update")


def generator_option():
    quant = int(input('Input the quantity to generate: '))
    print("1. For whole db\n2. For one table")
    option = input("Select 1-2: ")
    if option == '1':
        generate_all(quant)
    elif option == '2':
        num = select_table()
        print(f"Generated and inserted into {num} successfully") if db.generate(num, quant) else print(
            f"Can't generate and insert into {num}")


def search_option():
    tables = []
    print('Select first table')
    tables.append(select_table())
    print('Select second table')
    tables.append(select_table())
    key = input('Input the connecting key: ')
    expressions = []
    print('Input expressions Use "first" and "second" to address to the table attributes, if with string use like')
    # while True:
    #     ex = input('')
    #     if ex == '0':
    #         break
    #     expressions.append(ex)
    expressions = input()
    rows = db.search(tables, key, expressions)
    print(pretty_print(tables, rows[0]))
    print('Time to execute', rows[1] / 1000, ' ms')


def generate_all(quant: int):
    print("Generated and inserted into Patient successfully") if db.generate(5, quant) else print(
        "Can't generate and insert into Patient")
    print("Generated and inserted into Specialist successfully") if db.generate(6, quant) else print(
        "Can't generate and insert into Specialist")
    print("Generated and inserted into Hostital successfully") if db.generate(3, quant) else print(
        "Can't generate and insert into Hostital")
    print("Generated and inserted into Doctor successfully") if db.generate(2, quant) else print(
        "Can't generate and insert into Doctor")
    print("Generated and inserted into Hospital_Doctor successfully") if db.generate(4, quant) else print(
        "Can't generate and insert into Hospital_Doctor")
    print("Generated and inserted into Direction successfully") if db.generate(1, quant) else print(
        "Can't generate and insert into Direction")

def main_select_option():
    while True:
        print('1. Insert data in table')
        print('2. Edit data in table')
        print('3. Delete data from table')
        print('4. Print rows')
        print('5. Generate random data')
        print('6. Search from tables')
        print('0. Exit')

        match input('\tSelect option 0-6: '):
            case '1':
                insert_option(select_table(True))
            case '2':
                edit_option(select_table())
            case '3':
                delete_option(select_table())
            case '4':
                print(print_option(select_table()))
            case '5':
                generator_option()
            case '6':
                search_option()
            case '0':
                return
