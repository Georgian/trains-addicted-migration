#!/usr/bin/python2.7
# coding: utf-8

"""
This module translates the TA db.sqlite into a friendlier
and queryable database.

Blob data needs to be unserialized from php format and
further parsed for being written into either sqlite or mysql.
"""
__author__ = 'Marius Magureanu <marius@varni.sh>'
__version__ = '0.1'

import sqlite3
import os
import csv
import sys
# import re
from sqlite3 import Error
from serialize_tools import unserialize, loads, dumps

__SQL_CREATE_ORDERS_TABLE__ = """ CREATE TABLE IF NOT EXISTS orders (
                                    orderNo text NOT NULL,
                                    creationDate text,
                                    total real,
                                    productCode text,
                                    title text,
                                    totalPrice real,
                                    productId text,
                                    quantity integer,
                                    firstName text,
                                    lastName text,
                                    phone1 text,
                                    phone2 text,
                                    email text
                                ); """

__SQL_BULK_INSERT_ORDERS__ = """ INSERT INTO ORDERS(orderNo,
                                                creationDate,
                                                total,
                                                productCode,
                                                title,
                                                totalPrice,
                                                productId,
                                                quantity,
                                                firstName,
                                                lastName,
                                                phone1,
                                                phone2,
                                                email)
                                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);"""


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as conn_err:
        print(conn_err)

    return None


def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT orderNo, data, creationDate FROM objects where orderNo!=''")
    rows = cur.fetchall()
    orders_as_dicts = []
    try:
        for row in rows:
            order_info = unserialize(loads(dumps(row[1])))
            order_info["orderNo"] = row[0]
            order_info["creationDate"] = row[2]
            orders_as_dicts.append(order_info)
    except LookupError as lkp_err:
        print(lkp_err)

    return orders_as_dicts


def select_all_products(conn):
    """
    Query all rows in the objects table
    where class is CproductPage.
    """
    # reload(sys)
    # sys.setdefaultencoding('utf8')

    # CATEGORIES =============================================================
    print("   [*] categories")
    cur = conn.cursor()
    cur.execute("select id, data from objects where class='CcategoryPage'")
    rows = cur.fetchall()

    categories = {}
    for row in rows:
        category_meta = unserialize(loads(dumps(row[1])))
        categories[row[0]] = category_meta["title_ro".encode()]

    cur = conn.cursor()
    cur.execute(
        "select objects.productCode, products_categories.categoryId, products_categories.productId "
        "from objects "
        "join products_categories on objects.Id=products_categories.productId "
        "where objects.class='CproductPage'")
    rows = cur.fetchall()

    with open('product_categories.csv', 'w') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "productCode",
                "categoryId",
                "categoryText"])
        writer.writeheader()

        for row in rows:
            product_cats = {}
            if row[1]:
                product_cats["productCode"] = row[0]
                product_cats["categoryId"] = row[1]
                product_cats["categoryText"] = categories[row[1]].decode()
                writer.writerow(product_cats)

    # OPERATORS ===============================================================
    print("   [*] operators")
    cur = conn.cursor()
    cur.execute("select id, data from objects where class='CoperatorPage'")
    rows = cur.fetchall()

    operators = {}
    for row in rows:
        operator_meta = unserialize(loads(dumps(row[1])))
        operators[row[0]] = operator_meta["title_ro".encode()]

    cur = conn.cursor()
    cur.execute(
        "select objects.productCode, products_operators.operatorId, products_operators.productId "
        "from objects "
        "join products_operators on objects.Id=products_operators.productId")
    rows = cur.fetchall()

    with open('product_operators.csv', 'w') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "productCode",
                "operatorId",
                "operatorText"])
        writer.writeheader()

        for row in rows:
            product_ops = {}
            if row[1]:
                product_ops["productCode"] = row[0]
                product_ops["operatorId"] = row[1]
                product_ops["operatorText"] = operators[row[1]].decode()
                writer.writerow(product_ops)

    # EPOQUES ===============================================================
    print("   [*] epoques")
    cur = conn.cursor()
    cur.execute("select id, data from objects where class='CepoquePage'")
    rows = cur.fetchall()

    epoques = {}
    for row in rows:
        epoque_meta = unserialize(loads(dumps(row[1])))
        epoques[row[0]] = epoque_meta["title_ro".encode()]

    cur = conn.cursor()
    cur.execute(
        "select objects.productCode, products_epoques.epoqueId, products_epoques.productId "
        "from objects "
        "join products_epoques on objects.Id=products_epoques.productId")
    rows = cur.fetchall()

    with open('product_epoques.csv', 'w') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "productCode",
                "epoqueId",
                "epoqueText"])
        writer.writeheader()

        for row in rows:
            epoque_ops = {}
            if row[1]:
                epoque_ops["productCode"] = row[0]
                epoque_ops["epoqueId"] = row[1]
                epoque_ops["epoqueText"] = epoques[row[1]].decode()
                writer.writerow(epoque_ops)

    # SCALES ===============================================================
    print("   [*] scales")
    cur = conn.cursor()
    cur.execute("select id, data from objects where class='CscalePage'")
    rows = cur.fetchall()

    scales = {}
    for row in rows:
        scale_meta = unserialize(loads(dumps(row[1])))
        scales[row[0]] = scale_meta["title_ro".encode()]

    cur = conn.cursor()
    cur.execute(
        "select objects.productCode, products_scales.scaleId, products_scales.productId "
        "from objects "
        "join products_scales on objects.Id=products_scales.productId")
    rows = cur.fetchall()

    with open('product_scales.csv', 'w') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "productCode",
                "scaleId",
                "scaleText"])
        writer.writeheader()

        for row in rows:
            scale_ops = {}
            if row[1]:
                scale_ops["productCode"] = row[0]
                scale_ops["scaleId"] = row[1]
                scale_ops["scaleText"] = scales[row[1]].decode()
                writer.writerow(scale_ops)

    # PRODUCERS===================================================
    print("   [*] producers")
    cur = conn.cursor()
    cur.execute("SELECT id, title_ro FROM objects where class='CproducerPage'")

    rows = cur.fetchall()
    producers = {}

    for row in rows:
        producers[row[0]] = row[1]

    # PICTURES=====================================================
    print("   [*] pictures")
    cur = conn.cursor()
    cur.execute("SELECT parentId, data FROM objects where class='Cslideshow'")

    rows = cur.fetchall()

    tmp_pictures = {}
    for row in rows:
        pix_meta = unserialize(loads(dumps(row[1])))
        if "pictures" in pix_meta:
            pix = pix_meta["pictures"]
            tmp_pictures[row[0]] = [
                '/home/trains/public_html/Resources/{0}'.format(p) for p in pix.values()]

    cur = conn.cursor()
    cur.execute("SELECT id, productCode FROM objects where class='CproductPage'")

    rows = cur.fetchall()

    pictures = {}
    for row in rows:
        id = row[0]
        productCode = row[1]
        if id in tmp_pictures:
            pix = tmp_pictures[id]
            pictures[productCode] = pix

    # ==============================================================

    cur = conn.cursor()
    cur.execute(
        "SELECT productCode, title_ro, title_en, salePrice, discountPrice, parentId, creationDate, data "
        "FROM objects "
        "WHERE class='CproductPage'")
    rows = cur.fetchall()

    # these are fields to be extracted from the data blob
    fields = [
        "description_ro",
        "description_en",
        "metaDescription_ro",
        "metaDescription_en",
        "metaKeywords_ro",
        "metaKeywords_en"]

    csv_fields = [
        "productCode",
        "title_ro",
        "title_en",
        "salePrice",
        "discountPrice",
        "producer",
        "mainPicture",
        "extraPictures",
        "creationDate"]
    csv_fields.extend(fields)

    # p = re.compile(r'<.*?>')
    try:

        with open('products.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
            writer.writeheader()

            for row in rows:
                product_info = {
                    "productCode": row[0],
                    "title_ro": row[1],
                    "title_en": row[2],
                    "salePrice": row[3],
                    "discountPrice": row[4],
                    "producer": producers[row[5]]
                }
                pix = pictures.get(row[0], "")
                if pix:
                    product_info["mainPicture"] = pix[0]
                    product_info["extraPictures"] = ', '.join(pix[1:])

                product_info["creationDate"] = row[6]

                out = unserialize(loads(dumps(row[7])))
                for f in fields:
                    field_value = out.get(f, "")
                    product_info[f] = field_value
                    # product_info[f] = p.sub('', field_value).replace('&nbsp;', '')

                writer.writerow(product_info)

    except LookupError as lkp_err:
        print(lkp_err)


def select_all_users(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT id, firstName, lastName, email, creationDate, data FROM objects WHERE class='Cuser'")

    csv_fields = [
        "id",
        "firstName",
        "lastName",
        "email",
        "creationDate",
        "password"]
    rows = cur.fetchall()

    # reload(sys)
    # sys.setdefaultencoding('utf8')

    addresses = []
    contacts = []

    # USERS ============================================
    with open('users.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
        writer.writeheader()

        for row in rows:
            user_info = dict(id=row[0], firstName=row[1], lastName=row[2], email=row[3], creationDate=row[4])
            meta = unserialize(loads(dumps(row[5])))
            user_info["password"] = meta["password".encode()].decode()

            a = meta.get("deliveryAddresses".encode(), {})
            if a:
                for k, add in a.items():
                    unidict = {k.decode(): v.decode() for k, v in add.items()}
                    unidict["userId"] = row[0]
                    addresses.append(unidict)

            c = meta.get("contactPersons".encode(), {})
            if c:
                for k, ct in c.items():
                    print(ct)
                    unidict = {k.decode(): v.decode() for k, v in ct.items()}
                    unidict["userId"] = row[0]
                    contacts.append(unidict)

            writer.writerow(user_info)

    # USER ADDRESSES ==================================
    print("   [*] addresses")
    with open('user_addresses.csv', 'w') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "id",
                "streetAddress",
                "city",
                "district",
                "zipcode"])
        writer.writeheader()

        for addr in addresses:
            writer.writerow(addr)

    # USER CONTACTS ===================================
    print("   [*] contacts")
    with open('user_contacts.csv', 'w') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "id",
                "firstName",
                "lastName",
                "email",
                "phone1",
                "phone2"])
        writer.writeheader()

        for ct in contacts:
            writer.writerow(ct)


def write_to_db(conn, orders):
    """
    Write the orders as unparsed from the blob into the db.
    :param conn: the Connection of the database against which
    the orders will be written.
    :param orders: Dictionary containing the orders to be
    written.
    """

    bulk_orders = []

    for order in orders:
        if "order" in order:
            if "items" in order["order"]:
                for _, item in order["order"]["items"].iteritems():
                    order_line = [
                        order["orderNo"],
                        order["creationDate"],
                        order["order"]["totalPrice"],
                        item["productCode"],
                        item["title"],
                        item["totalPrice"],
                        item["productId"],
                        item["quantity"]
                    ]
                    # total price oitemer the entire order
                    # total price for the specific item in the order
                    if "contactPerson" in order["order"]:
                        order_line.append(
                            order["order"]["contactPerson"]["firstName"])
                        order_line.append(
                            order["order"]["contactPerson"]["lastName"])
                        order_line.append(
                            order["order"]["contactPerson"]["phone1"])
                        order_line.append(
                            order["order"]["contactPerson"]["phone2"])
                        order_line.append(
                            order["order"]["contactPerson"]["email"])
                    else:
                        order_line.append("")
                        order_line.append("")
                        order_line.append("")
                        order_line.append("")
                        order_line.append("")

                    bulk_orders.append(tuple(order_line))

    try:
        conn.text_factory = lambda x: unicode(x, "utf-8", "ignore")
        db_cursor = conn.cursor()
        db_cursor.execute(__SQL_CREATE_ORDERS_TABLE__)
        db_cursor.execute("delete from orders;")
        db_cursor.executemany(__SQL_BULK_INSERT_ORDERS__, bulk_orders)
        conn.commit()
    except Error as cursor_err:
        print(cursor_err)


def extract_orders(database):
    # create a database connection
    in_conn = create_connection(database)
    with in_conn:
        orders = select_all_tasks(in_conn)

        out_conn = create_connection("out.sqlite")
        with out_conn:
            write_to_db(out_conn, orders)


def extract_products(database):
    # create a database connection
    in_conn = create_connection(database)
    with in_conn:
        select_all_products(in_conn)


def extract_users(database):
    # create a database connection
    in_conn = create_connection(database)
    with in_conn:
        select_all_users(in_conn)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments.")
        print("Usage:\n\n./export.py [input-sqlite-file]")
        sys.exit(1)

    database = sys.argv[1]

    if not os.path.isfile(database):
        print("Invalid database file path")
        sys.exit(1)

    export_mode = "products"

    if len(sys.argv) == 3:
        export_mode = sys.argv[2]

    if export_mode == "orders":
        print("Export orders")
        extract_orders(database)
        print("Orders exported to out.sqlite")
    else:
        print("Export started")
        print("[-] Exporting products")
        extract_products(database)
        print("[+] Done with products")
        print("[-] Exporting users")
        extract_users(database)
        print("[+] Done with users")

    print("Done!")
