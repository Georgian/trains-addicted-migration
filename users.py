import csv

import unidecode

import common
from phpserialize import *


def deserialize(db_object):
    return unserialize(loads(dumps(db_object)))


def normalize_string(string):
    return unidecode.unidecode(string).title()


def process_and_export_users(conn):
    db_rows = common.fetch_all(conn, "SELECT id, data, email, lastName, firstName, creationDate "
                                     "FROM objects "
                                     "WHERE class = 'Cuser'"
                                     "ORDER BY creationDate DESC")

    main_file_headers = ["email", "firstname", "lastname", "password_hash", "created_at",
                         "group_id", "store_id", "website_id", "_website"]

    address_file_headers = ["_email", "_website", "_entity_id", "firstname", "lastname", "city", "country_id",
                            "postcode", "street", "telephone", "_address_default_billing_",
                            "_address_default_shipping_"]

    entity_id = 10
    with open('build/customers.csv', 'w') as main_file, open('build/customer_addresses.csv', 'w') as address_file:
        main_file_writer = csv.DictWriter(main_file, fieldnames=main_file_headers, quoting=csv.QUOTE_NONNUMERIC)
        main_file_writer.writeheader()
        address_file_writer = csv.DictWriter(address_file, fieldnames=address_file_headers,
                                             quoting=csv.QUOTE_NONNUMERIC)
        address_file_writer.writeheader()
        unique_emails = ['grec.georgian@gmail.com', 'bogdannf@yahoo.com', 'marius.developer@gmail.com']

        for db_row in db_rows:
            user_email = db_row[2].lower()
            if not user_email or user_email in unique_emails:
                continue

            unique_emails.append(user_email)
            user_meta = deserialize(db_row[1])
            user_telephone = ''

            if 'contactPersons'.encode() in user_meta:
                cps = user_meta['contactPersons'.encode()]
                for cp in cps.values():
                    if cp['email'.encode()].decode() == user_email:
                        user_telephone = cp['phone1'.encode()].decode()
                        break

            user_addresses = []
            if 'deliveryAddresses'.encode() in user_meta:
                db_user_addresses_map = user_meta['deliveryAddresses'.encode()]
                if db_user_addresses_map:
                    for db_user_address in db_user_addresses_map.values():
                        user_addresses.append({
                            'city': db_user_address['city'.encode()].decode(),
                            'district': db_user_address['district'.encode()].decode(),
                            'streetAddress': db_user_address['streetAddress'.encode()].decode(),
                            'zipcode': db_user_address['zipcode'.encode()].decode(),
                        })

            user_firstname = db_row[4]
            user_lastname = db_row[3]
            user_upgraded_password = user_meta['password'.encode()].decode() + '::0'

            # -- Write main file -- #
            main_file_writer.writerow({
                'email': user_email,
                'firstname': user_firstname,
                'lastname': user_lastname,
                'password_hash': user_upgraded_password,
                'created_at': db_row[5],
                'group_id': 1,
                'website_id': 1,
                'store_id': 1,
                "_website": "base"
            })

            # -- Write address file -- #
            if user_addresses:
                for idx, user_address in enumerate(user_addresses):
                    entity_id += 1
                    address_file_writer.writerow({
                        '_email': user_email,
                        '_website': 'base',
                        '_entity_id': entity_id,
                        'firstname': user_firstname,
                        'lastname': user_lastname,
                        'telephone': user_telephone,
                        'country_id': 'RO',
                        'city': normalize_string(user_address['city']),
                        'postcode': user_address['zipcode'],
                        # 'region': normalize_string(user_address['district']),
                        'street': user_address['streetAddress'],
                        '_address_default_billing_': 1 if idx == 0 else 0,
                        '_address_default_shipping_': 1 if idx == 0 else 0,
                    })


in_conn = common.create_connection('db.sqlite')
with in_conn:
    process_and_export_users(in_conn)
