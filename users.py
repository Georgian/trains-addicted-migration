import csv

import common
from serialize_tools import unserialize, loads, dumps


def deserialize(db_object):
    return unserialize(loads(dumps(db_object)))


def process_and_export_users(conn):
    db_rows = common.fetch_all(conn, "SELECT id, data, email, lastName, firstName, creationDate FROM objects WHERE class = 'Cuser'")

    headers = ["email", "_website", "firstname", "lastname", "password_hash", "created_at",
               "_address_city", "_address_region", "_address_street", "_address_postcode"]

    # --- All products ---
    with open('build/users.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()

        for db_row in db_rows:
            user_meta = deserialize(db_row[1])

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

            upgraded_password = user_meta['password'.encode()].decode() + '::0'

            csv_row = {
                'email': db_row[2],
                '_website': 'base',
                'firstname': db_row[4],
                'lastname': db_row[3],
                'password_hash': upgraded_password,
                'created_at': db_row[5]
            }

            if user_addresses:
                first_address = user_addresses[0]
                csv_row['_address_city'] = first_address['city']
                csv_row['_address_region'] = first_address['district']
                csv_row['_address_street'] = first_address['streetAddress']
                csv_row['_address_postcode'] = first_address['zipcode']
                writer.writerow(csv_row)

                for other_address in user_addresses[1:]:
                    writer.writerow({
                        'email': '',
                        '_website': '',
                        'firstname': '',
                        'lastname': '',
                        'password_hash': '',
                        'created_at': '',
                        '_address_city': other_address['city'],
                        '_address_region': other_address['district'],
                        '_address_street': other_address['streetAddress'],
                        '_address_postcode': other_address['zipcode']
                    })
            else:
                writer.writerow(csv_row)


in_conn = common.create_connection('db.sqlite')
with in_conn:
    process_and_export_users(in_conn)