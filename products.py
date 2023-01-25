import csv

from bs4 import BeautifulSoup
from phpserialize import *

from common import *


def deserialize(db_object):
    return unserialize(loads(dumps(db_object)))


def get_objects_by_id(conn, clazz):
    cursor = conn.cursor()
    cursor.execute("select id, data from objects where class=?", (clazz,))
    rows = cursor.fetchall()
    result = {}
    for row in rows:
        meta = deserialize(row[1])
        # CproducerPage has pageTitle instead of title
        result[row[0]] = meta[("title_ro" if clazz != 'CproducerPage' else "pageTitle_ro").encode()].decode()
    return result


def get_objects_by_product_code(conn, object_page, object_product_sql):
    objects_by_id = get_objects_by_id(conn, object_page)
    rows = fetch_all(conn, object_product_sql)
    result = {}
    for row in rows:
        if row[1]:
            result[row[0]] = objects_by_id[row[1]]
    return result


def get_categories_by_product_code(conn):
    return get_objects_by_product_code(
        conn,
        'CcategoryPage',
        "select objects.productCode, products_categories.categoryId, products_categories.productId "
        "from objects "
        "join products_categories on objects.Id=products_categories.productId "
        "where objects.class='CproductPage'"
    )


def get_operators_by_product_code(conn):
    return get_objects_by_product_code(
        conn,
        'CoperatorPage',
        "select objects.productCode, products_operators.operatorId, products_operators.productId "
        "from objects "
        "join products_operators on objects.Id=products_operators.productId"
    )


def get_epoques_by_product_code(conn):
    return get_objects_by_product_code(
        conn,
        'CepoquePage',
        "select objects.productCode, products_epoques.epoqueId, products_epoques.productId "
        "from objects "
        "join products_epoques on objects.Id=products_epoques.productId"
    )


def get_scales_by_product_code(conn):
    return get_objects_by_product_code(
        conn,
        'CscalePage',
        "select objects.productCode, products_scales.scaleId, products_scales.productId "
        "from objects "
        "join products_scales on objects.Id=products_scales.productId"
    )


def get_pictures_by_product_code(conn):
    rows = fetch_all(conn, "select o1.productCode, o2.id, o2.data "
                           "from objects o1, objects o2 "
                           "where o2.class='Cslideshow' and o1.class='CproductPage' and o2.parentId = o1.id")
    result = {}
    pic_path = "{}/{}"
    for row in rows:
        meta = decode_dict(deserialize(row[2]))
        if "pictures" in meta:
            pics = decode_dict(meta["pictures"]).values()
            result[row[0]] = [pic_path.format(row[1], sanitize_pic_name(pic)) for pic in pics]
    return result


def get_producers(conn):
    producers_by_id = get_objects_by_id(conn, 'CproducerPage')
    rows = fetch_all(conn, "select productCode, parentId from objects")
    result = {}
    for row in rows:
        if row[1] and row[1] in producers_by_id:
            result[row[0]] = producers_by_id[row[1]]
    return result


def get_images_by_product_id(conn):
    rows = fetch_all(conn, "SELECT parentId, data FROM objects where class='Cslideshow'")
    result = {}
    for row in rows:
        pix_meta = deserialize(row[1])
        if "pictures" in pix_meta:
            pix = pix_meta["pictures"]
            result[row[0]] = ['/home/trains/public_html/Resources/{0}'.format(p) for p in pix.values()]
    return result


def get_images_by_product_code(conn):
    images_by_product_id = get_images_by_product_id(conn)
    rows = fetch_all(conn, "SELECT id, productCode FROM objects where class='CproductPage'")
    result = {}
    for row in rows:
        product_id = row[0]
        product_code = row[1]
        if product_id in images_by_product_id:
            result[product_code] = images_by_product_id[product_id]
    return result


def get_weblink_mappings():
    mappings = {}
    with open('mappings/weblink_mappings.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        counter = 0
        for reader_row in reader:
            sku = reader_row['sku']
            sku_mapping = mappings[sku] if sku in mappings else []
            mappings[sku] = sku_mapping
            sku_mapping.append({
                'op': reader_row['operation'],
                'old_url': reader_row['old_url'],
                'new_url': reader_row['new_url']
            })
            counter += 1
        print('Read ' + str(counter) + ' description mappings')
    return mappings


def get_category_mappings():
    mappings = {}
    with open('mappings/category_mappings.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for reader_row in reader:
            mappings[reader_row['old_category']] = reader_row['new_category']
    return mappings


def get_producer_mappings():
    mappings = {}
    with open('mappings/producer_mappings.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for reader_row in reader:
            mappings[reader_row['old_producer']] = reader_row['new_producer']
    return mappings


def fetch_all(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def process_and_export_products(conn):
    product_categories = get_categories_by_product_code(conn)
    product_operators = get_operators_by_product_code(conn)
    product_epoques = get_epoques_by_product_code(conn)
    product_scales = get_scales_by_product_code(conn)
    product_producers = get_producers(conn)
    product_pictures = get_pictures_by_product_code(conn)

    category_mappings = get_category_mappings()
    producer_mappings = get_producer_mappings()
    weblink_mappings = get_weblink_mappings()
    multiple_values_separator = ';'

    db_products_raw = fetch_all(
        conn,
        "SELECT id, productCode, title_ro, title_en, salePrice, discountPrice, parentId, creationDate, stock, data "
        "FROM objects "
        "WHERE class='CproductPage'")

    # Sort by create_date (index 7)
    db_products = sorted(db_products_raw, reverse=True, key=lambda x: x[7])

    url_keys = []
    skus = []

    csv_rows = []
    csv_rows_en = []
    for db_product in db_products:
        product_code = db_product[1]
        if product_code in skus:
            print('Duplicate sku: ' + product_code)
            continue  # Skip duplicates
        skus.append(product_code)
        title_ro = db_product[2]
        title_en = db_product[3]
        product_meta = decode_dict(deserialize(db_product[9]))
        pics = product_pictures[product_code] if product_code in product_pictures else []
        main_pic = pics[0] if pics else ''
        special_price = db_product[5] if (db_product[5] and db_product[5] != '0.0') else ''

        processed_description = process_description(product_code, product_meta.get("description_ro", ""),
                                                    weblink_mappings)
        description_ro = processed_description['result']
        processed_description = process_description(product_code, product_meta.get("description_en", ""),
                                                    weblink_mappings)
        description_en = processed_description['result']

        meta_description = product_meta.get("metaDescription_ro", "")
        meta_description_en = product_meta.get("metaDescription_en", "")
        price = db_product[4]
        product_name = title_ro if title_ro and title_ro != '' else meta_description

        url_key = sanitize_url_key(product_name)
        if url_key in url_keys:
            # If URL key exists, add product code to make it unique
            url_key = '{}-{}'.format(url_key, sanitize_url_key(product_code))
        url_keys.append(url_key)

        old_category = product_categories[product_code] if product_code in product_categories else ''
        new_category = category_mappings[old_category] if old_category in category_mappings else old_category

        producer = product_producers.get(product_code, '')
        if producer in producer_mappings:
            producer = producer_mappings[producer]

        csv_rows.append({
            "sku": product_code.strip(),
            "name": product_name,
            "old_category": old_category,
            "categories": new_category,
            "description": description_ro,
            "created_at": db_product[7],
            "price": price if price else 0,  # salePrice
            "special_price": special_price,  # discountPrice
            "base_image": main_pic,
            "small_image": main_pic,
            "thumbnail_image": main_pic,
            "additional_images": multiple_values_separator.join(pics) if len(pics) > 1 else '',
            "url_key": url_key,
            "product_type": "simple",
            "attribute_set_code": "Default TA",
            "product_websites": "base",
            "qty": db_product[8],
            # "additional_attributes": '',  # TODO
            # "short_description": "",  # TODO
            "meta_title": title_ro,
            "meta_description": meta_description,
            "meta_keywords": product_meta.get("metaKeywords_ro", ""),
            "operator": product_operators.get(product_code, ''),
            "producator": producer,
            "scara": product_scales.get(product_code, ''),
            "epoca": product_epoques.get(product_code, ''),
        })

        # ----- english -----
        product_name = title_en if title_en and title_en != '' else meta_description_en

        url_key = sanitize_url_key(product_name)
        if url_key in url_keys:
            # If URL key exists, add product code to make it unique
            url_key = '{}-{}'.format(url_key, sanitize_url_key(product_code))
        url_keys.append(url_key)

        csv_rows_en.append({
            "sku": product_code.strip(),
            "store_view_code": "com",  # Configured in Magento Admin
            "name": product_name,
            "description": description_en,
            "meta_title": title_en,
            "meta_description": meta_description_en,
            "meta_keywords": product_meta.get("metaKeywords_en", "")
        })

    export_products(csv_rows)
    export_products_en(csv_rows_en)
    export_products_in_batches(csv_rows)
    export_all_skus(db_products)
    export_urls_in_descriptions(db_products)


HEADERS_RO = ["sku", "name", "old_category", "categories", "description", "created_at", "price", "special_price",
              "url_key", "product_type", "attribute_set_code", "product_websites", "qty", "additional_attributes",
              "short_description", "meta_title", "meta_keywords", "meta_description", "base_image",
              "additional_images", "thumbnail_image", "small_image", "epoca", "scara", "operator", "producator"]

HEADERS_EN = ["sku", "store_view_code", "name", "description", "meta_title", "meta_description", "meta_keywords"]


def export_products(csv_rows):
    with open_file_w('all_products.csv') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=HEADERS_RO, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for csv_row in csv_rows:
            writer.writerow(csv_row)


def export_products_en(csv_rows):
    with open_file_w('all_products_en.csv') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=HEADERS_EN, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for csv_row in csv_rows:
            writer.writerow(csv_row)


def export_products_in_batches(csv_rows):
    batch_size = 1500
    for idx, csv_row_batch in enumerate(batch(csv_rows, batch_size)):
        with open_file_w('products_ro_{}.csv'.format(idx + 1)) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=HEADERS_RO, quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            for csv_row in csv_row_batch:
                writer.writerow(csv_row)


def export_urls_in_descriptions(db_products):
    matcher = re.compile('(?<=href=")(.*?)trains-addicted.ro(.*?)(?=")')
    with open_file_w('urls_in_descriptions_ro.csv') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['sku', 'name', 'url_ro'])
        writer.writeheader()
        for db_product in db_products:
            product_code = db_product[1]
            product_meta = decode_dict(deserialize(db_product[9]))
            meta_description = product_meta.get("metaDescription_ro", "")
            title_ro = db_product[2]
            product_name = title_ro if title_ro and title_ro != '' else meta_description
            description_ro = process_description(product_code, product_meta.get("description_ro", ""), None)['result']
            for url in re.findall(matcher, description_ro):
                full_url = '{}trains-addicted.ro{}'.format(url[0], url[1])
                writer.writerow({'sku': product_code, 'name': product_name, 'url_ro': full_url})

    with open_file_w('urls_in_descriptions_en.csv') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['sku', 'name', 'url_en'])
        writer.writeheader()
        for db_product in db_products:
            product_code = db_product[1]
            product_meta = decode_dict(deserialize(db_product[9]))
            meta_description = product_meta.get("metaDescription_en", "")
            title_ro = db_product[2]
            product_name = title_ro if title_ro and title_ro != '' else meta_description
            description_en = process_description(product_code, product_meta.get("description_en", ""), None)['result']
            for url in re.findall(matcher, description_en):
                full_url = '{}trains-addicted.ro{}'.format(url[0], url[1])
                writer.writerow({'sku': product_code, 'name': product_name, 'url_en': full_url})


def export_all_skus(db_products):
    with open_file_w('all_skus.csv') as skus_csvfile:
        writer = csv.DictWriter(skus_csvfile, fieldnames=['sku'])
        writer.writeheader()
        for db_product in db_products:
            writer.writerow({'sku': db_product[1].strip()})


def process_description(sku, description, weblink_mappings):
    result = description
    # Replace description image paths
    if description and '/Upload' in description:
        for url in re.findall('(?<=src=")(.*?)/Upload/(.*?)(?=")', description):
            # URL will be a tuple. Example: ('Resources/70817815/^all', 'macara-edk-750-db-Roco-73035-a-.jpg')
            old_path = '{}/Upload/{}'.format(url[0], url[1])
            new_path = 'pub/media/wysiwyg/TA/descriptionImages/{}'.format(sanitize_pic_name(url[1]))
            result = result.replace(old_path, new_path)
    # Replace website urls
    processed_link_count = 0
    if weblink_mappings and sku in weblink_mappings:
        # Sorted reversed by old_url, because 'my-link-2.html' should be replaced before 'my-link.html'
        for sku_mapping in sorted(weblink_mappings[sku], key=lambda x: x['old_url'], reverse=True):
            op = sku_mapping['op']
            old_url = sku_mapping['old_url']
            new_url = sku_mapping['new_url']
            # Find every a href tag where the old url is present
            ahrefs = BeautifulSoup(result, 'html.parser').findAll('a', {'href': old_url})
            if op != '' and ahrefs:
                # Here we treat these complex cases where we need to delete link or link&text
                for ahref in ahrefs:
                    replacement_url = '' if op == 'DELETE_LINK_AND_TEXT' else ahref.get_text()
                    result = result.replace(str(ahref), replacement_url)
                    processed_link_count += 1
            elif old_url in result:
                # Simple case, where we just replace the link
                result = result.replace(old_url, new_url)
                processed_link_count += 1
            else:
                # Weird case, where we might have missed something
                print('WARN: Did not replace a weblink for sku ' + sku + ' ... please check manually')
    return {'result': result, 'count': processed_link_count}


in_conn = create_connection('db.sqlite')
with in_conn:
    process_and_export_products(in_conn)
