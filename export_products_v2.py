import csv
import sqlite3
from serialize_tools import unserialize, loads, dumps


def deserialize(db_object):
    return unserialize(loads(dumps(db_object)))


def decode(obj):
    return obj.decode() if isinstance(obj, bytes) else obj


def decode_dict(d):
    return {decode(k): decode(v) for k, v in d.items()}


def get_objects_by_id(conn, clazz):
    cursor = conn.cursor()
    cursor.execute("select id, data from objects where class=?", (clazz,))
    rows = cursor.fetchall()
    result = {}
    for row in rows:
        meta = deserialize(row[1])
        result[row[0]] = meta["title_ro".encode()].decode()
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
                           "where o2.class='Cslideshow' and o2.parentId = o1.id")
    result = {}
    pic_path = '{}/^all/{}'
    for row in rows:
        meta = decode_dict(deserialize(row[2]))
        if "pictures" in meta:
            pics = decode_dict(meta["pictures"]).values()
            result[row[0]] = [pic_path.format(row[1], pic) for pic in pics]
    return result


def get_manufacturers(conn):
    rows = fetch_all(conn, "SELECT id, title_ro FROM objects where class='CproducerPage'")
    result = {}
    for row in rows:
        result[row[0]] = row[1]
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


def get_category_mappings():
    mappings = {}
    with open('category_mappings.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for reader_row in reader:
            mappings[reader_row['old_category']] = reader_row['new_category']
    return mappings


def fetch_all(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def export_products_magento(conn):
    product_categories = get_categories_by_product_code(conn)
    product_operators = get_operators_by_product_code(conn)
    product_epoques = get_epoques_by_product_code(conn)
    product_scales = get_scales_by_product_code(conn)
    product_manufacturers = get_manufacturers(conn)
    # product_images = get_images_by_product_code(conn)
    product_pictures = get_pictures_by_product_code(conn)
    category_mappings = get_category_mappings()

    db_products = fetch_all(
        conn,
        "SELECT id, productCode, title_ro, title_en, salePrice, discountPrice, parentId, creationDate, data "
        "FROM objects "
        "WHERE class='CproductPage'")

    headers_en = [
        "sku",
        "name",
        "description",
        "meta_description",
        "meta_keywords"
    ]

    headers_ro = [
        "sku",
        "name",
        "old_category",
        "categories",
        "description",
        "price",
        "special_price",
        "url_key",
        "product_type",
        "attribute_set_code",
        "product_websites",
        "qty",
        "additional_attributes",
        "short_description",
        "meta_title",
        "meta_keywords",
        "meta_description",
        "base_image",
        "additional_images",
        "ta_epoca",
        "ta_scara",
        "ta_operator",
        "ta_producator"
    ]

    with open('products_magento_ro.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers_ro)
        writer.writeheader()
        for db_product in db_products[:100]:
            product_code = db_product[1]
            title_ro = db_product[2]
            product_meta = decode_dict(deserialize(db_product[8]))
            pics = product_pictures[product_code]
            try:
                old_category = product_categories[product_code]
            except KeyError:
                old_category = ''
            product = {
                "sku": product_code,
                "name": title_ro,  # title_ro,
                "old_category": old_category,
                "categories": category_mappings[old_category],
                "description": product_meta.get("description_ro", ""),
                "price": db_product[4],  # salePrice
                "special_price": db_product[5],  # discountPrice
                "base_image": pics[0] if pics else '',
                "additional_images": ','.join(pics) if len(pics) > 1 else '',
                "url_key": title_ro.strip().replace(" ", "-").lower() + '-' + product_code,
                "product_type": "simple",
                "attribute_set_code": "Default TA",
                "product_websites": "base",
                "qty": 1,  # TODO
                "additional_attributes": "",  # TODO
                "short_description": "",  # TODO
                "meta_title": title_ro,
                "meta_description": product_meta.get("metaDescription_ro", ""),
                "meta_keywords": product_meta.get("metaKeywords_ro", ""),
                "ta_epoca": product_epoques.get(product_code, ""),
                "ta_operator": product_operators.get(product_code, ""),
                "ta_scara": product_scales.get(product_code, ""),
                "ta_producator": product_manufacturers.get(product_code, "")
            }
            writer.writerow(product)


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as conn_err:
        print(conn_err)

    return None


in_conn = create_connection('db.sqlite')
with in_conn:
    export_products_magento(in_conn)
