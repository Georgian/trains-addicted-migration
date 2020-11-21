# Intro

This funky repo will translate the TA sqlite into magento/opencart data.

For now it will only dump the available orders into a single table database.

Stay tuned...

# Prereqs

Python 2.7;

# How to run

The main script to run is ``export.py``. As is, the script works on a few assumptions:

 * A ``db.sqlite`` file is found at the same path as the script. This will constitute the input database file.
 * The output of the script is another database called ``out.sqlite``. If succesfull, the output will be found in the working directory.

### * Export products and users:

```bash
$ ./export.py db.sqlite
```

The output of the above command will consist out various ``csv`` files.

### * Export orders:


```bash
$ ./export.py db.sqlite orders
```

The output of the above command will consist out of a new sqlite3 which contains all the orders in a queryable fashion.

The default name for the newly generated db is ``out.sqlite``.


# GNU/Make to the rescue

For convenience purposes, there is a ``Makefile`` available which exposes the following tasks:

### Export orders

```bash
$ make orders
```

### Export products

```bash
$ make products
```

### Clear previously exported artifacts

```bash
$ make clean
```

### Make all

```bash
$ make all
```