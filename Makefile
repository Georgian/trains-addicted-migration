MAKEFLAGS += --silent

clean:
	rm -f *.pyc || true
	rm -f *.csv || true
	rm -f out.sqlite || true

products:
	./export.py db.sqlite

orders:
	./export.py db.sqlite orders

all: clean products orders
