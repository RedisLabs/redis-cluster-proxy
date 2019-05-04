default: all

.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

.PHONY: install

test:
	cd src && $(MAKE) $@
.PHONY: test
