EXTENSION = meta
DATA = meta--0.1.sql meta.control 
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
