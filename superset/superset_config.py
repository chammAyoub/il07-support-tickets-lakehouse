import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "cle_par_defaut_pour_dev_local")