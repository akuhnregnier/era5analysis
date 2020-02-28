# -*- coding: utf-8 -*-

import pkg_resources

from .era5_download import *
from .era5_tables import *

__version__ = pkg_resources.require("era5analysis")[0].version
