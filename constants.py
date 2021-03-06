#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Constant values for syncx.
"""

from collections import OrderedDict
YEAR_SECONDS = 31622400
YEAR_INTERVAL_PRIMES = OrderedDict([
    (2**-14, 1931),    ### ~30 minutes
    (2**-13, 3863),    ### ~ 1 hour
    (2**-12, 7723),    ### ~2 hours
    (2**-11, 15443),   ### ~4 hours
    (2**-10, 30893),   ### ~8.5 hours
    (2**-9, 61781),    ### ~17 hours
    (2**-8, 123527),   ### ~1.5 days
    (2**-7, 247067),   ### ~3 days
    (2**-6, 494101),   ### ~6 days
    (2**-5, 988213),   ### ~1.5 weeks
    (2**-4, 1976411),  ### ~3 weeks
    (2**-3, 3952813),  ### ~1.5 months
    (2**-2, 7905607),  ### ~3 months
    (2**-1, 15811217), ### ~6 months
    (1, 31622429),     ### 1 year
    (2, 63244801),
    (3, 94867211),
    (4, 126489607),
    (5, 158112001),
    (6, 189734407),
    (7, 221356801),
    (8, 252979213),
    (9, 284601613),
    (10, 316224001),
    (11, 347846417),
    (12, 379468807),
    (13, 411091243),
    (14, 442713619),
    (15, 474336001),
    (16, 505958413),
    (17, 537580837),
    (18, 569203207),
    (19, 600825653),
    (20, 632448013),
    (21, 664070411),
    (22, 695692849),
    (23, 727315219),
    (24, 758937607),
    (25, 790560011),
    (26, 822182411),
    (27, 853804817),
    (28, 885427211),
    (29, 917049607),
    (30, 948672031),
    (31, 980294401),
    (32, 1011916817),
    (33, 1043539201),
    (34, 1075161629),
    (35, 1106784017),
    (36, 1138406407),
    (37, 1170028817),
    (38, 1201651201),
    (39, 1233273647),
    (40, 1264896001),
])


