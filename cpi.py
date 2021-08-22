#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Testing CPISync
"""

import sys

def _prod_mod(items, mod):
    result = items[0]
    for item in items[1:]:
        result = (result * item) % mod
    return result

def _choose_prime(seconds: int) -> int:
    from constants import YEAR_SECONDS, YEAR_INTERVAL_PRIMES
    years = seconds / YEAR_SECONDS
    if years > max(YEAR_INTERVAL_PRIMES):
        raise Exception(ValueError, 'Interval is too large.')
    for ratio, prime in YEAR_INTERVAL_PRIMES.items():
        if years < ratio:
            return prime

def reconcile_sets(A, B, Zs, begin_int: int, end_int: int):
    """
    Find the elements in set A (DeltaA) not found in set B.
    Assume set B is a subset of A (DeltaB is the null set).
    """
    import math, numpy as np
    from sympy.polys.specialpolys import interpolating_poly
    import galois

    m = len(Zs)
    mod_by_prime = _choose_prime(end_int)
    GF = galois.GF(mod_by_prime)
    fA = GF(A)
    fB = GF(B)
    fZs = np.negative(GF([abs(z) for z in Zs]))

    ### Calculate the characteristic polynomials for each test value Z.
    chis_A = GF([_prod_mod([(Z - item) for item in A], mod_by_prime) for Z in Zs])
    chis_B = GF([_prod_mod([(Z - item) for item in B], mod_by_prime) for Z in Zs])
    
    ### List of ratios between chi_A and chi_B.
    ratios = np.divide(chis_A, chis_B)

    ### Rational interpolation
    ### ====================== 
    ### p_0(Z)^0 + p_1(Z)^1 + p_2(Z)^2 + p_3(Z)^3 + p_4(Z)^4

    ### Because B is part of A, DeltaB is the null set and chi_DeltaB evaluates to 1.
    ### chi_A / chi_B = chi_DeltaA / chi_DeltaB = chi_DeltaA

    ### Build the matrix of Zs for the polynomial stated above.
    ### e.g. Z = -2 -> [1, -2, 4, -8, 16, -32]
    polynomial_fZs = GF([[(Z**i) for i in range(m)] for Z in fZs])

    coefficients = np.linalg.solve(
        polynomial_fZs,
        ratios
    )
    poly = galois.Poly(coefficients, order='asc')
    return poly.roots()


def main():
    import datetime, re, string, random
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.utcnow()
    begin = now
    end = begin + relativedelta(months=1)

    begin_int = int(begin.timestamp() - now.timestamp())
    end_int = int(end.timestamp() - now.timestamp())
    n_rows = 100
    missing_rows = 10
    A = [random.choice(range(begin_int, end_int)) for i in range(n_rows)]
    B = A[:(-1 * missing_rows)] 
    delta_A = [
        datetime.datetime.utcfromtimestamp(int(r) + now.timestamp())
        for r in reconcile_sets(
            A, B,
            list(range(-1, -100, -1)),
            begin_int,
            end_int,
        )
    ]
    print(delta_A)
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
