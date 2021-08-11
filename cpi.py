#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Testing CPISync
"""

import sys

def reconcile_sets(A, B, Zs):
    """
    Find the elements in set A (DeltaA) not found in set B.
    Assume set B is a subset of A (DeltaB is the null set).
    """
    import math

    ### Calculate the characteristic polynomials for each test value Z.
    chis_A = [math.prod([(item - (Z)) for item in A]) for Z in Zs]
    chis_B = [math.prod([(item - (Z)) for item in B]) for Z in Zs]

    ### List of ratios between chi_A and chi_B.
    ratios = [(chi_A / chi_B) for chi_A, chi_B in zip(chis_A, chis_B)]

    ### Rational Interpolation
    ### ====================== 
    ### p_0(Z)^0 + p_1(Z)^1 + p_2(Z)^2 + p_3(Z)^3 + p_4(Z)^4

    ### Because B is part of A, DeltaB is the null set and chi_DeltaB evaluates to 1.
    ### chi_A / chi_B = chi_DeltaA / chi_DeltaB = chi_DeltaA

    ### Build the matrix of Zs for the polynomial stated above.
    ### e.g. Z = -2 -> [1, -2, 4, -8, 16, -32]
    m = len(Zs)
    polynomial_Zs = [[(Z**i) for i in range(m)] for Z in Zs]

    print(polynomial_Zs)
    print(ratios)




def main():
    print(reconcile_sets({1, 2, 3}, {1, 2}, [-1, -2, -3, -4, -5, -6]))
    return 0

if __name__ == '__main__':
    sys.exit(main())
