from enum import IntEnum


class ProductAmountCacheResults(IntEnum):
    KEY_NOT_EXIST = 1
    ENOUGH_AMOUNT = 2
    NOT_ENOUGH_AMOUNT = 3

