#---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
#---------------------------------------------------------------------
def print_binary(data, prnt=True):

    def print_line(offset, dec, char, prnt=True):
        if len(dec) == 0:
            return

        line_offset = "%08X: " % offset

        line_dec = ""
        line_char = ""
        for d in dec:
            if isinstance(d, str):
                d = ord(d)
            # print(d)
            line_dec += "%02X " % d

        for c in char:
            line_char += "%c" % c

        pad = 16 - len(dec)

        for _ in range(pad):
            line_dec += "   "

        line_total = line_offset + line_dec + "  " + line_char

        if offset == 0 and prnt:
            print("-" * len(line_total))

        if prnt:
            print(line_total)
            return

        return line_total

    char = []
    dec = []
    result = []
    offset = 0

    i = 0
    for d in data:
        dec.append(d)
        if isinstance(d, str):
            d = ord(d)
        if d >= 32 and d < 127:
            char.append(d)
        else:
            char.append(".")

        i += 1
        if i == 16:
            result.append(print_line(offset, dec, char, prnt=prnt))
            offset += 16
            i = 0
            char = []
            dec = []

    last_line = print_line(offset, dec, char, prnt=prnt)
    if last_line:
        result.append(last_line)

    if prnt:
        return

    return result
