"""Slow python based decoder to output QA dataset"""
import csv
import re

import ais

# Config
types = [1, 2, 3, 5, 18, 24]
input_file = "../data/ais-mini-test/ais_mini_10000.dat"

# Setup regex to check for exceptions we care about, they all follow the format
# AisN:
exception_handle = re.compile(r"Ais+(18:|24:|1:|2:|3:|5:)")

# Some empty values to use later on
errors = []
parsed = 0
attempted = 0
failed = 0
failed_but_dont_care = 0
ids_written = []
append_next = False
five_part_1 = None

with open(input_file, 'r') as f:
    for line in f:

        # Print out info to console every now and again
        if attempted % 5000 == 0:
            print("===    "
                  f"read: {attempted} | " + f"written: {parsed} | " +
                  f"failed:{failed} | " +
                  f"failed (other msg type): {failed_but_dont_care}" +
                  "    ===")

        attempted += 1
        raw_message = line.split(',')[6]

        # Try and infer padding from pid length. The messages we care about are
        # usually multiples of 168 bits
        n_chars = len(raw_message) * 6
        padding = int((n_chars % 168) / 6)

        # Workaround for Message Type 5s that will fail on the first run due
        # to the second part of the message not being there
        if append_next:
            raw_message = five_part_1 + raw_message
            padding = 2  # This seems to always be 2 in this case
            append_next = False

        # Workaround for message types 24. Padding is incorrectly calculated
        # from the above
        if padding == 27:
            padding = 2

        try:
            data = ais.decode(raw_message, padding)  # Attempt the decode
            data['rawInput'] = line.rstrip("\n")  # Append the raw data

            if data['id'] <= 3:
                # Merge types 1,2,3 together - as they're the same really...
                target_file = '123.csv'
            else:
                target_file = str(data['id']) + '.csv'

            if data['id'] in types:
                with open(target_file, 'a') as out_f:
                    t_write = csv.DictWriter(out_f, data.keys())
                    if data['id'] not in ids_written:
                        t_write.writeheader()
                        ids_written.append(data['id'])
                    t_write.writerow(data)
                    parsed += 1

        except Exception as e:
            # Workaround for message 5 being two part sometimes
            if (n_chars == 348) & (str(e) == 'Ais5: AIS_ERR_BAD_BIT_COUNT'):
                five_part_1 = raw_message
                append_next = True
            else:
                # If we fail but it isn't a important message type log it and
                # carry on
                failed_but_dont_care += 1

                # If we fail to decode and it is a message type print out to
                # console
                if exception_handle.match(str(e)):
                    failed += 1
                    print(f'=== ERROR === \n Line: {line} \n' +
                          f'Message: {raw_message}\n' +
                          f"n chars: {n_chars} " + f"padding: {padding} ")

                    print(e)
