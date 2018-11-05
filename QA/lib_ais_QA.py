"""Slow python based decoder to output QA dataset."""
import csv
import re

import ais
import numpy as np

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
ids_written = {}
append_next = False
five_part_1 = None

# Keys to keep for msg types 123
m123keys = [
    'id', 'repeat_indicator', 'mmsi', 'nav_status', 'rot_over_range', 'rot',
    'sog', 'position_accuracy', 'x', 'y', 'cog', 'true_heading', 'timestamp',
    'special_manoeuvre', 'spare', 'raim', 'sync_state'
]

m123_nav_status_lookup = {
    1: "At anchor",
    0: "Under way using engine",
    2: "Not under command",
    3: "Restricted manoeuverability",
    4: "Constrained by her draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in Fishing",
    8: "Under way sailing",
    9: "Reserved for future amendment of Navigational Status for HSC",
    10: "Reserved for future amendment of Navigational Status for WIG",
    11: "Reserved for future use",
    12: "Reserved for future use",
    14: "AIS-SART is active",
    13: "Reserved for future use"
}


def bool2int(b):
    """Convert Boolean to integer.

    Parameters
    ----------
    b : boolean
        True or False

    """
    if isinstance(b, bool):
        return (int(b))
    else:
        return (b)


def stringcorrection(s):
    """Replace strings that are not handled correctly.

    Parameters
    ----------
    s : String, only replaced if encountered

    """
    if isinstance(s, str):
        return (s.replace('`', '\''))
    else:
        return (s)


def corrections(x):
    """Collect together the various corrections applied to the whole dict.

    Parameters
    ----------
    x : any non iterable

    """
    x = bool2int(x)
    x = stringcorrection(x)
    return (x)


def null_handling(dictionary):
    """Add null handling that is missing in libais, but implimented in scala.

    Parameters
    ----------
    dictionary : dict
        Dictionary returned by libais.decode

    """
    d = dictionary.copy()

    null_dict = {
        'true_heading': lambda x: None if x == 511 else x,
        'rot': lambda x: None if np.abs(x) > 720.1 else x,
        'timestamp': lambda x: None if x > 59 else x,
        'sog': lambda x: None if x > 102.3 else x,
        'x': lambda x: None if x == 181.0 else x,
        'y': lambda x: None if x == 91.0 else x,
        'cog': lambda x: None if x == 260 else x,
        'eta_month': lambda x: None if x == 0 else x,
        'eta_day': lambda x: None if x == 0 else x,
        'eta_hour': lambda x: None if x == 24 else x,
        'eta_minute': lambda x: None if x == 60 else x
    }

    d_with_null = {
        k: null_dict[k](v)
        for k, v in dictionary.items() if k in null_dict.keys()
    }

    d.update(d_with_null)

    return (d)


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

            # We need the whole message to validate the decoder
            # so replace the message on the shorter string.
            # Note this does mean that checksums will fail.
            deconstructed_line = line.split(',')
            deconstructed_line[6] = raw_message
            line = ','.join(deconstructed_line)

        # Workaround for message types 24. Padding is incorrectly calculated
        # from the above
        if padding == 27:
            padding = 2

        try:
            data = ais.decode(raw_message, padding)  # Attempt the decode

            target_file = str(data['id']) + '.csv'

            if data['id'] <= 3:
                # Merge types 1,2,3 together - as they're the same for our
                # purposes
                target_file = '123.csv'

                # The output fields vary a bit, we only want the core AIS ones
                # or it messes up the csv
                unwanted = set(data.keys()) - set(m123keys)
                for unwanted_key in unwanted:
                    del data[unwanted_key]

                # In Scala we map the nav_status to a string discription, so do
                # this here too
                data['nav_status'] = m123_nav_status_lookup[data['nav_status']]

            elif data['id'] == 24:
                # Type 24 has parts A and B, so output to diff files
                target_file = (
                    str(data['id']) + '_' + str(data['part_num']) + '.csv')

            # Convert any Bools to integers (as that's how they're handled in
            # Scala)
            data = {k: corrections(v) for k, v in data.items()}

            data = null_handling(data)

            # Append the raw data
            data['rawInput'] = line.rstrip("\n")

            # Only write if we care about the message type
            if data['id'] in types:
                with open(target_file, 'a') as out_f:
                    t_write = csv.DictWriter(out_f, data.keys())

                    # Only write header if its the first time we're writing
                    # this particular file type
                    if target_file not in ids_written:
                        t_write.writeheader()
                        ids_written.update({target_file: len(data)})

                    if len(data) != ids_written[target_file]:
                        print(f"Error writing {target_file}" +
                              f" Data had {len(data)} fields when" +
                              f" {ids_written[target_file]} fields expected")

                    else:
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
