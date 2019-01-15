# Apache Spark (Scala) AIS Decoder

A decoder to transform raw AIS (Marine Automatic Identification System) messages to tabular parquet files. 

```
.
├── QA		# Scripts to generate a comparison dataset using `schwehr/libais`
└── aisdecode	# Spark Scala library
```

## Approach

The decoder builds on the code from [datasciencecampus/off-course](https://github.com/datasciencecampus/off-course). Following the specification detailed in [Eric S. Raymond's writeup](http://catb.org/gpsd/AIVDM.html) of AIVDM/AIVDO protocol and adapted [schwehr/libais](https://github.com/schwehr/libais) C++ implementation. 

## Features

Currently supporting message types:

| ID      | Description                                                  |
| ------- | ------------------------------------------------------------ |
| 1, 2, 3 | Position Report Class A                                      |
| 5       | Static and Voyage Related Data                               |
| 18      | Standard Class B CS Position Report (Equivalent of a 1,2,3 for ships using Class B equipment) |
| 24      | Static data repot (Equivalent of a Type 5 message for ships using Class B equipment.) |

* Checksum verification
* Null handling for each message type 
* Unit tests for each message type (test coverage 87%)

