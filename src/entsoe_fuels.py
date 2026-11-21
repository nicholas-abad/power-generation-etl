"""ENTSO-E generation production types (A73 psrType) and their fuel names.

One map for every path that writes ENTSO-E rows: the loader (src/database.py),
and a row the reviewed fuel exceptions move to another fuel
(src/entsoe_fuel_exceptions.py), so the same observation always gets the same
fuel_type string. Example: B05 -> "Fossil Hard coal", B01 -> "Biomass".
"""

PSR_TO_FUEL_TYPE = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
    "B25": "Energy storage",
}
