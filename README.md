# SCH Client Integration Library
This repository contains a library to interact with the SimpleCampusHousing Integration API as well as scripts which utilize this library to interface with a variety of specific campus ERP systems. Additionally, a pair of scripts for importing and exporting comma separated values (csv) using the SimpleCampusHousing API is also included. Values are transmitted between these scripts and SCH over a secure REST API.

sch_client.py is the core Python library used to interact with the SimpleCampusHousing Integration API

The executable scripts expect a configuration file named config.json to exist in the same directory. All configuration and runtime options are set through this file. Sample config files are included for reference.

## Included ERP Sample Scripts
Sample integration scripts for the following ERP systems are included
* Sungard PowerCAMPUS
* Jenzabar EX

Pulling student information from a campus ERP system into SimpleCampusHousing can be accomplished using sync_pull.py. This script can connect to any database available over ODBC and run a custom SQL query. The output from this query is then mapped to a format compatible with SCH and saved to the SimpleCampusHousing web application via our REST API. The database connection string, SQL query, and JSON mapping file are set in config.json

Pushing housing and meal plan information from SimpleCampusHousing to specific ERP systems is accomplished via the "sync_push" series of scripts. A unique script is included for each sample ERP system (e.g. "sync_push_jenzabar.py").

Full integration employs first running sync_pull.py followed by the applicable sync_push script.

## CSV Import/Export
A pair of scripts (csv_import.py & csv_export.py) provide the ability to interface with SimpleCampusHousing using comma separated values as a common interchange format without connecting directly to the ERP database. Input/output configuration is done through settings in config.json. Mapping values in the import CSV file to those compatible with SimpleCampusHousing is done using the same JSON format as the sync_pull script.

### CSV Calculated Columns
Additional output columns can be added to the csv export file resulting from csv_export.py. These columns are defined by the config rule 'calculated_columns'.  This rule expects a dictionary where each key is the new column name. Values in this dictionary are arrays of rules for setting the value of the column. The first matching rule is used.

Each rule is itself a dictionary. The following keys can be used to define a rule:
* _field_ - The name of the input field to check. If no _field_ is set, the rule acts as a default and will always match.
* _value_ - The value of _field_ to match. If the value matches, sets the calculated column to _output_. If _value_ is not defined, _field_ being not null is considered a match
* _output_ - The value to output for this calculated column if the rule matches. If not defined, a blank value is used for output

## API Debug Reference
The following JSON can be used to directly test the HTTP PUT method of /resident in the SCH API. Provided a valid token, this data will save sample students to SimpleCampusHousing

    {
      "token":"6199$2a$10$Q7epLW2UZwzUhzmfRHJ/DOcpTP/qahLyn1DWXERMYcO9ORPIRoRqq",
      "data": [
          ["0034235", "John", "Doe", "M", "1/1/1994", "123 Awesome Street"],
          ["0063453", "Jane", "Smith", "F", "10/21/1995", "5634 W Grandeur Dr"],
          ["0012345", "Joseph", "Johnson", "M", "2/15/1987", "101 Random Ave"]
      ],
      "columns": [
          {"name": "id"},
          {"model": "Resident", "field": "firstName"},
          {"model": "Resident", "field": "lastName"},
          {"model": "Resident", "field": "gender"},
          {"model": "Resident", "field": "birthDate"},
          {"model": "Resident", "field": "addressPart", "arguments": ["address1"]}
      ]
    }
