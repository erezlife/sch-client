# Sample resident put json:

    {
      "token":"6199$2a$10$Q7epLW2UZwzUhzmfRHJ/DOcpTP/qahLyn1DWXERMYcO9ORPIRoRqq",
      "data": [
          ["0034235", "John", "Doe", "M", "1/1/1994", "123 Awesome Street"],
          ["0063453", "Jane", "Smith", "F", "10/21/1995", "5634 W Grandeur Dr"],
          ["0012345", "Joseph", "Smith", "M", "2/15/1987", "101 Random Ave"]
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
