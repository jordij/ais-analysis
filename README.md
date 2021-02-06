AIS analysis
------------

You'll need to install docker and docker-compose on your machine. Then run:

```
$ docker-compose up -d
$ docker-compose run app /bin/bash
$ python app/app.py sample.json output.json
```

Where `sample.json` is your data file and `output.json` the file where results are stored in GeoJSON.

Be patient, the analysis will take a while