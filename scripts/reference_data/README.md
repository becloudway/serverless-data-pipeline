# Generate reference data and create geojson for locations

* download xml
* convert to json
* generate csv of reference data
* convert to geojson

GEOJSON
```json
{
  "type": "FeatureCollection",
  "features": [    
    {
      "type": "Feature",
      "id": 0,
      "geometry": {
        "type": "Point",
        "coordinates": [
          4.452993707,
          51.21587307
        ]
      },
      "properties": {
          "name": "Dinagat Islands"
        }
    }
  ]
}
```

## Use the script
* install dependencies: `pip install -r requirements.txt`
* run `python generate_reference_and_geojson.py`

## Test the script
* install dependencies: `pip install -r requirements-dev.txt`
* run `pytest test-script.py`

```
