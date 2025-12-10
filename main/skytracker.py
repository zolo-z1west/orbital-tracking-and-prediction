from skyfield.api import load, EarthSatellite

ts = load.timescale()
tle = [
    "ISS (ZARYA)",
    "1 25544U 98067A   24343.34461806  .00016717  00000-0  30709-3 0  9992",
    "2 25544  51.6448 297.3353 0007289  34.8254 116.1037 15.50479884640947"
]

sat = EarthSatellite(tle[1], tle[2], tle[0], ts)

t=ts.now()
geocentric = sat.at(t)
lat, lon, alt = geocentric.subpoint().latitude.degrees, geocentric.subpoint().longitude.degrees, geocentric.subpoint().elevation.km

print(f"Lat: {lat}, Lon: {lon}, Alt: {alt} km")
