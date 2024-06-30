from datetime import datetime
from osgeo import gdal

def get_band(filename, filter_band):
    # https://gdal.org/programs/gdalinfo.html
    info = gdal.Info(filename, format='json')

    def find_band(band):
        metadata = band['metadata']['']
        return (filter_band['element'] == metadata['GRIB_ELEMENT'] and
                filter_band['layer'] == metadata['GRIB_SHORT_NAME'] and
                filter_band['pdtn'] == metadata['GRIB_PDS_PDTN'])

    return next(filter(find_band, info['bands']))

def translate_vector(dest, src, element):
    band = get_band(src, {
        'element': element,
        'layer': '10-HTGL',
        'pdtn': '0',
    })
    options = f"-of Gtiff -b {band['band']} -a_nodata none"
    # https://gdal.org/programs/gdal_translate.html
    gdal.Translate(dest, src, options=options)

def get_vector_metadata(src):
    info = gdal.Info(src, format='json')
    metadata = info['bands'][0]['metadata']['']

    return {
        'scanMode': '0',
        'refTime': datetime.fromtimestamp(int(metadata['GRIB_REF_TIME'])).isoformat()+'Z',
        'forecastTime': int(metadata['GRIB_FORECAST_SECONDS']) / 3600,
        'parameterCategory': metadata['GRIB_PDS_TEMPLATE_NUMBERS'][0],
        'parameterNumber':   metadata['GRIB_PDS_TEMPLATE_NUMBERS'][2],
        'nx': info['size'][0],
        'ny': info['size'][1],
        'lo1': -180.0,
        'la1': 90.0,
        'dx': 1.0,
        'dy': 1.0
    }

def translate_layer(dest, src, layer):
    band = get_band(src, layer['band'])
    options = ' '.join([
        f'-b {band["band"]}',
        f'-scale {layer["scale"][0]} {layer["scale"][1]}',
        '-ot Byte',
        '-of Gtiff',
        '-a_nodata none',
    ])
    # https://gdal.org/programs/gdal_translate.html
    gdal.Translate(dest, src, options=options)
