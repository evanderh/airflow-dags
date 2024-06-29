from legend import (
    tmp_legend_text,
    prate_legend_text,
    legend_text,
    pres_legend_text,
)

tmp_color_scale = [
    [50.0, 158,   1,  66], 
    [40.0, 213,  62,  79], 
    [35.0, 244, 109,  67], 
    [30.0, 253, 174,  97], 
    [25.0, 254, 224, 139], 
    [20.0, 255, 255, 191], 
    [15.0, 230, 245, 152], 
    [10.0, 171, 221, 164], 
    [ 5.0, 102, 194, 165], 
    [ 0.0,  50, 136, 189], 
    [-50.0, 94,  79, 162]
]
cloud_color_scale = [
    [100,  64,  64,  64], 
    [  0, 223, 223, 223], 
]

tmp_layer = {
    'band': {
        'element': 'TMP',
        'layer': '2-HTGL',
        'pdtn': '0',
    },
    'name': 'tmp',
    'scale': (-50, 50),
    'color_scale': tmp_color_scale,
    'legend_text': tmp_legend_text(),
}
aptmp_layer = {
    'band': {
        'element': 'APTMP',
        'layer': '2-HTGL',
        'pdtn': '0',
    },
    'name': 'aptmp',
    'scale': (-50, 50),
    'color_scale': tmp_color_scale,
    'legend_text': tmp_legend_text(),
}
trotmp_layer = {
    'band': {
        'element': 'TMP',
        'layer': '0-TRO',
        'pdtn': '0',
    },
    'name': 'trotmp',
    'scale': (-100, 0),
    'color_scale': [
        [   0.0,  50, 136, 189], 
        [ -50.0,  94,  79, 162],
        [-100.0, 223, 223, 223]
    ],
    'legend_text': tmp_legend_text(False),
}
prate_layer = {
    'band': {
        'element': 'PRATE',
        'layer': '0-SFC',
        'pdtn': '0',
    },
    'name': 'prate',
    'scale': (0, 32/3600),
    'color_scale': [
        [ 32/3600, 158,   1,  66],
        [ 16/3600, 213,  62,  79],
        [  8/3600, 244, 109,  67],
        [  4/3600, 253, 174,  97],
        [  2/3600, 171, 221, 164],
        [  1/3600, 102, 194, 165],
        [0.5/3600,  50, 136, 189],
        [       0, 223, 223, 223, 0],
    ],
    'legend_text': prate_legend_text,
}
lcdc_layer = {
    'band': {
        'element': 'LCDC',
        'layer': '0-LCY',
        'pdtn': '0',
    },
    'name': 'lcdc',
    'scale': (0, 100),
    'color_scale': cloud_color_scale,
    'legend_text': legend_text('% Cloud Cover'),
}
mcdc_layer = {
    'band': {
        'element': 'MCDC',
        'layer': '0-MCY',
        'pdtn': '0',
    },
    'name': 'mcdc',
    'scale': (0, 100),
    'color_scale': cloud_color_scale,
    'legend_text': legend_text('% Cloud Cover'),
}
hcdc_layer = {
    'band': {
        'element': 'HCDC',
        'layer': '0-HCY',
        'pdtn': '0',
    },
    'name': 'hcdc',
    'scale': (0, 100),
    'color_scale': cloud_color_scale,
    'legend_text': legend_text('% Cloud Cover'),
}
tcdc_layer = {
    'band': {
        'element': 'TCDC',
        'layer': '0-EATM',
        'pdtn': '0',
    },
    'name': 'tcdc',
    'scale': (0, 100),
    'color_scale': cloud_color_scale,
    'legend_text': legend_text('% Cloud Cover'),
}
pres_layer = {
    'band': {
        'element': 'PRMSL',
        'layer': '0-MSL',
        'pdtn': '0',
    },
    'name': 'pres',
    'scale': (98000, 103000),
    'color_scale': [
        [103000, 140,81,10],
        [102000, 216,179,101],
        [101000, 245,245,245],
        [100000, 90,180,172],
        [ 98000, 1,102,94]
    ],
    'legend_text': pres_legend_text,
}
rh_layer = {
    'band': {
        'element': 'RH',
        'layer': '2-HTGL',
        'pdtn': '0',
    },
    'name': 'rh',
    'scale': (0, 100),
    'color_scale': [
        [100, 140,81,10],
        [ 75, 216,179,101],
        [ 50, 245,245,245],
        [ 25, 90,180,172],
        [  0, 1,102,94]
    ],
    'legend_text': legend_text('% Humidity'),
}
vis_layer = {
    'band': {
        'element': 'VIS',
        'layer': '0-SFC',
        'pdtn': '0',
    },
    'name': 'vis',
    'scale': (0, 25000),
    'color_scale': [
        [25000, 223, 223, 223], 
        [    0,  32,  32,  32], 
    ],
    'legend_text': legend_text(' m'),
}
gust_layer = {
    'band': {
        'element': 'GUST',
        'layer': '0-SFC',
        'pdtn': '0',
    },
    'name': 'gust',
    'scale': (0, 40),
    'color_scale': [
        [40.0, 189,0,38],
        [30.0, 227,26,28],
        [20.0, 252,78,42],
        [10.0, 253,141,60],
        [ 5.0, 254,178,76],
        [ 0.0, 223,223,223],
    ],
    'legend_text': legend_text(' m/s'),
}
sunsd_layer = {
    'band': {
        'element': 'SUNSD',
        'layer': '0-SFC',
        'pdtn': '0',
    },
    'name': 'sunsd',
    'scale': (0, 3600),
    'color_scale': [
        [3600, 223, 223, 223], 
        [   0,  32,  32,  32], 
    ],
    'legend_text': legend_text(' sec'),
}

LAYERS = [
    tmp_layer,
    prate_layer,
    pres_layer,
    rh_layer,
    gust_layer,
]
