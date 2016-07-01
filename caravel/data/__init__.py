"""Loads datasets, dashboards and slices in a new caravel instance"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import gzip
import json
import os
import textwrap
import datetime
import random

import pandas as pd
from sqlalchemy import String, DateTime, Date, Float

from caravel import app, db, models, utils

# Shortcuts
DB = models.Database
Slice = models.Slice
TBL = models.SqlaTable
Dash = models.Dashboard

config = app.config

DATA_FOLDER = os.path.join(config.get("BASE_DIR"), 'data')


def get_or_create_db(session):
    print("Creating database reference")
    dbobj = session.query(DB).filter_by(database_name='main').first()
    if not dbobj:
        dbobj = DB(database_name="main")
    print(config.get("SQLALCHEMY_DATABASE_URI"))
    dbobj.sqlalchemy_uri = config.get("SQLALCHEMY_DATABASE_URI")
    session.add(dbobj)
    session.commit()
    return dbobj


def get_or_create_elasticsearch_cluster(urls):
    session = db.session
    print("Creating ElasticSearch cluster reference")
    clusterobj = session.query(models.ElasticsearchCluster).filter_by(cluster_name='ElasticSearch examples').first()
    if not clusterobj:
        clusterobj = models.ElasticsearchCluster(cluster_name='ElasticSearch examples')
    clusterobj.urls = urls
    db.session.add(clusterobj)

    db.session.commit()
    return clusterobj


def import_data(file_name, table_name, description, dtype, es_cluster):
    if es_cluster:
        ds_name = "example-%s" % table_name
        print("Dumping to ElasticSearch index [%s] from [%s]" % (ds_name, file_name))
        with gzip.open(os.path.join(DATA_FOLDER, file_name)) as f:
            data = json.loads(f.read())
        es = es_cluster.get_elasticsearch_client()
        es.indices.delete(
            index=ds_name,
            ignore=404)
        body='\n'.join('{"index":{}}\n%s\n' % json.dumps(row) for row in data)
        es.bulk(
            index=ds_name,
            doc_type=table_name,
            body=body)
        print("Creating ElasticSearch datasource [%s] reference" % ds_name)
        ds = db.session.query(models.ElasticsearchDatasource).filter_by(datasource_name=ds_name).first()
        if not ds:
            ds = models.ElasticsearchDatasource(datasource_name=ds_name)
        if table_name == 'birth_names':
            ds.main_dttm_col = 'ds'
        elif table_name == 'wb_health_population':
            ds.main_dttm_col = 'year'
        ds.cluster = es_cluster
        ds.index_name = ds_name
        ds.description = description
        ds.is_featured = True
        db.session.merge(ds)
        db.session.commit()
        ds.refresh_fields()
        ds.generate_metrics()
    else:
        print("Creating table [%s] from [%s]" % (table_name, file_name))
        with gzip.open(os.path.join(DATA_FOLDER, file_name)) as f:
            pdf = pd.read_json(f)
        pdf.columns = [col.replace('.', '_') for col in pdf.columns]
        if table_name == 'birth_names':
            pdf.ds = pd.to_datetime(pdf.ds, unit='ms')
        elif table_name == 'wb_health_population':
            pdf.year = pd.to_datetime(pdf.year)
        elif table_name == 'random_time_series':
            pdf.ds = pd.to_datetime(pdf.ds, unit='s')

        pdf.to_sql(
            table_name,
            db.engine,
            if_exists='replace',
            chunksize=500,
            dtype=dtype,
            index=False)

        print("Creating table [%s] reference" % table_name)
        ds = db.session.query(TBL).filter_by(table_name=table_name).first()
        if not ds:
            ds = TBL(table_name=table_name)
        ds.is_featured = True
        if table_name == 'birth_names':
            ds.main_dttm_col = 'ds'
        elif table_name == 'wb_health_population':
            ds.main_dttm_col = 'year'
        elif table_name == 'random_time_series':
            ds.main_dttm_col = 'ds'
            ds.is_featured = False
        ds.database = get_or_create_db(db.session)
        ds.description = description
        db.session.merge(ds)
        db.session.commit()
        ds.fetch_metadata()
    print("-" * 80)
    return ds



def get_or_create_slice(slice_name, viz_type, datasource, defaults, params):
    table = None
    elasticsearch_datasource = None
    if datasource.type == 'elasticsearch':
        slice_name = slice_name + ' (ES)'
        elasticsearch_datasource = datasource
    else:
        table = datasource
    p = defaults.copy()
    p['slice_name'] = slice_name
    p['viz_type'] = viz_type
    p['datasource_type'] = datasource.type
    p['datasource_id'] = datasource.id
    p['datasource_name'] = datasource.name
    p.update(params)
    o = db.session.query(Slice).filter_by(slice_name=slice_name).first()
    if o:
        db.session.delete(o)
    slc = Slice(
        slice_name=slice_name,
        viz_type=viz_type,
        datasource_type=datasource.type,
        table=table,
        elasticsearch_datasource=elasticsearch_datasource,
        params=json.dumps(p, indent=4, sort_keys=True))
    db.session.add(slc)
    db.session.commit()
    return slc


def load_energy(es_cluster=None):
    """Loads an energy related dataset to use with sankey and graphs"""
    ds = import_data(
        file_name='energy.json.gz',
        table_name='energy_usage',
        description="Energy consumption",
        dtype={
            'source': String(255),
            'target': String(255),
            'value': Float(),
        },
        es_cluster=es_cluster)
    get_or_create_slice(
        slice_name="Energy Sankey",
        viz_type='sankey',
        datasource=ds,
        defaults={},
        params={
            "collapsed_fieldsets": "",
            "flt_col_0": "source",
            "flt_eq_0": "",
            "flt_op_0": "in",
            "groupby": [
                "source",
                "target"
            ],
            "having": "",
            "metric": "sum__value",
            "row_limit": "5000",
            "slice_id": "",
            "where": ""
        }
    )

    get_or_create_slice(
        slice_name="Energy Force Layout",
        viz_type='directed_force',
        datasource=ds,
        defaults={},
        params={
            "charge": "-500",
            "collapsed_fieldsets": "",
            "flt_col_0": "source",
            "flt_eq_0": "",
            "flt_op_0": "in",
            "groupby": [
                "source",
                "target"
            ],
            "having": "",
            "link_length": "200",
            "metric": "sum__value",
            "row_limit": "5000",
            "where": ""
        }
    )

    get_or_create_slice(
        slice_name="Heatmap",
        viz_type='heatmap',
        datasource=ds,
        defaults={},
        params={
            "all_columns_x": "source",
            "all_columns_y": "target",
            "canvas_image_rendering": "pixelated",
            "collapsed_fieldsets": "",
            "flt_col_0": "source",
            "flt_eq_0": "",
            "flt_op_0": "in",
            "having": "",
            "linear_color_scheme": "blue_white_yellow",
            "metric": "sum__value",
            "normalize_across": "heatmap",
            "where": "",
            "xscale_interval": "1",
            "yscale_interval": "1"
        }
    )


def load_world_bank_health_n_pop(es_cluster=None):
    """Loads the world bank health dataset, slices and a dashboard"""
    ds = import_data(
        file_name='countries.json.gz',
        table_name='wb_health_population',
        description=utils.readfile(os.path.join(DATA_FOLDER, 'countries.md')),
        dtype={
            'year': DateTime(),
            'country_code': String(3),
            'country_name': String(255),
            'region': String(255),
        },
        es_cluster=es_cluster)

    defaults = {
        "compare_lag": "10",
        "compare_suffix": "o10Y",
        "limit": "25",
        "granularity": "year",
        "groupby": [],
        "metric": 'sum__SP_POP_TOTL',
        "metrics": ["sum__SP_POP_TOTL"],
        "row_limit": config.get("ROW_LIMIT"),
        "since": "2014-01-01",
        "until": "2014-01-01",
        "where": "",
        "markup_type": "markdown",
        "country_fieldtype": "cca3",
        "secondary_metric": "sum__SP_POP_TOTL",
        "entity": "country_code",
        "show_bubbles": "y",
    }

    print("Creating slices")
    slices = [
        get_or_create_slice(
            slice_name="Region Filter",
            viz_type='filter_box',
            datasource=ds,
            defaults=defaults,
            params={
                "groupby": ['region', 'country_name'],
            }),
        get_or_create_slice(
            slice_name="World's Population",
            viz_type='big_number',
            datasource=ds,
            defaults=defaults,
            params={
                "since": '2000',
                "compare_lag": "10",
                "metric": 'sum__SP_POP_TOTL',
                "compare_suffix": "over 10Y",
            }),
        get_or_create_slice(
            slice_name="Most Populated Countries",
            viz_type='table',
            datasource=ds,
            defaults=defaults,
            params={
                "metrics": ["sum__SP_POP_TOTL"],
                "groupby": ['country_name'],
            }),
        get_or_create_slice(
            slice_name="Growth Rate",
            viz_type='line',
            datasource=ds,
            defaults=defaults,
            params={
                "since": "1960-01-01",
                "metrics": ["sum__SP_POP_TOTL"],
                "num_period_compare": "10",
                "groupby": ['country_name'],
            }),
        get_or_create_slice(
            slice_name="% Rural",
            viz_type='world_map',
            datasource=ds,
            defaults=defaults,
            params={
                "metric": "sum__SP_RUR_TOTL_ZS",
                "num_period_compare": "10",
            }),
        get_or_create_slice(
            slice_name="Life Expexctancy VS Rural %",
            viz_type='bubble',
            datasource=ds,
            defaults=defaults,
            params={
                "since": "2011-01-01",
                "until": "2011-01-01",
                "series": "region",
                "limit": "0",
                "entity": "country_name",
                "x": "sum__SP_RUR_TOTL_ZS",
                "y": "sum__SP_DYN_LE00_IN",
                "size": "sum__SP_POP_TOTL",
                "max_bubble_size": "50",
                "flt_col_1": "country_code",
                "flt_op_1": "not in",
                "flt_eq_1": "TCA,MNP,DMA,MHL,MCO,SXM,CYM,TUV,IMY,KNA,ASM,ADO,AMA,PLW",
                "num_period_compare": "10",
            }),
        get_or_create_slice(
            slice_name="Rural Breakdown",
            viz_type='sunburst',
            datasource=ds,
            defaults=defaults,
            params={
                "groupby": ["region", "country_name"],
                "secondary_metric": "sum__SP_RUR_TOTL",
                "since": "2011-01-01",
                "until": "2011-01-01",
            }),
        get_or_create_slice(
            slice_name="World's Pop Growth",
            viz_type='area',
            datasource=ds,
            defaults=defaults,
            params={
                "since": "1960-01-01",
                "until": "now",
                "groupby": ["region"],
            }),
        get_or_create_slice(
            slice_name="Box plot",
            viz_type='box_plot',
            datasource=ds,
            defaults=defaults,
            params={
                "since": "1960-01-01",
                "until": "now",
                "whisker_options": "Tukey",
                "groupby": ["region"],
            }),
        get_or_create_slice(
            slice_name="Treemap",
            viz_type='treemap',
            datasource=ds,
            defaults=defaults,
            params={
                "since": "1960-01-01",
                "until": "now",
                "metrics": ["sum__SP_POP_TOTL"],
                "groupby": ["region", "country_code"],
            }),
        get_or_create_slice(
            slice_name="Parallel Coordinates",
            viz_type='para',
            datasource=ds,
            defaults=defaults,
            params={
                "since": "2011-01-01",
                "until": "2011-01-01",
                "limit": 100,
                "metrics": [
                    "sum__SP_POP_TOTL",
                    'sum__SP_RUR_TOTL_ZS',
                    'sum__SH_DYN_AIDS'],
                "secondary_metric": 'sum__SP_POP_TOTL',
                "series": ["country_name"],
            }),
    ]

    print("Creating a World's Health Bank dashboard")
    dash_name = "World's Bank Data"
    slug = "world_health"
    dash = db.session.query(Dash).filter_by(slug=slug).first()

    if not dash:
        dash = Dash()
    js = textwrap.dedent("""\
    [
        {
            "size_y": 2,
            "size_x": 3,
            "col": 10,
            "slice_id": "22",
            "row": 1
        },
        {
            "size_y": 3,
            "size_x": 3,
            "col": 10,
            "slice_id": "23",
            "row": 3
        },
        {
            "size_y": 8,
            "size_x": 3,
            "col": 1,
            "slice_id": "24",
            "row": 1
        },
        {
            "size_y": 3,
            "size_x": 6,
            "col": 4,
            "slice_id": "25",
            "row": 6
        },
        {
            "size_y": 5,
            "size_x": 6,
            "col": 4,
            "slice_id": "26",
            "row": 1
        },
        {
            "size_y": 4,
            "size_x": 6,
            "col": 7,
            "slice_id": "27",
            "row": 9
        },
        {
            "size_y": 3,
            "size_x": 3,
            "col": 10,
            "slice_id": "28",
            "row": 6
        },
        {
            "size_y": 4,
            "size_x": 6,
            "col": 1,
            "slice_id": "29",
            "row": 9
        },
        {
            "size_y": 4,
            "size_x": 5,
            "col": 8,
            "slice_id": "30",
            "row": 13
        },
        {
            "size_y": 4,
            "size_x": 7,
            "col": 1,
            "slice_id": "31",
            "row": 13
        }
    ]
    """)
    l = json.loads(js)
    for i, pos in enumerate(l):
        pos['slice_id'] = str(slices[i].id)

    dash.dashboard_title = dash_name
    dash.position_json = json.dumps(l, indent=4)
    dash.slug = slug

    dash.slices = slices[:-1]
    db.session.merge(dash)
    db.session.commit()


def load_css_templates(es_cluster=None):
    """Loads 2 css templates to demonstrate the feature"""
    print('Creating default CSS templates')
    CSS = models.CssTemplate  # noqa

    obj = db.session.query(CSS).filter_by(template_name='Flat').first()
    if not obj:
        obj = CSS(template_name="Flat")
    css = textwrap.dedent("""\
    .gridster div.widget {
        transition: background-color 0.5s ease;
        background-color: #FAFAFA;
        border: 1px solid #CCC;
        box-shadow: none;
        border-radius: 0px;
    }
    .gridster div.widget:hover {
        border: 1px solid #000;
        background-color: #EAEAEA;
    }
    .navbar {
        transition: opacity 0.5s ease;
        opacity: 0.05;
    }
    .navbar:hover {
        opacity: 1;
    }
    .chart-header .header{
        font-weight: normal;
        font-size: 12px;
    }
    /*
    var bnbColors = [
        //rausch    hackb      kazan      babu      lima        beach     tirol
        '#ff5a5f', '#7b0051', '#007A87', '#00d1c1', '#8ce071', '#ffb400', '#b4a76c',
        '#ff8083', '#cc0086', '#00a1b3', '#00ffeb', '#bbedab', '#ffd266', '#cbc29a',
        '#ff3339', '#ff1ab1', '#005c66', '#00b3a5', '#55d12e', '#b37e00', '#988b4e',
     ];
    */
    """)
    obj.css = css
    db.session.merge(obj)
    db.session.commit()

    obj = (
        db.session.query(CSS).filter_by(template_name='Courier Black').first())
    if not obj:
        obj = CSS(template_name="Courier Black")
    css = textwrap.dedent("""\
    .gridster div.widget {
        transition: background-color 0.5s ease;
        background-color: #EEE;
        border: 2px solid #444;
        border-radius: 15px;
        box-shadow: none;
    }
    h2 {
        color: white;
        font-size: 52px;
    }
    .navbar {
        box-shadow: none;
    }
    .gridster div.widget:hover {
        border: 2px solid #000;
        background-color: #EAEAEA;
    }
    .navbar {
        transition: opacity 0.5s ease;
        opacity: 0.05;
    }
    .navbar:hover {
        opacity: 1;
    }
    .chart-header .header{
        font-weight: normal;
        font-size: 12px;
    }
    .nvd3 text {
        font-size: 12px;
        font-family: inherit;
    }
    body{
        background: #000;
        font-family: Courier, Monaco, monospace;;
    }
    /*
    var bnbColors = [
        //rausch    hackb      kazan      babu      lima        beach     tirol
        '#ff5a5f', '#7b0051', '#007A87', '#00d1c1', '#8ce071', '#ffb400', '#b4a76c',
        '#ff8083', '#cc0086', '#00a1b3', '#00ffeb', '#bbedab', '#ffd266', '#cbc29a',
        '#ff3339', '#ff1ab1', '#005c66', '#00b3a5', '#55d12e', '#b37e00', '#988b4e',
     ];
    */
    """)
    obj.css = css
    db.session.merge(obj)
    db.session.commit()


def load_birth_names(es_cluster=None):
    """Loading birth name dataset from a zip file in the repo"""
    ds = import_data(
        file_name='birth_names.json.gz',
        table_name='birth_names',
        description='',
        dtype={
            'ds': DateTime,
            'gender': String(16),
            'state': String(10),
            'name': String(255),
        },
        es_cluster=es_cluster)



    defaults = {
        "compare_lag": "10",
        "compare_suffix": "o10Y",
        "flt_op_1": "in",
        "limit": "25",
        "granularity": "ds",
        "groupby": [],
        "metric": 'sum__num',
        "metrics": ["sum__num"],
        "row_limit": config.get("ROW_LIMIT"),
        "since": "100 years ago",
        "until": "now",
        "where": "",
        "markup_type": "markdown",
    }

    print("Creating some slices")
    slices = [
        get_or_create_slice(
            slice_name="Girls",
            viz_type='table',
            datasource=ds,
            defaults=defaults,
            params={
                "groupby": ['name'],
                "flt_col_1": 'gender',
                "flt_eq_1": "girl",
                "row_limit": 50,
            }),
        get_or_create_slice(
            slice_name="Boys",
            viz_type='table',
            datasource=ds,
            defaults=defaults,
            params={
                "groupby": ['name'],
                "flt_col_1": 'gender',
                "flt_eq_1": "boy",
                "row_limit": 50,
            }),
        get_or_create_slice(
            slice_name="Participants",
            viz_type='big_number',
            datasource=ds,
            defaults=defaults,
            params={
                "granularity": "ds",
                "compare_lag": "5",
                "compare_suffix": "over 5Y",
            }),
        get_or_create_slice(
            slice_name="Genders",
            viz_type='pie',
            datasource=ds,
            defaults=defaults,
            params={
                "groupby": ['gender'],
            }),
        get_or_create_slice(
            slice_name="Genders by State",
            viz_type='dist_bar',
            datasource=ds,
            defaults=defaults,
            params={
                "flt_eq_1": "other",
                "metrics": ['sum__sum_girls', 'sum__sum_boys'],
                "groupby": ['state'],
                "flt_op_1": 'not in',
                "flt_col_1": 'state',
            }),
        get_or_create_slice(
            slice_name="Trends",
            viz_type='line',
            datasource=ds,
            defaults=defaults,
            params={
                "groupby": ['name'],
                "granularity": 'ds',
                "rich_tooltip": 'y',
                "show_legend": 'y',
            }),
        get_or_create_slice(
            slice_name="Title",
            viz_type='markup',
            datasource=ds,
            defaults=defaults,
            params={
                "markup_type": "html",
                "code": """\
<div style="text-align:center">
    <h1>Birth Names Dashboard</h1>
    <p>
        The source dataset came from
        <a href="https://github.com/hadley/babynames">[here]</a>
    </p>
    <img src="http://monblog.system-linux.net/image/tux/baby-tux_overlord59-tux.png">
</div>
""",
            }),
        get_or_create_slice(
            slice_name="Name Cloud",
            viz_type='word_cloud',
            datasource=ds,
            defaults=defaults,
            params={
                "size_from": "10",
                "series": 'name',
                "size_to": "70",
                "rotation": "square",
                "limit": '100',
            }),
        get_or_create_slice(
            slice_name="Pivot Table",
            viz_type='pivot_table',
            datasource=ds,
            defaults=defaults,
            params={
                "metrics": ['sum__num'],
                "groupby": ['name'],
                "columns": ['state'],
            }),
        get_or_create_slice(
            slice_name="Number of Girls",
            viz_type='big_number_total',
            datasource=ds,
            defaults=defaults,
            params={
                "granularity": "ds",
                "flt_col_1": 'gender',
                "flt_eq_1": 'girl',
                "subheader": 'total female participants',
            }),
    ]

    print("Creating a dashboard")
    dash = db.session.query(Dash).filter_by(dashboard_title="Births").first()

    if not dash:
        dash = Dash()
    js = textwrap.dedent("""\
        [
            {
                "size_y": 4,
                "size_x": 2,
                "col": 8,
                "slice_id": "85",
                "row": 7
            },
            {
                "size_y": 4,
                "size_x": 2,
                "col": 10,
                "slice_id": "86",
                "row": 7
            },
            {
                "size_y": 2,
                "size_x": 2,
                "col": 1,
                "slice_id": "87",
                "row": 1
            },
            {
                "size_y": 2,
                "size_x": 2,
                "col": 3,
                "slice_id": "88",
                "row": 1
            },
            {
                "size_y": 3,
                "size_x": 7,
                "col": 5,
                "slice_id": "89",
                "row": 4
            },
            {
                "size_y": 4,
                "size_x": 7,
                "col": 1,
                "slice_id": "90",
                "row": 7
            },
            {
                "size_y": 3,
                "size_x": 3,
                "col": 9,
                "slice_id": "91",
                "row": 1
            },
            {
                "size_y": 3,
                "size_x": 4,
                "col": 5,
                "slice_id": "92",
                "row": 1
            },
            {
                "size_y": 4,
                "size_x": 4,
                "col": 1,
                "slice_id": "93",
                "row": 3
            }
        ]
        """)
    l = json.loads(js)
    for i, pos in enumerate(l):
        pos['slice_id'] = str(slices[i].id)
    dash.dashboard_title = "Births"
    dash.position_json = json.dumps(l, indent=4)
    dash.slug = "births"
    dash.slices = slices[:-1]
    db.session.merge(dash)
    db.session.commit()


def load_unicode_test_data(es_cluster=None):
    """Loading unicode test dataset from a csv file in the repo"""
    df = pd.read_csv(os.path.join(DATA_FOLDER, 'unicode_utf8_unixnl_test.csv'),
                     encoding="utf-8")
    # generate date/numeric data
    df['date'] = datetime.datetime.now().date()
    df['value'] = [random.randint(1, 100) for _ in range(len(df))]
    df.to_sql(
        'unicode_test',
        db.engine,
        if_exists='replace',
        chunksize=500,
        dtype={
            'phrase': String(500),
            'short_phrase': String(10),
            'with_missing': String(100),
            'date': Date(),
            'value': Float(),
        },
        index=False)
    print("Done loading table!")
    print("-" * 80)

    print("Creating table reference")
    obj = db.session.query(TBL).filter_by(table_name='unicode_test').first()
    if not obj:
        obj = TBL(table_name='unicode_test')
    obj.main_dttm_col = 'date'
    obj.database = get_or_create_db(db.session)
    obj.is_featured = False
    db.session.merge(obj)
    db.session.commit()
    obj.fetch_metadata()
    ds = obj

    slice_data = {
        "flt_op_1": "in",
        "granularity": "date",
        "groupby": [],
        "metric": 'sum__value',
        "row_limit": config.get("ROW_LIMIT"),
        "since": "100 years ago",
        "until": "now",
        "where": "",
        "viz_type": "word_cloud",
        "size_from": "10",
        "series": "short_phrase",
        "size_to": "70",
        "rotation": "square",
        "limit": "100",
    }

    print("Creating a slice")
    slc = get_or_create_slice(
        slice_name="Unicode Cloud",
        viz_type='word_cloud',
        datasource=ds,
        defaults={},
        params=slice_data,
    )

    print("Creating a dashboard")
    dash = db.session.query(Dash).filter_by(dashboard_title="Unicode Test").first()

    if not dash:
        dash = Dash()
    pos = {
        "size_y": 4,
        "size_x": 4,
        "col": 1,
        "row": 1,
        "slice_id": slc.id,
    }
    dash.dashboard_title = "Unicode Test"
    dash.position_json = json.dumps([pos], indent=4)
    dash.slug = "unicode-test"
    dash.slices = [slc]
    db.session.merge(dash)
    db.session.commit()


def load_random_time_series_data(es_cluster=None):
    """Loading random time series data from a zip file in the repo"""
    ds = import_data(
        file_name='random_time_series.json.gz',
        table_name='random_time_series',
        description="Random time series",
        dtype={
            'ds': DateTime,
        },
        es_cluster=es_cluster)

    get_or_create_slice(
        slice_name="Calendar Heatmap",
        viz_type='cal_heatmap',
        datasource=ds,
        defaults={},
        params={
            "granularity": "day",
            "row_limit": config.get("ROW_LIMIT"),
            "since": "1 year ago",
            "until": "now",
            "where": "",
            "domain_granularity": "month",
            "subdomain_granularity": "day",
        },
    )
