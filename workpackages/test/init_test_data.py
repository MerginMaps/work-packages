from osgeo import ogr, osr


def create_gpkg(filename):
    return ogr.GetDriverByName("GPKG").CreateDataSource(filename)


def open_gpkg(filename):
    return ogr.Open(filename, update=1)


def create_crs_from_epsg(epsg_code):
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    return srs


def create_layer(gpkg_ds, layer_name, ogr_geom_type, crs_epsg_code, fields):
    """Creates a table in GeoPackage with given name, geometry type, CRS and fields.

    Supported geometry types: ogr.wkbPoint, ogr.wkbLineString, ogr.wkbPolygon, ...

    "Fields" is a list of tuples (name, ogr_type).
    Supported field types:
    - numeric: ogr.OFTInteger, ogr.OFTReal
    - text: ogr.OFTString
    - time: ogr.OFTDate, ogr.OFTDateTime
    """
    lyr = gpkg_ds.CreateLayer(
        layer_name,
        geom_type=ogr_geom_type,
        srs=create_crs_from_epsg(crs_epsg_code) if crs_epsg_code is not None else None,
        options=["GEOMETRY_NAME=geometry"],
    )

    lyr.StartTransaction()
    for field_name, field_type in fields:
        lyr.CreateField(ogr.FieldDefn(field_name, field_type))
    lyr.CommitTransaction()
    return lyr


def open_layer(gpkg_ds, layer_name):
    """Returns layer object given open GPKG connection and layer name"""
    return gpkg_ds.GetLayer(layer_name)


def create_feature(lyr, geom_wkt, field_values, fid=-1):
    """
    Adds a new feature to the layer. Field_values is a dictionary.
    """
    feat = ogr.Feature(lyr.GetLayerDefn())

    if geom_wkt:
        geom = ogr.CreateGeometryFromWkt(geom_wkt)
        feat.SetGeometry(geom)

    feat.SetFID(fid)
    for field_name, field_value in field_values.items():
        feat.SetField(field_name, field_value)

    lyr.StartTransaction()
    assert lyr.CreateFeature(feat) == 0, "cannot create feature"
    lyr.CommitTransaction()


def delete_feature(layer, fid):
    """Deletes a feature given by its ID"""
    layer.DeleteFeature(fid)  # feat.GetFID()


def update_feature(layer, fid, field_values):
    """Updates attributes of the feature given by its ID"""
    feat = layer.GetFeature(fid)
    assert feat is not None, f"Unable to find feature fid={fid}"
    for field_name, field_value in field_values.items():
        feat.SetField(field_name, field_value)
    layer.SetFeature(feat)


def open_layer_and_create_feature(filename, layer_name, geom_wkt, field_values, fid=-1):
    gpkg = open_gpkg(filename)
    lyr = open_layer(gpkg, layer_name)
    create_feature(lyr, geom_wkt, field_values, fid)


def open_layer_and_update_feature(filename, layer_name, fid, field_values):
    gpkg = open_gpkg(filename)
    lyr = open_layer(gpkg, layer_name)
    update_feature(lyr, fid, field_values)


def open_layer_and_delete_feature(filename, layer_name, fid):
    gpkg = open_gpkg(filename)
    lyr = open_layer(gpkg, layer_name)
    delete_feature(lyr, fid)


def create_farm_dataset(gpkg_path):
    gpkg_ds = create_gpkg(gpkg_path)

    layer_farms = create_layer(
        gpkg_ds,
        "farms",
        ogr.wkbPolygon,
        3857,
        [
            ("name", ogr.OFTString),
            ("owner", ogr.OFTString),
        ],
    )
    layer_trees = create_layer(
        gpkg_ds,
        "trees",
        ogr.wkbPoint,
        3857,
        [
            ("tree_species_id", ogr.OFTInteger),
            ("farm_id", ogr.OFTInteger),
            ("age_years", ogr.OFTInteger),
        ],
    )
    layer_tree_species = create_layer(
        gpkg_ds,
        "tree_species",
        ogr.wkbNone,
        None,
        [
            ("name", ogr.OFTString),
            ("name_latin", ogr.OFTString),
        ],
    )

    create_feature(layer_tree_species, None, {"name": "Apple tree", "name_latin": "Malus domestica"}, fid=1)
    create_feature(layer_tree_species, None, {"name": "Orange tree", "name_latin": "Citrus sinensis"}, fid=2)
    create_feature(layer_tree_species, None, {"name": "Mango tree", "name_latin": "Mangifera indica"}, fid=3)

    create_feature(
        layer_farms, "POLYGON((5 5,10 5,10 10,5 10,5 5))", {"name": "Oasis Gardens", "owner": "Emma Johnston"}, fid=1
    )
    create_feature(
        layer_farms,
        "POLYGON((15 5,20 5,20 10,15 10,15 5))",
        {"name": "Tranquility Estate", "owner": "Emma Johnston"},
        fid=2,
    )
    create_feature(
        layer_farms,
        "POLYGON((15 15,20 15,20 20,15 20,15 15))",
        {"name": "Rainbow Farm", "owner": "Lily Fleming"},
        fid=3,
    )
    create_feature(
        layer_farms, "POLYGON((5 15,10 15,10 20,5 20,5 15))", {"name": "Melody Orchard", "owner": "Kyle Flynn"}, fid=4
    )

    # Oasis garden - only apples (Emma)
    create_feature(layer_trees, "POINT(6 6)", {"tree_species_id": 1, "farm_id": 1})
    create_feature(layer_trees, "POINT(8 7)", {"tree_species_id": 1, "farm_id": 1})
    create_feature(layer_trees, "POINT(7 8)", {"tree_species_id": 1, "farm_id": 1})

    # Tranquility estate - mix of trees (Emma)
    create_feature(layer_trees, "POINT(16 6)", {"tree_species_id": 1, "farm_id": 2})
    create_feature(layer_trees, "POINT(18 7)", {"tree_species_id": 2, "farm_id": 2})
    create_feature(layer_trees, "POINT(17 8)", {"tree_species_id": 3, "farm_id": 2})

    # Rainbow farm - just one mango tree (Lily)
    create_feature(layer_trees, "POINT(17.5 17.5)", {"tree_species_id": 3, "farm_id": 3})

    # Melody orchard - a mix (Kyle)
    create_feature(layer_trees, "POINT(8 17)", {"tree_species_id": 2, "farm_id": 4})
    create_feature(layer_trees, "POINT(7 18)", {"tree_species_id": 3, "farm_id": 4})
