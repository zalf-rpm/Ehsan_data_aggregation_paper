#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF))

from collections import defaultdict
from datetime import date, timedelta
import json
import numpy as np
from pyproj import CRS, Transformer
import sqlite3
import sys
import time
import zmq

import monica_io3
import soil_io3
import monica_run_lib as Mrunlib

PATHS = {
     # adjust the local path to your environment
    "seserman": {
        "include-file-base-path": "data/params/",
        "path-to-climate-dir": "/Climate/1Km/", # local path
        "monica-path-to-climate-dir": "/Climate/1Km/", 
        "path-to-data-dir": "./data/", 
        "path-debug-write-folder": "./debug-out/",
    },
    "berg": {
        "include-file-base-path": "data/params/",
        "path-to-climate-dir": "/home/berg/GitHub/Ehsan_data_aggregation_paper/BW_ModelInputs_Scaling/Climate/1Km/CL_BW/", # local path
        "monica-path-to-climate-dir": "/home/berg/GitHub/Ehsan_data_aggregation_paper/BW_ModelInputs_Scaling/Climate/1Km/CL_BW/", 
        "path-to-data-dir": "data/", 
        "path-debug-write-folder": "debug-out/",
    },
    "remoteProducer-remoteMonica": {
        #"include-file-base-path": "/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/data/", # mounted path to archive or hard drive with climate data 
        "monica-path-to-climate-dir": "/monica_data/climate-data/", # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/", # mounted path to archive or hard drive with data 
        "path-debug-write-folder": "/out/debug-out/",
    }
}

DATA_SOIL_DB = "BW_ModelInputs_Scaling/Soil/250Meter/all_soil_250m_grid.sqlite"
DATA_GRID_HEIGHT = "germany/dem_1000_25832_etrs89-utm32n.asc" 
DATA_GRID_SLOPE = "germany/slope_1000_25832_etrs89-utm32n.asc"
TEMPLATE_PATH_LATLON = "{path_to_climate_dir}/latlon-to-rowcol.json"
TEMPLATE_PATH_CLIMATE_CSV = "daily_mean_RES1_C{ccol:03d}R{crow:03d}.txt"

TEMPLATE_PATH_HARVEST = "{path_to_data_dir}/projects/monica-germany/ILR_SEED_HARVEST_doys_{crop_id}.csv"

# commandline parameters e.g "server=localhost port=6666 shared_id=2"
def run_producer(server = {"server": None, "port": None}):
    "main"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH) # pylint: disable=no-member
    #config_and_no_data_socket = context.socket(zmq.PUSH)
    config = {
        "user": "berg",
        "server-port": server["port"] if server["port"] else "6666", ## local: 6667, remote 6666
        "server": server["server"] if server["server"] else "localhost",
        "path_to_dem_grid": "",
        "sim.json": "sim.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
        "setups-file": "sim_setups.csv",
        "run-setups": "[1]",
    }
    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v

    print("config:", config)

    # select paths 
    paths = PATHS[config["user"]]
    # open soil db connection
    soil_db_con = sqlite3.connect(DATA_SOIL_DB)
    # connect to monica proxy (if local, it will try to connect to a locally started monica)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # read setup from csv file
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    #transforms geospatial coordinates from one coordinate reference system to another
    # transform wgs84 into gk5
    soil_crs_to_x_transformers = {}
    wgs84_crs = CRS.from_epsg(4326)
    #utm32_crs = CRS.from_epsg(25832)
    soil_crs = CRS.from_epsg(3035)
    soil_crs_to_x_transformers[wgs84_crs] = Transformer.from_crs(soil_crs, wgs84_crs, always_xy=True)

    ilr_seed_harvest_data = defaultdict(lambda: {"interpolate": None, "data": defaultdict(dict), "is-winter-crop": None})

    # Load grids
    ## note numpy is able to load from a compressed file, ending with .gz or .bz2

    #admin_units = []
    soils = []
    soil_db_con.row_factory = sqlite3.Row
    #for row in soil_db_con.cursor().execute("select DISTINCT Administrative_units_ID as au from Wheat_Soil_AdminUnit"):
    for row in soil_db_con.cursor().execute("select DISTINCT Id_Soil_250m as id, Column as c, Row as r, X as x, Y as y from All_soil2_withClimate_250mgrid_2"):
        soil = {"id": int(row["id"]), "c": int(row["c"]), "r": int(row["r"]), "x": int(row["x"]), "y": int(row["y"])}
        soils.append(soil)

    # height data for germany
    path_to_dem_grid = paths["path-to-data-dir"] + DATA_GRID_HEIGHT 
    dem_epsg_code = int(path_to_dem_grid.split("/")[-1].split("_")[2])
    dem_crs = CRS.from_epsg(dem_epsg_code)
    if dem_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[dem_crs] = Transformer.from_crs(soil_crs, dem_crs, always_xy=True)
    dem_metadata, _ = Mrunlib.read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=6)
    dem_interpolate = Mrunlib.create_ascii_grid_interpolator(dem_grid, dem_metadata)
    print("read: ", path_to_dem_grid)

    # slope data
    path_to_slope_grid = paths["path-to-data-dir"] + DATA_GRID_SLOPE
    slope_epsg_code = int(path_to_slope_grid.split("/")[-1].split("_")[2])
    slope_crs = CRS.from_epsg(slope_epsg_code)
    if slope_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[slope_crs] = Transformer.from_crs(soil_crs, slope_crs, always_xy=True)
    slope_metadata, _ = Mrunlib.read_header(path_to_slope_grid)
    slope_grid = np.loadtxt(path_to_slope_grid, dtype=float, skiprows=6)
    slope_interpolate = Mrunlib.create_ascii_grid_interpolator(slope_grid, slope_metadata)
    print("read: ", path_to_slope_grid)

    sent_env_count = 1
    start_time = time.perf_counter()

    for _, setup_id in enumerate(run_setups):

        if setup_id not in setups:
            continue
        start_setup_time = time.perf_counter()

        setup = setups[setup_id]
        crop_id = setup["crop-id"]

        ## extract crop_id from crop-id name that has possible an extenstion
        crop_id_short = crop_id.split('_')[0]

        # add crop id from setup file
        try:
            #read seed/harvest dates for each crop_id
            path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_data_dir=paths["path-to-data-dir"],  crop_id=crop_id_short)
            print("created seed harvest gk5 interpolator and read data: ", path_harvest)
            Mrunlib.create_seed_harvest_geoGrid_interpolator_and_read_data(path_harvest, wgs84_crs, soil_crs, ilr_seed_harvest_data)
        except IOError:
            path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_data_dir=paths["path-to-data-dir"],  crop_id=crop_id_short)
            print("Couldn't read file:", path_harvest)
            continue

        # read template sim.json 
        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        # change start and end date acording to setup
        if setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup["end_date"]:
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"]) 
        sim_json["include-file-base-path"] = paths["include-file-base-path"]

        #sim_json["output"]["obj-outputs?"] = False

        # read template site.json 
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)

        site_json["EnvironmentParameters"]["rcp"] = "rcp85"

        # read template crop.json
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)

        crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = setup["use_vernalisation_fix"] if "use_vernalisation_fix" in setup else False

        # set the current crop used for this run id
        crop_json["cropRotation"][2] = crop_id

        # create environment template from json templates
        env_template = monica_io3.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        soil_id_cache = {}
        for soil in soils:
            print(soil)
            
            tcoords = {}

            soil_id = soil["id"]

            #get coordinate of soil grid cell center
            sh, sr = soil["y"], soil["x"]
            #inter = crow/ccol encoded into integer
            crow, ccol = soil["r"], soil["c"]

            if wgs84_crs not in tcoords:
                tcoords[wgs84_crs] = soil_crs_to_x_transformers[wgs84_crs].transform(sr, sh)
            slon, slat = tcoords[wgs84_crs]
            
            if soil_id in soil_id_cache:
                soil_profile = soil_id_cache[soil_id]
            else:
                soil_profile = soil_io3.soil_parameters(soil_db_con, soil_id)
                soil_id_cache[soil_id] = soil_profile

            worksteps = env_template["cropRotation"][0]["worksteps"]
            sowing_ws = next(filter(lambda ws: ws["type"][-6:] == "Sowing", worksteps))
            harvest_ws = next(filter(lambda ws: ws["type"][-7:] == "Harvest", worksteps))

            ilr_interpolate = ilr_seed_harvest_data[crop_id_short]["interpolate"]
            seed_harvest_cs = ilr_interpolate(sr, sh) if ilr_interpolate else None

            # set external seed/harvest dates
            if seed_harvest_cs:
                seed_harvest_data = ilr_seed_harvest_data[crop_id_short]["data"][seed_harvest_cs]
                if seed_harvest_data:
                    is_winter_crop = ilr_seed_harvest_data[crop_id_short]["is-winter-crop"]

                    if setup["sowing-date"] == "fixed":  # fixed indicates that regionally fixed sowing dates will be used
                        sowing_date = seed_harvest_data["sowing-date"]
                    elif setup["sowing-date"] == "auto":  # auto indicates that automatic sowng dates will be used that vary between regions
                        sowing_date = seed_harvest_data["latest-sowing-date"]
                    elif setup["sowing-date"] == "fixed1":  # fixed1 indicates that a fixed sowing date will be used that is the same for entire germany
                        sowing_date = sowing_ws["date"]

                    sds = [int(x) for x in sowing_date.split("-")]
                    sd = date(2001, sds[1], sds[2])
                    sdoy = sd.timetuple().tm_yday

                    if setup["harvest-date"] == "fixed":  # fixed indicates that regionally fixed harvest dates will be used
                        harvest_date = seed_harvest_data["harvest-date"]                         
                    elif setup["harvest-date"] == "auto":  # auto indicates that automatic harvest dates will be used that vary between regions
                        harvest_date = seed_harvest_data["latest-harvest-date"]
                    elif setup["harvest-date"] == "auto1":  # fixed1 indicates that a fixed harvest date will be used that is the same for entire germany
                        harvest_date = harvest_ws["latest-date"]

                    hds = [int(x) for x in harvest_date.split("-")]
                    hd = date(2001, hds[1], hds[2])
                    hdoy = hd.timetuple().tm_yday

                    esds = [int(x) for x in seed_harvest_data["earliest-sowing-date"].split("-")]
                    esd = date(2001, esds[1], esds[2])

                    # sowing after harvest should probably never occur in both fixed setup!
                    if setup["sowing-date"] == "fixed" and setup["harvest-date"] == "fixed":
                        #calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])
                    
                    elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto":
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto1":
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy - 1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], hds[1], hds[2])
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "fixed":
                        sowing_ws["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        calc_sowing_date = date(2000, 12, 31) + timedelta(days=max(hdoy+1, sdoy))
                        sowing_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(sds[0], calc_sowing_date.month, calc_sowing_date.day)
                        harvest_ws["date"] = seed_harvest_data["harvest-date"]
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["earliest-date"], "<",
                                sowing_ws["latest-date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])

                    elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "auto":
                        sowing_ws["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["latest-date"] = seed_harvest_data["latest-sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["earliest-date"], "<",
                                sowing_ws["latest-date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "fixed1" and setup["harvest-date"] == "fixed":
                        #calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = sowing_date
                        # print(seed_harvest_data["sowing-date"])
                        harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])


            if len(soil_profile) == 0:
                env_template["customId"] = {
                    "setup_id": setup_id,
                    "crow": int(crow), "ccol": int(ccol),
                    "soil_id": soil_id,
                    "env_id": sent_env_count,
                    "nodata": True
                }
                socket.send_json(env_template)
                # print("sent nodata env ", sent_env_count, " customId: ", env_template["customId"])
                sent_env_count += 1
                continue

            if dem_crs not in tcoords:
                tcoords[dem_crs] = soil_crs_to_x_transformers[dem_crs].transform(sr, sh)
            demr, demh = tcoords[dem_crs]
            height_nn = dem_interpolate(demr, demh)

            if slope_crs not in tcoords:
                tcoords[slope_crs] = soil_crs_to_x_transformers[slope_crs].transform(sr, sh)
            slr, slh = tcoords[slope_crs]
            slope = slope_interpolate(slr, slh)

            env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup["LeafExtensionModifier"]
                
            #print("soil:", soil_profile)
            env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

            if setup["elevation"]:
                env_template["params"]["siteParameters"]["heightNN"] = float(height_nn)

            if setup["slope"]:
                env_template["params"]["siteParameters"]["slope"] = slope / 100.0

            if setup["latitude"]:
                env_template["params"]["siteParameters"]["Latitude"] = slat

            if setup["FieldConditionModifier"]:
                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["species"]["FieldConditionModifier"] = float(setup["FieldConditionModifier"])

            if setup["StageTemperatureSum"]:
                stage_ts = setup["StageTemperatureSum"].split('_')
                stage_ts = [int(temp_sum) for temp_sum in stage_ts]
                orig_stage_ts = env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0]
                if len(stage_ts) != len(orig_stage_ts):
                    stage_ts = orig_stage_ts
                    print('The provided StageTemperatureSum array is not '
                            'sufficiently long. Falling back to original StageTemperatureSum')

                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0] = stage_ts

            env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup["fertilization"]
            env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup["irrigation"]

            env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]
            env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup["WaterDeficitResponseOn"]
            env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup["EmergenceMoistureControlOn"]
            env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup["EmergenceFloodingControlOn"]

            env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
            
            subpath_to_csv = TEMPLATE_PATH_CLIMATE_CSV.format(crow=int(crow), ccol=int(ccol))
            env_template["pathToClimateCSV"] = paths["monica-path-to-climate-dir"] + subpath_to_csv

            env_template["customId"] = {
                "setup_id": setup_id,
                "crow": int(crow), "ccol": int(ccol),
                "soil_id": soil_id,
                "env_id": sent_env_count,
                "nodata": False
            }

            socket.send_json(env_template)
            print("sent env ", sent_env_count, " customId: ", env_template["customId"])

            sent_env_count += 1

        stop_setup_time = time.perf_counter()
        print("Setup ", (sent_env_count-1), " envs took ", (stop_setup_time - start_setup_time), " seconds")

    stop_time = time.perf_counter()

    try:
        print("sending ", (sent_env_count-1), " envs took ", (stop_time - start_time), " seconds")
        print("exiting run_producer()")
    except Exception:
        raise

if __name__ == "__main__":
    run_producer()