import h5py
import geopandas as gp
import pandas as pd
import os
import time
from pathlib import Path
import pickle

def process(gedi_file, out_dir, aoi_path, qual):
    skip_list = ["/METADATA"]
    aoi_overlay = gp.read_file(aoi_path)
    df_list = []
    gedi_swath = h5py.File(gedi_file, 'r')
    polys = gp.read_file(aoi_path)
    
    for beam in gedi_swath.values():
        if beam.name in skip_list:
            continue
        print(beam.name)
        beam_num = beam['beam']
        lat = beam['geolocation']['latitude_bin0']
        lon = beam['geolocation']['longitude_bin0']
        rh100 = beam['rh100']
        qual = beam['l2b_quality_flag']
        shot = beam['geolocation']['shot_number']
        pai = beam['pai']
        modes = beam['num_detectedmodes']
        cover = beam['cover']

        product_dict = {
            'fname': os.path.basename(gedi_file),
            'beam_num': beam_num,
            'lat': lat,
            'lon': lon,
            'rh100': rh100,
            'pai': pai,
            'qual': qual,
            'shot': shot,
            'modes': modes,
            'cover': cover, 
        }
        
        df = pd.DataFrame(product_dict)
        df.dropna()
        
        if qual == True:
            df = df[df['qual'] ==1]
            
        df_list.append(df)


    concat_df = pd.concat(df_list)
    
    print("Coverting pandas to geopandas ...")
    gdf = gp.GeoDataFrame(concat_df, geometry=gp.points_from_xy(concat_df.lon, concat_df.lat))
    
    print("Setting common CRS ...")
    gdf.crs = polys.crs
    
    print("Processing spatial join ...")
    final_gdf = gp.tools.sjoin(gdf, polys, op='within')
    
    print("Writing dataframe to .pkl")    
    final_gdf.to_pickle(out_dir + "/" + Path(gedi_file).stem + '.pkl', compression='infer', protocol=5)


def gedi_aoi_process(data_dir, out_dir, poly, qual):
    start_time = time.time()
    l = os.listdir(out_dir)
    existing_pkls = [x.split('.')[0] for x in l]
    counter = 0
    gedi_file_list = []
    
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            gedi_file_list.append(os.path.join(root,file))
    gedi_file_list.sort()
    
    for gedi_file in gedi_file_list:
        gedi_base = Path(gedi_file).stem
        if gedi_base in existing_pkls:
            print(gedi_base + " is already pickled, skipping.")
            continue
        else:
            counter = counter + 1
            print("Processing {}".format(gedi_file))
            print("File {} of {}".format(counter, len(gedi_file_list)))
            process(gedi_file, out_dir, poly, qual)

    pkl_file_list = [os.path.join(out_dir, file_name)
                      for file_name
                      in os.listdir(out_dir)
                      if file_name.endswith('.pkl')]
    

    pkl_list = []
    pkl_counter = 0
        
    print("Loading .pkls into dataframes")
    for filename in pkl_file_list:
        pkl_counter = pkl_counter + 1
        print("Loading {}".format(filename))
        print("File {} of {}".format(pkl_counter, len(pkl_file_list)))

        df = pd.read_pickle(filename)
        pkl_list.append(df)
        del(df)

    print("Concatinating dataframes")
    out_df = pd.concat(pkl_list)
    del out_df['index_right']
    pkl_list.clear()
    
    print("Writing .shp")
    out_df.to_file(out_dir + "/out.shp")
    
    print("Writing .csv")
    out_df.to_csv(out_dir + "out.csv")
    
    end_time = time.time()
    hours, rem = divmod(end_time - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Elapsed {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))