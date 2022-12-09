# -*- coding: iso-8859-15 -*-
'''
Created on 28.03.2014

@author: andreas
'''
import h5py
import os
import time
import glob
import pandas as pd


import sys
import numpy as np
import gzip
import shutil
import pickle



def read_hdf5(BASEDIR):


    #find list with hdf5 files
    print("Find files. This might take a while")
    st0 = time.time()
    filename_list = glob.glob("%s/001_buildings.hdf5" %(BASEDIR)) 
    filename_list.extend(glob.glob("%s/**/_scen_*/001_buildings.hdf5" %(BASEDIR), recursive = True)) 
    
    results_file = "%s/ZEN_RESULTS" % BASEDIR
    print("%i files found in %4.2f sec"%(len(filename_list), time.time() - st0))

    if 0==1:
        last_fn = sorted(glob.glob("%s/ZEN_RESULTS_*.pk"% BASEDIR))[-1]
    else:
        # Run through file list
        last_fn = None
        FINAL_RESULTS_DICT = {}
        for filename in filename_list:
            #read hdf5 file
            print (filename)
            hdf5_f = h5py.File(filename,'r')

            # get data elements stored in hdf5 container
            item_names_hdf5 = hdf5_f.items()
            print("items stored in data container:")
            print(list(item_names_hdf5))
            
            # Extract simulation periods stored in data container
            year_list_hdf5 = []
            for curr_item_name in item_names_hdf5: 
                if curr_item_name[0][:3] == "BC_":
                    year_list_hdf5.append(int(curr_item_name[0][-4:]))
                
            print("Years stored in data container: %s" % str(year_list_hdf5))
            
            
            bca = hdf5_f["BuildingCategories"][()]             # building class file as Numpy recarray
            sector_idx_bca = bca["sector_idx"]
            is_res_bca = sector_idx_bca == 1
            
            for yr in year_list_hdf5: 
                print("Current yr: %i"%yr)
                st = time.time()
        
                key_bc = "BC_%i"%yr   # Building class file
                key_bssh = "BSSH_%i"%yr # building segment (space heating) file
                try:
                    bc = hdf5_f[key_bc][()]             # building class file as Numpy recarray
                    bssh = hdf5_f[key_bssh][()]         # building segment file as Numpy recarray
                except:
                    pass
                    continue
         
            
                bc_index = bc["index"]     #building class index of elements in bc dataset
                
                bc_index_bssh = bssh["building_classes_index"]            #building class index of elements in bssh dataset

                bca_index_bc = bc["building_categories_index"]          # vector that contains the building category index of each building class
                is_res_bc = is_res_bca[bca_index_bc - 1]                # vector that is true if building class is residential
                
                is_res_bssh = is_res_bc[bc_index_bssh - 1]      # vector that is true if building segment is residential
                
                area_bssh = bc["grossfloor_area"][bc_index_bssh - 1] * bssh["number_of_buildings"]
                #cooling_needs_bssh = bc["ued_cool"][bc_index_bssh - 1] * bssh["number_of_buildings"]
                heating_needs_bssh = bc["hwb_norm"][bc_index_bssh - 1] * area_bssh
                
                effective_indoor_temp_jan_bc = bc["effective_indoor_temp_jan"]
                effective_indoor_temp_jan_bssh = effective_indoor_temp_jan_bc[bc_index_bssh - 1]  #indoor temperatur in january of each building segment
                               

                area_res_build_bssh = np.copy(area_bssh)
                area_res_build_bssh[is_res_bssh == False] = 0       #set value of non-res buildings to Zero
                
                average_indoor_temp_jan = np.sum(effective_indoor_temp_jan_bssh * area_res_build_bssh) / np.sum(area_res_build_bssh)
                # Get the heated floor area share which indoor temp. is below a certain t_indoor_thresholds
                
                t_indoor_thresholds = [17.5,18,18.5,19,19.5,20,20.5,21,21.5,22]     #List of temperature thresholds
                RESULTS = np.zeros(len(t_indoor_thresholds), dtype="f4")
                
                tot_sum_res_build_area = np.sum(area_res_build_bssh)
                for i, t_th in enumerate(t_indoor_thresholds):
                    is_below_temp_th = effective_indoor_temp_jan_bssh < t_th
                    share_of_heated_floor_area_below_temp_th = np.sum(area_res_build_bssh[is_below_temp_th]) / tot_sum_res_build_area
                    RESULTS[i] = share_of_heated_floor_area_below_temp_th
                
                          
                FINAL_RESULTS_DICT[f"{filename}_{yr}"] =   RESULTS

                ##############################################
                print("Done in %4.2f sec"%(time.time() - st))
                #end of year loop
            last_fn = f"{results_file}_{int(time.time()/1000)}.pk"
            with open(last_fn, 'wb') as handle:
                RESULTS_MATRIX = pickle.dump(FINAL_RESULTS_DICT, handle, protocol=pickle.HIGHEST_PROTOCOL)
            
            
            
            
            hdf5_f.close()
            #end of hdf5 file loop

        if last_fn == None:
            return
    
    
    with open(last_fn, 'rb') as handle:
        FINAL_RESULTS_DICT = pickle.load(handle)
        
    print(t_indoor_thresholds)
    for k in FINAL_RESULTS_DICT.keys():
        print(k)
        print((FINAL_RESULTS_DICT[k] * 100).astype("int"))
    res_fn =  f"{results_file}.csv"
    
    
    #THis is old code
    print(res_fn)
    with open(res_fn, 'w') as f:
        f.write("scenario,year,SFH(0)/MFH(1),Bundesland (BU=1/KA=2/NO=3/OO=4/SA=5/ST=6/TI=7/VO=8/WI=9),ZEN_BUILDING_GROUP(0=allBuildings/1=very_old-unrefb/2=1945-1970_unrefurb/3=1970-2005_or-refurb/4=new),area[Mio. m2],cooling_needs [GWh],heating_needs [GWh]" 
                    +",specific cooling_needs [kWh/m2],specific heating_needs [kWh/m2]" 
                    +",immissionFlaeche_sommer_nachweis_B8110_3,immissionsflaechenbezogenerLuftwechsel_sommer_nachweis_B8110_3"
                    +",erforderliche_speicherwirksameMasse_sommer_nachweis_B8110_3,immissionsflaechenbezogene_speicherwirksameMasse_sommer_nachweis_B8110_3,heatstoragemass_ratio_sommer_nachweis_B8110_3,"
                    +"\n")
        for scen_yr in FINAL_RESULTS_DICT.keys():
            RESULTS_MATRIX = FINAL_RESULTS_DICT[scen_yr]
            (bcat,ZEB_B_GROUP, res, BL) = RESULTS_MATRIX.shape
            for BL in range(9):
                for b in range(bcat):
                    for z in range(ZEB_B_GROUP):
                        f.write(f"{os.path.basename(os.path.dirname(scen_yr[:-5]))},{scen_yr[-4:]},{b},{BL+1}, {z},")
                        for r in range(3):
                            f.write(f"{RESULTS_MATRIX[b,z,r, BL]/10**6},")
                        
                        f.write(f"{RESULTS_MATRIX[b,z,1, BL]/np.maximum(0.00001, RESULTS_MATRIX[b,z,0, BL])},{RESULTS_MATRIX[b,z,2, BL]/np.maximum(0.00001, RESULTS_MATRIX[b,z,0, BL])},")
                        f.write(f"{RESULTS_MATRIX[b,z,3, BL]/np.maximum(0.00001, RESULTS_MATRIX[b,z,0, BL])},{RESULTS_MATRIX[b,z,3, BL]/np.maximum(0.00001, RESULTS_MATRIX[b,z,0, BL])},")
                        f.write(f"{RESULTS_MATRIX[b,z,5, BL]/np.maximum(0.00001, RESULTS_MATRIX[b,z,0, BL])},{RESULTS_MATRIX[b,z,6, BL]/np.maximum(0.00001, RESULTS_MATRIX[b,z,0, BL])},")
                        f.write(f"{RESULTS_MATRIX[b,z,7, BL]/np.maximum(0.00001, RESULTS_MATRIX[b,z,0, BL])},")
                            
                        f.write(f"\n") 
                
        
        


    print(time.time()-st0)
    print("  -----  THE END  --------")
    return


    
if __name__ == "__main__":

    
    basedir = r"/home/users/amueller/projects/2020_ZEN/invert/output_2021-11-18"
    basedir = r"C:\Users\andre\OneDrive\Desktop"
    print ("\n\n#####################")
    print(basedir)
    print (os.name)
    if os.name=="nt":
        print ("\n#####################" * 5)
        print("\n\n\nThis module is going to open large data sets. " +
              "It is highly recommended to run the code on the computer where the data are stored\n\n\n"
              )
        print ("\n#####################" * 5)
    read_hdf5(basedir)
    print("Done")