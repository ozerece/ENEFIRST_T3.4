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


def read_hdf5(BASEDIR):


    #find list with hdf5 files
    print("Find files. This might take a while")
    st = time.time()
    filename_list = glob.glob("%s/001_buildings.hdf5" %(BASEDIR)) 
    filename_list.extend(glob.glob("%s/**/_scen_/001_buildings.hdf5" %(BASEDIR), recursive = True)) 
    
    print("%i files found in %4.2f sec"%(len(filename_list), time.time() - st))


    # Run through file list
    for filename in filename_list:
        #read hdf5 file
        print (filename)
        hdf5_f = h5py.File(filename,'r')

        # get data elements stored in hdf5 container
        item_names_hdf5 = hdf5_f.items()
        print("items stored in data container:")
        print(item_names_hdf5)
        
        # Extract simulation periods stored in data container
        year_list_hdf5 = []
        for curr_item_name in item_names_hdf5: 
            if curr_item_name[0][:3] == "BC_":
                year_list_hdf5.append(int(curr_item_name[0][-4:]))
            
        print("Years stored in data container: %s" % str(year_list_hdf5))
        


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

            #if working with pandas dataframe is preferred:
            if 1 == 1:
                # convert recarray to pandas dataframe
                df_bc = pd.DataFrame(bc) # This is pandas dataframe containing the building class data
                del bc #remove bc to save RAM
                df_bssh = pd.DataFrame(bssh) # This is pandas dataframe containing the building segment data
                del bssh
                
                print(df_bc)
                
                
            #########################################
            #
            #
            # ADD HERE YOUR CODE
            #
            #
            #########################################
            
            
            
            
            
                
                
                
            ##############################################
            print("Done in %4.2f sec"%(time.time() - st))
            #end of year loop
        hdf5_f.close()
        #end of hdf5 file loop

            

    


    print(time.time()-st0)
    print("  -----  THE END  --------")
    return


    
if __name__ == "__main__":

    
    basedir = r"Z:\projects\2021_ENEFIRST\invert\output_20210622\MLT"
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