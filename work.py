import pandas as pd
import os
import os.path
import time
import gc

def main():
    # start timer on whole script
    script_start = time.time()

    input_folder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/decompressed/'
    output_folder = '/Users/patrickburke/Library/CloudStorage/OneDrive-EmoryUniversity/ECON496RW/cleaned/'

    counter = 0

    for file_name in os.listdir(input_folder):

        # check if file has already been cleaed
        completed = os.listdir(output_folder)
        if file_name.lower().replace(".txt",".csv") in completed:
            counter += 1
            continue

        # if size < 8gb skip for now
        raw_size = os.path.getsize(input_folder + file_name)
        if raw_size > 5000000000:
            continue
        converted_size = convert_bytes(raw_size)

        # ignore ds store
        if file_name == ".DS_Store":
            counter += 1
            continue

        # load the file
        load_start = time.time()
        print("Initialzing reading {} into pandas dataframe.".format(file_name))
        print("Size of {} = {}".format(file_name, converted_size))

        df = pd.read_csv(input_folder + file_name, sep="|")
        load_finish = time.time()
        print("{} successfully read into pandas dataframe.".format(file_name))
        print("Time to load = {} seconds".format(int(load_finish - load_start)))
        print("{} has {} rows and {} columns".format(file_name, len(df.index),len(df.columns)))

        print("Initializing data cleaning on {}".format(file_name))
        clean_start = time.time()
        df.columns = df.columns.str.strip("~'")
        df = df.apply(lambda x: x.str.strip("~ ") if x.dtype == "object" else x)
        new_file_name = file_name.replace(".txt",".csv").lower()

        # export
        df.to_csv(output_folder + new_file_name, index=False)

        # free up memory
        del df
        gc.collect()
        df = pd.DataFrame()

        clean_end = time.time()
        print(new_file_name + " has been successfully cleaned and exported")
        print("Time to clean = {} seconds".format(int(clean_end - clean_start)))
        export_size = convert_bytes(os.path.getsize(output_folder + new_file_name))
        print("Size of file after cleaning = {}".format(export_size))
        counter += 1
        print("Progress = {}/{}".format(counter,len(os.listdir(input_folder))))

    
    script_end = time.time()
    print("All data successfully cleaned")
    print("Time for entire script to run = {} seconds".format(script_end - script_start))


# calculate file size in KB, MB, GB
def convert_bytes(size):
    """ Convert bytes to KB, or MB or GB"""
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return "%3.1f %s" % (size, x)
        size /= 1024.0

if __name__ == '__main__':
	main()

