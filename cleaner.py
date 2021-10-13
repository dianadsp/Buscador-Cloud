import os
from os import listdir
from os.path import isfile, join
import pprint
import json
import shutil
from tqdm import tqdm

in_path = "input/"
out_path = "out/"

def read_last_id():
    with open ("last_id.txt","r") as f:
        return int(f.read())

def save_last_id(num):
    with open ("last_id.txt","w") as f:
        f.write(str(num))      

def save_dict_ids (d_books):
    with open('index_ids.json', 'r') as json_file:
        data = json.load(json_file)
    data.update(d_books)
    with open('index_ids.json', 'w') as fp:
        json.dump(data, fp)
    
def preproprocessing ():
    onlyfiles = [f for f in listdir(in_path) if isfile(join(in_path, f))]
    d_authors = {}
    ids = read_last_id()
    for f in tqdm(onlyfiles):
        l_file = f.split("___");
        author = l_file[0]
        name = l_file[1][:-4]
        d_authors [ids] = {"author":author,"name":name}
        shutil.copyfile(in_path+f, out_path+str(ids)+'.txt')
        os.remove(in_path+f)
        ids += 1
    save_last_id (ids)
    save_dict_ids(d_authors)

def main():
    preproprocessing()

if __name__=="__main__":
    main()











