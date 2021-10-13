import os
from os import listdir
from os.path import isfile, join
import pprint
from tqdm import tqdm
import time
import json
import threading

in_path = "output/"
search_in = "search/in/"
search_out = "search/out/"

global index_inverted
global ids_files

def read_index_inverted():
    i_inverted = {}
    onlyfiles = [f for f in listdir(in_path) if isfile(join(in_path, f))]
    for f in onlyfiles:
        file_data = ""
        with open (in_path+f,'r') as data:
            file_data = data.read()
        file_data = file_data.split('\n')
        for line in tqdm(file_data):
            data = line.split('\t')
            #print (data)
            if data != ['']:
                l_files = data[1].split(';')
                l_data = []
                for d_file in l_files:
                    if d_file != "":
                        l_data += [d_file.split(' ')]            
                i_inverted[data[0]] = list(reversed(l_data))
    return i_inverted

def read_search ():
    onlyfiles = [f for f in listdir(search_in) if isfile(join(search_in, f))]
    #print (onlyfiles)
    if onlyfiles == []:
        return None,None,None
    else:
        data_r = []
        with open (search_in+onlyfiles[0], 'r')as file_in:
            data_r = file_in.read().split()
        os.remove (search_in+onlyfiles[0])
        return onlyfiles[0],data_r[0], data_r[1:]

def page_rank (words):
    global index_inverted
    print("Execute Search:",words)
    d_search = {}
    for w in words:
        if w in index_inverted:
            rank = [[s,float(r)*(10**(-6))] for s,r in index_inverted[w]]
            #print (rank)
            for i,r in enumerate(rank):
                if r[0] in d_search:
                    d_search[r[0]]["score"] += (1+r[1])
                    d_recur = d_search[r[0]]["data"]
                    if w in d_recur:
                        d_search[r[0]]["data"][w] += index_inverted[w][i][1]
                    else:
                        d_search[r[0]]["data"][w] = index_inverted[w][i][1]
                else:                    
                    d_search[r[0]] = {"score":r[1], "data":{w:index_inverted[w][i][1]}}
    sort_orders = sorted(d_search.items(), key=lambda x: x[1]["score"], reverse=True)
    return sort_orders


def read_dict_ids ():
    with open('database/index_ids.json', 'r') as json_file:
        return json.load(json_file)

def print_text_html (id, title, author, content):
    return """<div class="col mb-4">
                        <div class="card border-dark">
                          <div class="card-header">
                                """+id+"""
                          </div>
                          <div class="card-body">
                                <h5 class="card-title">"""+title+"""</h5>
                                <p class="card-text">"""+content+"""</p>
                                <p class="card-text"><small class="text-muted">Autor: """+author+"""</small></p>
                          </div>
                        </div>
                </div>"""

def print_text_html_sec (id, title, author, content):
    return """<div class="col mb-4">
                        <div class="card text">
                          <div class="card-header">
                                """+id+"""
                          </div>
                          <div class="card-body">
                                <h5 class="card-title">"""+title+"""</h5>
                                <p class="card-text">"""+content+"""</p>
                                <p class="card-text"><small class="text-muted">Autor: """+author+"""</small></p>
                          </div>
                        </div>
                </div>"""

def print_error (string_d):
    return '<div class="alert alert-danger" role="alert">'+string_d+"</div>"

def print_alert (string_d):
    return '<div class="alert alert-primary" role="alert">'+string_d+"</div>"

def read_file (name_file, n):
    with open(name_file, 'r') as f:
        return f.read(n)

def write_file (name_file, data):
    with open(name_file, 'w') as f:
        f.write(data)


def ver_mas (file, l_words):
    return  str('<a href="vermas.php?file='+file+'&words='+
                str(l_words).replace(" ", "").replace("'", "")[1:-1]+
                '"> Ver Mas...</a>')

def print_html (l_ranks, num, words, max_n=200):
    global ids_files
    
    str_html = '<div class="row row-cols-1 row-cols-md-2">'
    f_file = l_ranks[0][0][:-4]
    score = l_ranks[0][1]["score"]
    data = str(l_ranks[0][1]["data"])
    title = ids_files[f_file]["name"]
    author = ids_files[f_file]["author"]
    content = read_file("database/out/"+f_file+".txt", max_n)
    str_html += print_text_html ("ID:"+f_file+
                                 ", Score:"+"{:.6f}".format(score)+
                                 ", "+data,
                                 title,
                                 author,
                                 content + ver_mas(f_file,words))

    for r_f in l_ranks[1:num]:
        f_file = r_f[0][:-4]
        score = r_f[1]["score"]
        data = str(r_f[1]["data"])
        title = ids_files[f_file]["name"]
        author = ids_files[f_file]["author"]
        content = read_file("database/out/"+f_file+".txt", max_n)
        str_html += print_text_html_sec ("ID:"+f_file+
                                         ", Score:"+"{:.6f}".format(score)+
                                         ", "+data,
                                         title,
                                         author,
                                         content + ver_mas(f_file,words))
            
    str_html += '</div>'
    return str_html

def update_database():
    global index_inverted
    global ids_files
    while(1):
        time.sleep (300)
        ids_files = read_dict_ids ()
        index_inverted = read_index_inverted()
    
def main():
    global index_inverted
    global ids_files

    ids_files = read_dict_ids ()
    index_inverted = read_index_inverted()

    t = threading.Thread(target=update_database)
    t.start()
    
    while(1):
        name,n,s = read_search()
        if (s != None):
            l_ranks = page_rank (s)
            str_html = print_alert("Resultados para: "+str(s))
            if (len(l_ranks)>0):
                str_html += print_html (l_ranks, int(n), s)
            else:
                str_html += print_error("No se encontro ningun resultado")
            write_file ("search/out/"+name,str_html)
        else:
            time.sleep(0.01)

if __name__=="__main__":
    main()

    
