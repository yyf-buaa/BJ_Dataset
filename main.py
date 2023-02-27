import json
from datetime import datetime
from tqdm import  tqdm
import pandas as pd

def geometry2json(geometry):
    json = geometry.replace('POLYGON ','')
    json = json.replace('((','[[*[')
    json = json.replace('))',']*]]')
    json = json.replace(', ','],*[')
    json = json.replace(' ',',')
    json = json.replace('*',' ')
    return json

geo_file = pd.DataFrame(columns=['geo_id','type','coordinates','function','traffic_type'])
#.geo
#region
region_bj = pd.read_csv('Data/region_bj.csv',delimiter=',')
region_num = len(region_bj)
index = 0
for i in tqdm(range(len(region_bj)),desc="geo_region"):
    type = 'Polygon'
    coordinates = geometry2json(region_bj.loc[i,'geometry'])
    function = region_bj.loc[i,'FUNCTION']
    traffic_type = 'region'
    geo_file.loc[index] = [index,type,coordinates,function,traffic_type]
    index += 1
#road
road_bj = pd.read_csv('Data/road_bj.csv',delimiter=',')
road_num = len(road_bj)
for i in tqdm(range(len(road_bj)),desc="geo_road"):
    type = 'LineString'
    coordinates = road_bj.loc[i,'coordinates']
    function = -1
    traffic_type = 'road'
    geo_file.loc[index] = [index,type,coordinates,function,traffic_type]
    index += 1
#poi
poi_bj = pd.read_csv('Data/POI_bj.csv',delimiter=',')
poi_num = len(poi_bj)
for i in tqdm(range(len(poi_bj)),desc="geo_poi"):
    type = 'Point'
    x = poi_bj.loc[i,'X']
    y = poi_bj.loc[i,'Y']
    coordinates = '['+str(x)+','+str(y)+']'
    function = poi_bj.loc[i,'P']
    traffic_type = 'poi'
    geo_file.loc[index] = [index,type,coordinates,function,traffic_type]
    index += 1
#save .geo
geo_file.to_csv('result/bj_dataset.geo',index = False)

#usr---没有额外的信息不需要单独一张表了
#Rel
rel_file = pd.DataFrame(columns=['rel_id','type','origin_id','destination_id','rel_type'])
index = 0
road2region = pd.read_json('Data/road2region_bj.json', typ='series')
for i in tqdm(road2region.index.asi8,desc="rel_road2region"):
    type = 'geo'
    origin_id = i+region_num
    destination_id = road2region[i]
    rel_type = 'road2region'
    rel_file.loc[index] = [index,type,origin_id,destination_id,rel_type]
    index+=1

region2road = pd.read_json('Data/region2road_bj.json', typ='series')
for i in tqdm(region2road.index.asi8,desc="rel_region2road"):
    type = 'geo'
    origin_id = i
    destination_id_list = [des+region_num for des in list(region2road[i])]
    rel_type = 'region2road'
    for destination_id in destination_id_list:
        rel_file.loc[index] = [index,type,origin_id,destination_id,rel_type]
        index+=1

poi2region = pd.read_json('Data/poi2region_bj.json', typ='series')
for i in tqdm(poi2region.index.asi8,desc="rel_poi2region"):
    type = 'geo'
    origin_id = i+region_num+road_num
    destination_id = poi2region[i]
    rel_type = 'poi2region'
    rel_file.loc[index] = [index,type,origin_id,destination_id,rel_type]
    index+=1

region2poi = pd.read_json('Data/region2poi_bj.json', typ='series')
for i in tqdm(region2poi.index.asi8,desc="rel_region2poi"):
    type = 'geo'
    origin_id = i
    destination_id_list = [des+region_num+road_num for des in list(region2poi[i])]
    rel_type = 'region2poi'
    for destination_id in destination_id_list:
        rel_file.loc[index] = [index,type,origin_id,destination_id,rel_type]
        index+=1

poi2road = pd.read_json('Data/poi2road_bj.json', typ='series')
for i in tqdm(poi2road.index.asi8,desc="rel_poi2road"):
    type = 'geo'
    origin_id = i+region_num+road_num
    destination_id_list = [des+region_num for des in list(poi2road[i])]
    rel_type = 'poi2road'
    for destination_id in destination_id_list:
        rel_file.loc[index] = [index,type,origin_id,destination_id,rel_type]
        index+=1

road2poi = pd.read_json('Data/road2poi_bj.json', typ='series')
for i in tqdm(road2poi.index.asi8,desc="rel_road2poi"):
    type = 'geo'
    origin_id = i+region_num
    destination_id_list = [des+region_num+road_num for des in list(road2poi[i])]
    rel_type = 'road2poi'
    for destination_id in destination_id_list:
        rel_file.loc[index] = [index,type,origin_id,destination_id,rel_type]
        index+=1
rel_file.to_csv('result/bj_dataset.rel',index = False)

#dyna
dyna_file = pd.DataFrame(columns=['dyna_id','type','time','entity_id','traj_id','geo_id'])
traj = pd.read_csv('Data/traj_bj.csv',delimiter=';')
index = 0
for i in tqdm(range(len(traj)),desc="dyna"):
    path = traj.loc[i,'path']
    path = path[1:len(path)-1].split(',')
    path = [int(s) for s in path]
    t_list = traj.loc[i,'tlist']
    t_list = t_list[1:len(t_list)-1].split(',')
    t_list = [int(s) for s in t_list]
    t_list = [datetime.datetime.utcfromtimestamp(tim) for tim in t_list]
    type = 'trajectory'
    entity_id = traj.loc[i,'usr_id']
    traj_id = traj.loc[i,'traj_id']
    for j in range(len(path)):
        time = t_list[j]
        geo_id = path[j]
        dyna_file.loc[index] = [index,type,time,entity_id,traj_id,geo_id]
        index+=1
dyna_file.to_csv('result/bj_dataset.dyna',index=False)

#config
geo_obj = {"including_types":["Point", "LineString", "Polygon"],"Point":{"function":"other","traffic_type":"enum"},"LineString":{"function":"other","traffic_type":"enum"},"Polygon":{"function":"num","traffic_type":"enum"}}
rel_obj = {"including_types":["geo"],"geo":{"rel_type":"enum"}}
dyna_obj = {"including_types":["trajectory"],"trajectory":{"geo_id":"geo_id"}}
info_obj = {"data_files":"bj_dataset","geo_file":"bj_dataset","rel_file":"bj_dataset","calculate_weight_adj":True}
config_obj = {"geo":geo_obj,"rel":rel_obj,"dyna":dyna_obj,"info":info_obj}
with open('result/config.json','w') as f:
    json.dump(config_obj,f,indent = 2)





