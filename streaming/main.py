import time
from datetime import datetime, timedelta
import configparser
import pandas as pd
import json
import numpy as np
from elasticsearch import Elasticsearch
from sklearn.preprocessing import StandardScaler
import torch
from DCRNN import *


INITIAL_TIMESTAMP = '2019-02-01 00:00:00'
INITIAL_TIMESTEP = 12  # The initial timestamp is 01:00:00
TIME_INTERVAL = 5  # 5 minutes time interval
TIME_SCALE = 60  # Scale the time with 1min -> 1sec, 1hour -> 1min

current_timestep = INITIAL_TIMESTEP
manhattan_zones = pd.read_csv("../data-NYCZones/zones/manhattan_zones.csv")
flow_array = None

config = configparser.ConfigParser()
config.read('capstone.ini')
es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)


def read_from_es(index_name):
    global current_timestep

    search_query = None
    if current_timestep == INITIAL_TIMESTEP:
        # Load the preloaded data
        search_query = {
            "query": {'match_all': {}},
            'from': 0,
            'size': 1000
        }
    else:
        # Load the data in the current_timestep
        initial_datetime = datetime.strptime(
            INITIAL_TIMESTAMP, '%Y-%m-%d %H:%M:%S')
        current_timestamp = (initial_datetime + timedelta(
            minutes=TIME_INTERVAL * current_timestep)).strftime('%Y-%m-%d %H:%M:%S')
        search_query = {
            "query": {'match': {"interval_start": current_timestamp}},
        }

    # Execute the search query and get the results
    resp = es.search(index=index_name, body=search_query)
    data = [r['_source'] for r in resp['hits']['hits']]
    return data


def write_to_es(index_name, data):
    # First delete all documents in the specified index
    delete_query = {"query": {"match_all": {}}}
    es.delete_by_query(index=index_name, body=delete_query)

    # Convert the DataFrame to a JSON format
    json_data = data.to_json(orient='records')

    # Index the documents into Elasticsearch
    for i, doc in enumerate(json.loads(json_data)):
        es.index(index='test', document=doc)


# Convert the flow data into the numpy array for model inference
def flow_to_ndarray(flow):
    if not flow:
        return np.zeros([1, 69])

    df = pd.DataFrame(flow)

    # Join the two tables on the zone_id column, calculate timestep
    joined_flow_table = pd.merge(
        df, manhattan_zones, left_on='DOLocationID', right_on='zone_id', how='left')
    joined_flow_table = joined_flow_table.drop(
        ['DOLocationID', 'zone_id', 'zone_name'], axis=1)
    joined_flow_table['interval_start'] = pd.to_datetime(
        joined_flow_table['interval_start'])
    joined_flow_table['timestep'] = ((joined_flow_table['interval_start'] -
                                     joined_flow_table['interval_start'][0]).dt.total_seconds() / 60 // TIME_INTERVAL)

    global current_timestep
    rows = 12 if current_timestep == INITIAL_TIMESTEP else 1
    inflows_array = np.zeros([rows, 69])
    for _, row in joined_flow_table.iterrows():
        i = int(row['timestep'])
        j = int(row['graph_id'])
        inflows_array[i][j] = row['inflow']

    return inflows_array


def load_all(index_name):
    global flow_array
    preloaded_data = read_from_es(index_name)
    flow_array = flow_to_ndarray(preloaded_data)


def load_step(index_name):
    global flow_array
    next_step_flow = read_from_es(index_name)
    new_row = flow_to_ndarray(next_step_flow)
    flow_array = np.delete(flow_array, 0, axis=0)
    flow_array = np.append(flow_array, new_row, axis=0)


# Read previous 12 timestemps from ES
def load_es_flow(current_timestep):
    if current_timestep == INITIAL_TIMESTEP:
        load_all("taxi_inflow_es")
    else:
        load_step("taxi_inflow_es")


# flow_array: nd_array with shape (T, N) = (12, 69)
# Return prediction array for the next timestep
def run_model(flow_array):
    torch.manual_seed(100)
    np.random.seed(100)
    device = torch.device("cpu")

    # The data flow of 2019-01 are scaled
    scaler = StandardScaler()
    scaled_flow = scaler.fit_transform(flow_array)

    def get_model():
        adj_mx = load_adj(config['MODEL']['ADJPATH'],
                          config['MODEL']['ADJTYPE'])
        model = DCRNN(device, num_nodes=69,
                      input_dim=1, output_dim=1,
                      out_horizon=3, P=adj_mx).to(device)
        return model

    def predict(input):
        model = get_model()
        model.load_state_dict(torch.load(
            config['MODEL']['MODELNAME'] + '.pt', map_location=torch.device('cpu')))
        XS = torch.Tensor(input)
        with torch.no_grad():
            Y_pred = model(XS)
        Y_pred = Y_pred.cpu().numpy()
        Y_pred = scaler.inverse_transform(np.squeeze(Y_pred))

        prediction = Y_pred[0].astype(int)
        return prediction

    input = scaled_flow[np.newaxis, :, :, np.newaxis]
    result = predict(input)
    return result


# Detect the abnormal regions and write the data into ES
def detect_anomality(ground_truth, prediction):
    diff = [abs(i - j) for i, j in zip(ground_truth, prediction)]
    abnormal_graph_ids = [index for index, value in enumerate(diff) if value > 10]
    abnormal_zones = manhattan_zones[manhattan_zones['graph_id'].isin(abnormal_graph_ids)]
    
    write_to_es("taxi_inflow", abnormal_zones)
    raise KeyError


# Preload the initial 12 timesteps data
load_es_flow(current_timestep)

while True:
    # print(f"Timestep: {current_timestep}")
    # print(flow_array)

    # Predict the flow for the next timestep
    prediction = run_model(flow_array)

    # Wait for a time interval
    current_timestep += 1
    time.sleep(TIME_INTERVAL * 60 / TIME_SCALE)

    # Update the flow_array
    # Deleting the first row (first timestep)
    # Append a new row to the last
    load_es_flow(current_timestep)
    ground_truth = flow_array[-1]

    # Compare the prediction with the real flow
    detect_anomality(ground_truth, prediction)
