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


INITIAL_TIMESTAMP = '2019-02-01 00:00:00'  # The initial timestamp of the system
INITIAL_TIMESTEP = 12  # The initial time step is 01:00:00, marked as 12
TIME_INTERVAL = 5  # 5 minutes time interval
TIME_SCALE = 60  # Scale the time with 1min -> 1sec, 1hour -> 1min
FLOW_TYPES = ['taxi_inflow', 'taxi_outflow']
DETECT_THRESHOLD = 10

current_timestep = INITIAL_TIMESTEP
manhattan_zones = pd.read_csv("../data-NYCZones/zones/manhattan_zones.csv")
flow_array = None  # Flow with shape (T, N, C) = (12, 69, 4)

config = configparser.ConfigParser()
config.read('config.ini')
es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)


# Read the data from a certain index
# Full load all the data if the timestep is initial step
# Else only load the data incrementaly for the current_timestep
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


# Write a pandas dataframe to ES
# Used for writing the abnormal regions' information
def write_to_es(index_name, data):
    # First delete all documents in the specified index
    delete_query = {"query": {"match_all": {}}}
    try:
        es.delete_by_query(index=index_name, body=delete_query)
    except:
        Exception

    # Convert the DataFrame to a JSON format
    json_data = data.to_json(orient='records')

    # Index the documents into Elasticsearch
    for i, doc in enumerate(json.loads(json_data)):
        es.index(index=index_name, id=i, body=doc)


# Convert the flow data into the numpy array for model inference
def flow_to_ndarray(flow_name, input):
    if not input:
        return np.zeros([1, 69])

    df = pd.DataFrame(input)

    # Join the two tables on the zone_id column, calculate timestep
    joined_flow_table = pd.merge(df, manhattan_zones, left_on='region_id', right_on='zone_id', how='left')
    joined_flow_table = joined_flow_table.drop(['region_id', 'zone_id', 'zone_name'], axis=1)
    joined_flow_table['interval_start'] = pd.to_datetime(joined_flow_table['interval_start'])
    joined_flow_table['timestep'] = ((joined_flow_table['interval_start'] -
                                     joined_flow_table['interval_start'][0]).dt.total_seconds() / 60 // TIME_INTERVAL)

    global current_timestep
    rows = 12 if current_timestep == INITIAL_TIMESTEP else 1
    flows = np.zeros([rows, 69])
    for _, row in joined_flow_table.iterrows():
        i = int(row['timestep'])
        j = int(row['graph_id'])
        if i == 12:
            break
        flows[i][j] = row[flow_name.split('_')[1]]

    return flows


def load_all():
    global flow_array
    channel = len(FLOW_TYPES)
    flow_array = np.zeros([12, 69, channel])

    for i in range(channel):
        preloaded_data = read_from_es(FLOW_TYPES[i] + '_es')
        flow_array[:, :, i] = flow_to_ndarray(FLOW_TYPES[i], preloaded_data)


def load_step():
    global flow_array
    channel = len(FLOW_TYPES)

    for i in range(channel):
        next_step_flow = read_from_es(FLOW_TYPES[i] + '_es')
        new_row = flow_to_ndarray(FLOW_TYPES[i], next_step_flow)
        flow_array[1:, :, i] = flow_array[:-1, :, i]
        flow_array[-1, :, i] = new_row


# Read previous 12 timestemps from ES
def load_flow(current_timestep):
    if current_timestep == INITIAL_TIMESTEP:
        load_all()
    else:
        load_step()


# flow_array: nd_array with shape (T, N) = (12, 69)
# Return prediction array for the next timestep
def run_model(flow_array):
    torch.manual_seed(100)
    np.random.seed(100)
    device = torch.device("cpu")

    # Get the corresponding model for the given flow
    def get_model(flow_name):
        adj_mx = load_adj(config['MODEL']['ADJPATH'],
                          config['MODEL']['ADJTYPE'])
        model = DCRNN(device, num_nodes=69,
                      input_dim=1, output_dim=1,
                      out_horizon=3, P=adj_mx).to(device)
        model.load_state_dict(torch.load(
            './pretrained/' + config['MODEL']['MODELNAME'] + '_' + flow_name + '.pt', map_location=torch.device('cpu')))
        return model

    # Predict using the model and the input with shape (1, T, N, 1)
    def predict(flow_name):
        model = get_model(flow_name)
        flow = flow_array[:, :, FLOW_TYPES.index(flow_name)]

        scaler = StandardScaler()
        scaled_input = scaler.fit_transform(flow)
        scaled_input = scaled_input[np.newaxis, :, :, np.newaxis]

        XS = torch.Tensor(scaled_input)
        with torch.no_grad():
            Y_pred = model(XS)
        Y_pred = Y_pred.cpu().numpy()

        Y_pred = scaler.inverse_transform(np.squeeze(Y_pred))
        prediction = Y_pred[0].astype(int)
        return prediction

    results = [predict(flow_name) for flow_name in FLOW_TYPES]
    stacked_result = np.stack(results, axis=-1)
    return stacked_result


# Detect the abnormal regions and write the data into ES
def detect_anomality(ground_truth, prediction):
    anomalies = pd.DataFrame({'zone_id': [], 'graph_id': [], 'zone_name': [], 'flow_type': []})

    channel = len(FLOW_TYPES)
    for k in range(channel):
        diff = [abs(i - j) for i, j in zip(ground_truth[:, k], prediction[:, k])]
        abnormal_graph_ids = [index for index, value in enumerate(diff) if value > DETECT_THRESHOLD]
        abnormal_zones = manhattan_zones[manhattan_zones['graph_id'].isin(abnormal_graph_ids)]
        abnormal_zones['flow_type'] = FLOW_TYPES[k]
        anomalies = pd.concat([anomalies, abnormal_zones], ignore_index=True)

    write_to_es("anomalies", anomalies)


# Preload the initial 12 timesteps data
load_flow(current_timestep)

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
    load_flow(current_timestep)
    ground_truth = flow_array[-1]

    # Compare the prediction with the real flow
    detect_anomality(ground_truth, prediction)
