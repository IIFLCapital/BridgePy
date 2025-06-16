
import asyncio
from re import U
from bridgePy import connector
from dataclasses import dataclass
import struct
import ctypes
from typing import List
import threading
import time
import json
import datetime
from collections import OrderedDict





class Depth(ctypes.Structure):
    _fields_ = [
        ("quantity", ctypes.c_uint32),      # UInt32
        ("price", ctypes.c_int32),         # Int32
        ("orders", ctypes.c_int16),# Int16
        ("transactionType", ctypes.c_int16)  # Int16
    ]
    def to_dict(self):
        # Convert the Point object to a dictionary
        return {field[0]: getattr(self, field[0]) for field in self._fields_}

class MWBOCombined(ctypes.Structure):
    _pack_ = 2  # Match the `Pack = 2` in C#
    _fields_ = [
        ("ltp", ctypes.c_int32),
        ("lastTradedQuantity", ctypes.c_uint32),
        ("tradedVolume", ctypes.c_uint32),
        ("high", ctypes.c_int32),
        ("low", ctypes.c_int32),
        ("open", ctypes.c_int32),
        ("close", ctypes.c_int32),
        ("averageTradedPrice", ctypes.c_int32),
        ("reserved", ctypes.c_uint16),
        ("bestBidQuantity", ctypes.c_uint32),
        ("bestBidPrice", ctypes.c_int32),
        ("bestAskQuantity", ctypes.c_uint32),
        ("bestAskPrice", ctypes.c_int32),
        ("totalBidQuantity", ctypes.c_uint32),
        ("totalAskQuantity", ctypes.c_uint32),
        ("priceDivisor", ctypes.c_int32),
        ("lastTradedTime", ctypes.c_int32),
        ("marketDepth", Depth * 10)  # Array of 10 Depth structures

    ]

    def to_dict(self):
        # Convert fields of the Depth object to a dictionary
        result = {field[0]: getattr(self, field[0]) for field in self._fields_}
        
        # Special handling for the 'points' field, which is an array of Point objects
        if 'marketDepth' in result:
            result['marketDepth'] = [depth.to_dict() for depth in result['marketDepth']]
        
        return result


  
def bytearray_to_structure(data: bytearray) -> MWBOCombined:
    # Ensure the byte array size matches the structure size
    expected_size = ctypes.sizeof(MWBOCombined)
    data=data[:186]
    length=len(data)
    if len(data) != expected_size:
        raise ValueError(f"Byte array size ({len(data)}) does not match structure size ({expected_size})")
    return MWBOCombined.from_buffer_copy(data)




# Define the acknowledgment response handler function
# This function will be called when an connection, subscription, unsubscription and disconnection  request is acknowledged. 
# User must define and register this function in order to receive the acknowledgment message of the above rqeuests.
def acknowledgement_handler(response: str)->str:
    print(f"Acknowledgment: {response}")
    


# Define the error handler function
# This function will be called when an error occured.User must define and register this function in order to receive the error message
def error_handler(code: int, message: str):
    print(f"Error {code}: {message}")
    



# Define callback functions to handle the subscribed data. User must define and register these functions in order to receive the any kind of market data

# This function will be called when feed data is received
def feed_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    Data=bytearray_to_structure(data)    
    # Print the JSON string
    dictData=Data.to_dict()
    ordered_data = OrderedDict()
    ordered_data["topic"]=topic
    ordered_data.update(dictData)
    current_time = datetime.datetime.fromtimestamp(ordered_data["lastTradedTime"])
    ordered_data["lastTradedTime"]=str(current_time)
    
    ordered_data["ltp"]=ordered_data["ltp"]/ordered_data["priceDivisor"]
    ordered_data["high"]=ordered_data["low"]/ordered_data["priceDivisor"]
    ordered_data["open"]=ordered_data["open"]/ordered_data["priceDivisor"]
    ordered_data["close"]=ordered_data["close"]/ordered_data["priceDivisor"]
    ordered_data["low"]=ordered_data["low"]/ordered_data["priceDivisor"]
    ordered_data["low"]=ordered_data["low"]/ordered_data["priceDivisor"]
    ordered_data["low"]=ordered_data["low"]/ordered_data["priceDivisor"]
    ordered_data["low"]=ordered_data["low"]/ordered_data["priceDivisor"]


    print("Feed data received :"+ json.dumps(ordered_data))


# This function will be called when open interest data is received
def open_interest_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    format="iiii"
    unpacked=struct.unpack(format,data)
    # print(f"OI data received on topic {topic}: openInterest-{unpacked[0]},dayHighOi-{unpacked[1]},dayLowOi-{unpacked[2]},previousOi-{unpacked[3]}")
    # Create a dictionary to represent the data
    oi_data = {
        "topic": topic,
        "openInterest": unpacked[0],
        "dayHighOi": unpacked[1],
        "dayLowOi": unpacked[2],
        "previousOi": unpacked[3]
    }

    # Convert the dictionary to a JSON string
    json_data = json.dumps(oi_data, indent=4)

    # Print the JSON string
    print("Open Interest data received :"+json_data)
 
# This function will be called when lpp data is received
def lpp_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    format="IIi"
    unpacked=struct.unpack(format,data)
    # Create a dictionary to represent the data
    lpp_data = {
        "topic": topic,
        "lppHigh": unpacked[0],
        "lppLow": unpacked[1],
        "priceDivisor": unpacked[2]
    }

    # Convert the dictionary to a JSON string
    json_data = json.dumps(lpp_data, indent=4)

    # Print the JSON string
    print("Lpp data received :"+json_data) 

# This function will be called when market status data is received
def market_status_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    data=data[0:2]
    fromat="H"
    unpacked=struct.unpack(format,data)
    # Create a dictionary to represent the data
    market_status_data = {
        "topic": topic,
        "MarketStatusCode": unpacked[0]
    }

    # Convert the dictionary to a JSON string
    json_data = json.dumps(market_status_data, indent=4)

    # Print the JSON string
    print("Market status data received :"+json_data) 

# This function will be called when upper circuit data is received
def upper_circuit_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    format="IIi"
    unpacked=struct.unpack(format,data)
    # Create a dictionary to represent the data
    upper_circuit_data = {
        "topic": topic,
        "instrumentId": unpacked[0],
        "upperCircuit": unpacked[1],
        "priceDivisor": unpacked[2]
    }

    # Convert the dictionary to a JSON string
    json_data = json.dumps(upper_circuit_data, indent=4)

    # Print the JSON string
    print("upper_circuit data received :" + json_data)

# This function will be called when lower circuit data is received  
def lower_circuit_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    format="IIi"
    l=len(data)
    unpacked=struct.unpack(format,data)
    # Create a dictionary to represent the data
    lower_circuit_data = {
        "topic": topic,
        "instrumentId": unpacked[0],
        "lowerCircuit": unpacked[1],
        "priceDivisor": unpacked[2]
    }

    # Convert the dictionary to a JSON string
    json_data = json.dumps(lower_circuit_data, indent=4)

    # Print the JSON string
    print("lower_circuit data received :" + json_data)

# This function will be called when high 52week data is received
def high_52week_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    format="IIi"
    unpacked=struct.unpack(format,data)
    # Create a dictionary to represent the data
    high_52week_data = {
        "topic": topic,
        "instrumentId": unpacked[0],
        "52WeekHigh": unpacked[1],
        "priceDivisor": unpacked[2]
    }

    # Convert the dictionary to a JSON string
    json_data = json.dumps(high_52week_data, indent=4)

    # Print the JSON string
    print("high_52week data received :" + json_data)

# This function will be called when low 52week data is received
def low_52week_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    format="IIi"
    unpacked=struct.unpack(format,data)
    # Create a dictionary to represent the data
    low_52week_data = {
        "topic": topic,
        "instrumentId": unpacked[0],
        "52WeekLow": unpacked[1],
        "priceDivisor": unpacked[2]
    }

    # Convert the dictionary to a JSON string
    json_data = json.dumps(low_52week_data, indent=4)

    # Print the JSON string
    print("low_52week data received :" +json_data)

# This function will be called when order updates data is received
def order_update_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    
    print(f"Order updates data received on topic {topic}: {data.decode('utf-8')}")
  
# This function will be called when trade updates data is received
def trade_update_handler(data: bytearray, topic: str):
    # convert bytearray to structure to see the readable data
    print(f"trade update data received on topic {topic}:{data.decode('utf-8')}")
  

async def main():
    
    # Create an instance of the Connect class
    connection_obj = connector.Connect()
    
    # Register the all callbacks functions which are defined above to receive the corresponding data

    # Register the acknowledgment response handler to receive the acknowledgment message of the connection, subscription, unsubscription and disconnection requests
    connection_obj.on_acknowledge_response = acknowledgement_handler
    
    # Register the error handler to receive the error message
    connection_obj.on_error = error_handler

    # Register the feed data handler to receive the feed data
    connection_obj.on_feed_data_received = feed_handler
    
    # Register the open interest data handler to receive the open interest data
    connection_obj.on_open_interest_data_received=open_interest_handler
    
    # Register the lpp data handler to receive the lpp data
    connection_obj.on_lpp_data_received=lpp_handler
    
    # Register the market status data handler to receive the market status data
    connection_obj.on_market_status_data_received=market_status_handler
    
    # Register the upper circuit data handler to receive the upper circuit data
    connection_obj.on_upper_circuit_data_received=upper_circuit_handler
    
    # Register the lower circuit data handler to receive the lower circuit data
    connection_obj.on_lower_circuit_data_received=lower_circuit_handler
    
    # Register the high 52week data handler to receive the high 52week data
    connection_obj.on_high_52week_data_received=high_52week_handler

    # Register the low 52week data handler to receive the low 52week data
    connection_obj.on_low_52week_data_received=low_52week_handler
    
    # Register the order updates data handler to receive the order updates data
    connection_obj.on_order_updates_received=order_update_handler
    
    # Register the trade updates data handler to receive the trade updates data
    connection_obj.on_trade_update_data_received=trade_update_handler
    
    # Connect to the host
    conn_request = '{"host": "bridge.iiflcapital.com", "port": 9906, "token": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIxVks4TEhlRnRvSmp6YWk1RmJlSGNPbDI3ekpGanBScTE2Vmt4eGJBZ0ZjIn0.eyJleHAiOjE3NTQ4MDY2ODcsImlhdCI6MTczOTI1NDY4NywianRpIjoiYWMyZjEyZDktYTJkNy00M2ExLWIwMmMtZGZmZDc2Mjc1Y2M3IiwiaXNzIjoiaHR0cHM6Ly9rZXljbG9hay5paWZsc2VjdXJpdGllcy5jb20vcmVhbG1zL0lJRkwiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiNDk4NTU2NTAtMDcyNS00ZGUzLWFlMmQtN2ExMGYxYzIwODI4IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiSUlGTCIsInNpZCI6ImViZmM1NDk0LTY0NzQtNGExZC1hY2Q4LTRjM2U4ODM2MzllMCIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cDovLzEwLjEyNS42OC4xNDQ6ODA4MC8iXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRlZmF1bHQtcm9sZXMtaWlmbCIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJJSUZMIjp7InJvbGVzIjpbIkdVRVNUX1VTRVIiLCJBQ1RJVkVfVVNFUiJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgZW1haWwgcHJvZmlsZSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJ1Y2MiOiI5MzA4MDA0OCIsInNvbGFjZV9ncm91cCI6IlNVQlNDUklCRVJfQ0xJRU5UIiwibmFtZSI6IlNBVEhFRVNIIEtVTUFSIEdPUElTRVRUWSBOQSIsInByZWZlcnJlZF91c2VybmFtZSI6IjkzMDgwMDQ4IiwiZ2l2ZW5fbmFtZSI6IlNBVEhFRVNIIEtVTUFSIEdPUElTRVRUWSIsImZhbWlseV9uYW1lIjoiTkEiLCJlbWFpbCI6InNhdGlzaGt1bWFyZ29waXNldHR5MTExQGdtYWlsLmNvbSJ9.pSWSX3CHk7Fow-hltmXEdDiyE_B-KkBsfQBIR6-1NbNmRj4WvmFX8B0qZ7Mo8uf1S-Snuuo4Fy9WSG6w5v2MFsCmJn01H-Y84PVPZrRhAJFwsIA7ANe1vHfLN1f0t3Bm0JLKvQ20wF9GH5vOyfq3SvgOaJ7pClsFK_Xau4mInulWWQCC-dPDb5AIj_S8gR0QYZZPAO8879nevWIT6z3Q_9qBL0QeQJD_oPP0R7bYPR0h8bgDRs8MZFmsyfd_n7ZkZbVTTumkg8ehMXFED6JmeF6qUM15uwFH0xKBhupN701mQK0li4ACtDRzawUhFZSVbhstbtgF3chHj15JtPYXdg"}'
    response = await connection_obj.connect_host(conn_request)
    print(f"Connect response: {response}")

    
   
    # Subscribe to feed
    subscribe_request = '{"subscriptionList": ["nseeq/2885"]}'
    subscribe_response = await connection_obj.subscribe_feed(subscribe_request)
    print(f"Subscribe Feed response: {subscribe_response}")

    # # Subscribe to  oi
    # subscribe_request ='{"subscriptionList": ["nsefo/100336"]}'
    # subscribe_response = await connection_obj.subscribe_open_interest(subscribe_request)
    # print(f"Subscribe Oi response: {subscribe_response}")

    # # Subscribe to lpp
    # subscribe_request ='{"subscriptionList": ["nsefo/2885","nc2885"]}'
    # subscribe_response = await connection_obj.subscribe_lpp(subscribe_request)
    # print(f"Subscribe lpp response: {subscribe_response}")

    # # Subscribe to marketstatus
    # subscribe_request ='{"subscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # subscribe_response = await connection_obj.subscribe_market_status(subscribe_request)
    # print(f"Subscribe market status response: {subscribe_response}")

    # # Subscribe to uppercircuit
    # subscribe_request ='{"subscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # subscribe_response = await connection_obj.subscribe_upper_circuit(subscribe_request)
    # print(f"Subscribe upperCircuit response: {subscribe_response}")

    # # Subscribe to lowercircuit
    # subscribe_request ='{"subscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # subscribe_response = await connection_obj.subscribe_lower_circuit(subscribe_request)
    # print(f"Subscribe lowerCircuit response: {subscribe_response}")

    # # Subscribe to high_52week
    # subscribe_request ='{"subscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # subscribe_response = await connection_obj.subscribe_high_52week(subscribe_request)
    # print(f"Subscribe high_52week response: {subscribe_response}")

    # # Subscribe to low_52week
    # subscribe_request ='{"subscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # subscribe_response = await connection_obj.subscribe_low_52week(subscribe_request)
    # print(f"Subscribe low_52week response: {subscribe_response}")

    # # Subscribe to  order updates
    # subscribe_request ='{"subscriptionList": ["93080048"]}'
    # subscribe_response = await connection_obj.subscribe_order_updates(subscribe_request)
    # print(f"Subscribe low_52week response: {subscribe_response}")

    # # Subscribe to trade updates
    # subscribe_request ='{"subscriptionList": ["93080048"]}'
    # subscribe_response = await connection_obj.subscribe_trade_updates(subscribe_request)
    # print(f"Subscribe low_52week response: {subscribe_response}")

    # # Unsubscribe from feed
    # unsubscribe_request = '{"unsubscriptionList": ["nseeq/2885","bseeq/199018","mcxcomm/428770","ncdexcomm/72926"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_feed(unsubscribe_request)
    # print(f"Unsubscribe feed response: {unsubscribe_response}")
    
    # # Unsubscribe from oi
    # unsubscribe_request = '{"unsubscriptionList": ["nseeq/2885","bsefo/1152436","mcxcomm/429816"]}'
    # unsubscribe_response= await connection_obj.unsubscribe_open_interest(unsubscribe_request)
    # print(f"Unsubscribe oi response: {unsubscribe_response}")
    
    # # Unsubscribe from lpp
    # unsubscribe_request = '{"unsubscriptionList": ["nsefo/2885+"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_lpp(unsubscribe_request)
    # print(f"Unsubscribe lpp response: {unsubscribe_response}")
    
    # # Unsubscribe from  marketstatus
    # unsubscribe_request = '{"unsubscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_market_status(unsubscribe_request)
    # print(f"Unsubscribe market status response: {unsubscribe_response}")
    
    # # Unsubscribe from uppercircuit
    # unsubscribe_request = '{"unsubscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_upper_circuit(unsubscribe_request)
    # print(f"Unsubscribe upperCircuit response: {unsubscribe_response}")
    
    # # Unsubscribe from lowercircuit
    # unsubscribe_request = '{"unsubscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_lower_circuit(unsubscribe_request)
    # print(f"Unsubscribe lowerCircuit response: {unsubscribe_response}")
    
    # # Unsubscribe from high_52week
    # unsubscribe_request = '{"unsubscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_high_52week(unsubscribe_request)
    # print(f"Unsubscribe high_52week response: {unsubscribe_response}")

    # # Unsubscribe from low_52week
    # unsubscribe_request = '{"unsubscriptionList": ["nseeq","bseeq","nsefo","bsefo","mcxcomm","ncdexcomm"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_low_52week(unsubscribe_request)
    # print(f"Unsubscribe low_52week response: {unsubscribe_response}")
    
    # # Unsubscribe from order update
    # unsubscribe_request = '{"unsubscriptionList": ["93080048"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_order_updates(unsubscribe_request)
    # print(f"Unsubscribe order update response: {unsubscribe_response}")
    
    # # Unsubscribe from trade update
    # unsubscribe_request = '{"unsubscriptionList": ["93080048"]}'
    # unsubscribe_response = await connection_obj.unsubscribe_trade_updates(unsubscribe_request)
    # print(f"Unsubscribe trade update response: {unsubscribe_response}")
    
    # response = await connection_obj.connect_host(conn_request)
    # print(f"Connect response: {response}")
    
    await asyncio.sleep(100)
   
    #Disconnect from the host
    disconnect_response = await connection_obj.disconnect_host()
    print(f"Disconnect response:{disconnect_response}")


# Run the main function
asyncio.run(main())