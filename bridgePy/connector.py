from datetime import datetime
import ssl
import threading
import paho.mqtt.client as mqtt
import json
import requests
import re
from typing import Callable, Optional
from paho.mqtt.enums import CallbackAPIVersion
from . import models as ds
import logging
import base64


CallbackOnHandleEventResponse = Callable[[str], None]
CallbackOnData = Callable[[bytearray, str], None]
CallbackOnError = Callable[[int, str], None]

logging.basicConfig(level=logging.INFO)

class Connect:
    """
    Singleton class to manage MQTT client connections, subscriptions, unsubscriptions, and message handling.

    This class provides a connection object for managing MQTT client interactions.

    Features:
    - Properties to set callback functions for handling:
      - Data receive events
      - Request acknowledgments (connect, subscribe, unsubscribe, disconnect)
      - Error events
    - Methods to:
      - Connect to the host
      - Subscribe to topics
      - Unsubscribe from topics
      - Disconnect from the host
    """
    _instance = None   # Class variable to hold the single instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized:
            self.__initialize_mqtt_client()
            self._initialized = True
            self._callback_mutex = threading.RLock()
            self.__initialize_callbacks()
            self.__initialize_topics()
            self.__subscriptions = {}
            self.lock = threading.Lock()
            self.count = 0

    def __initialize_mqtt_client(self) -> None:
        self.__mqttClient = mqtt.Client(
            client_id="bridgePy" + datetime.now().strftime("%d%m%y%H%M%S"),
            protocol=mqtt.MQTTv311,
            callback_api_version=CallbackAPIVersion.VERSION2,
            reconnect_on_failure=False
        )
        self.__mqttClient.tls_set(
            ca_certs=None, certfile=None, keyfile=None,
            cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None
        )
        self.__mqttClient.tls_insecure_set(True)
        self.__mqttClient.keepalive = 20

    def __initialize_callbacks(self) -> None:
        self.__on_feed_data_received: Optional[CallbackOnData] = None
        self.__on_index_data_received: Optional[CallbackOnData] = None
        self.__on_open_interest_data_received: Optional[CallbackOnData] = None
        self.__on_market_status_data_received: Optional[CallbackOnData] = None
        self.__on_lpp_data_received: Optional[CallbackOnData] = None
        self.__on_high_52_week_data_received: Optional[CallbackOnData] = None
        self.__on_low_52_week_data_received: Optional[CallbackOnData] = None
        self.__on_upper_circuit_data_received: Optional[CallbackOnData] = None
        self.__on_lower_circuit_data_received: Optional[CallbackOnData] = None
        self.__on_order_updates_received: Optional[CallbackOnData] = None
        self.__on_trade_updates_received: Optional[CallbackOnData] = None
        self.__on_acknowledge_response: Optional[CallbackOnHandleEventResponse] = None
        self.__on_error: Optional[CallbackOnError] = None

    def __initialize_topics(self) -> None:
        self.mw_topic = "prod/marketfeed/mw/v1/"
        self.index_topic = "prod/marketfeed/index/v1/"
        self.oi_topic = "prod/marketfeed/oi/v1/"
        self.market_status = "prod/marketfeed/marketstatus/v1/"
        self.lpp = "prod/marketfeed/lpp/v1/"
        self.high_52_week = "prod/marketfeed/high52week/v1/"
        self.low_52_week = "prod/marketfeed/low52week/v1/"
        self.upper_circuit = "prod/marketfeed/uppercircuit/v1/"
        self.lower_circuit = "prod/marketfeed/lowercircuit/v1/"
        self.order_updates = "prod/updates/order/v1/"
        self.trade_updates = "prod/updates/trade/v1/"
        self.validateTokenUrl="https://idaas.iiflsecurities.com/v1/access/check/token"
        self.__topic_handler = {}
        
    
    @property
    def is_host_connected(self) -> bool:
        """
        Checks if the MQTT client is connected to the host.

        Returns:
            bool: True if connected, False otherwise.
        """
        try:
            return self.__mqttClient.is_connected()
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))
            return False
        

    @property
    def on_feed_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the registered callback function for processing feed data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_feed_data_received

    @on_feed_data_received.setter
    def on_feed_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function to process received feed data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be invoked whenever data is received on the feed topic.
        """
        self.__set_callback(func, self.mw_topic, '__on_feed_data_received')

    @property
    def on_index_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the registered callback function for processing index data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_index_data_received

    @on_index_data_received.setter
    def on_index_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function to process received index data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be invoked whenever data is received on the index topic.
        """
        self.__set_callback(func, self.index_topic, '__on_index_data_received')

    @property
    def on_open_interest_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the registered callback function for processing open interest data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_open_interest_data_received

    @on_open_interest_data_received.setter
    def on_open_interest_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function to process received open interest data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be invoked whenever data is received on the open interest topic.
        """
        self.__set_callback(func, self.oi_topic, '__on_open_interest_data_received')

    @property
    def on_market_status_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the registered callback function for processing market status data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_market_status_data_received

    @on_market_status_data_received.setter
    def on_market_status_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function to process received market status data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be invoked whenever data is received on the market status topic.
        """
        self.__set_callback(func, self.market_status, '__on_market_status_data_received')

    @property
    def on_lpp_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the registered callback function for processing lpp data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_lpp_data_received

    @on_lpp_data_received.setter
    def on_lpp_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function to process received lpp data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be invoked whenever data is received on the lpp topic.
        """
        self.__set_callback(func, self.lpp, '__on_lpp_data_received')

    @property
    def on_high_52_week_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the registered callback function for processing high 52-week data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_high_52_week_data_received

    @on_high_52_week_data_received.setter
    def on_high_52_week_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function to process received high 52-week data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be invoked whenever data is received on the high 52-week topic.
        """
        self.__set_callback(func, self.high_52_week, '__on_high_52_week_data_received')

    @property
    def on_low_52_week_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the registered callback function for processing low 52-week data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_low_52_week_data_received

    @on_low_52_week_data_received.setter
    def on_low_52_week_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function to process received low 52-week data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be invoked whenever data is received on the low 52-week topic.
        """
        self.__set_callback(func, self.high_52_week, '__on_low_52_week_data_received')

    @property
    def on_lower_circuit_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the currently registered callback function for handling lower circuit data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_lower_circuit_data_received

    @on_lower_circuit_data_received.setter
    def on_lower_circuit_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function for handling lower circuit data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be called whenever data is received on the lower circuit topic.
        """
        self.__set_callback(func, self.lower_circuit, '__on_lower_circuit_data_received')


    @property
    def on_upper_circuit_data_received(self) -> Optional[CallbackOnData]:
        """
        Retrieves the currently registered callback function for handling upper circuit data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_upper_circuit_data_received

    @on_upper_circuit_data_received.setter
    def on_upper_circuit_data_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function for handling upper circuit data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be called whenever data is received on the upper circuit topic.
        """
        self.__set_callback(func, self.upper_circuit, '__on_upper_circuit_data_received')
    
    @property
    def on_order_updates_received(self)-> Optional[CallbackOnData]:
        """
        Retrieves the currently registered callback function for handling order updates data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_order_updates_received
    
    @on_order_updates_received.setter
    def on_order_updates_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function for handling order updates data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be called whenever data is received on the order updates topic.
        """
        self.__set_callback(func, self.order_updates, '__on_order_updates_received')
        
    @property
    def on_trade_updates_received(self)-> Optional[CallbackOnData]:
        """
        Retrieves the currently registered callback function for handling trade updates data.

        Returns:
            Optional[CallbackOnData]: The registered callback function, or None if no callback is set.
        """
        return self.__on_trade_updates_received
    
    @on_trade_updates_received.setter
    def on_trade_updates_received(self, func: Optional[CallbackOnData]) -> None:
        """
        Registers a callback function for handling trade updates data.

        Args:
            func (Optional[CallbackOnData]): The callback function to register.
                The function should have the following signature: (bytearray, str).

        Behavior:
            This function will be called whenever data is received on the trade updates topic.
        """
        self.__set_callback(func, self.trade_updates, '__on_trade_updates_received')
    
        
    def __set_callback(self, func: Optional[CallbackOnData], topic: str, attr_name: str) -> None:
        with self._callback_mutex:
            setattr(self, attr_name, func)
            self.__topic_handler[topic] = func
            
    @property
    def on_acknowledge_response(self) -> Optional[CallbackOnHandleEventResponse]:
        """
        Retrieves the registered callback function for handling acknowledgment responses.

        Returns:
            Optional[CallbackOnHandleEventResponse]: The registered callback function, or None if no callback is set.
        """
        return self.__on_acknowledge_response

    @on_acknowledge_response.setter
    def on_acknowledge_response(self, func: Optional[CallbackOnHandleEventResponse]) -> None:
        """
        Registers a callback function to handle acknowledgment responses for 
        connection, subscription, unsubscription, and disconnection events.

        Args:
            func (Optional[CallbackOnHandleEventResponse]): The callback function to register.
                The function should have the following signature: (str).

        Behavior:
            The callback function will be invoked with a string parameter that 
            represents the acknowledgment details.
        """
        self.__on_acknowledge_response = func

    @property
    def on_error(self) -> Optional[CallbackOnError]:
        """
        Retrieves the registered callback function for handling errors.

        Returns:
            Optional[CallbackOnError]: The registered callback function, or None if no callback is set.
        """
        return self.__on_error

    @on_error.setter
    def on_error(self, func: Optional[CallbackOnError]) -> None:
        """
        Registers a callback function to handle errors.

        Args:
            func (Optional[CallbackOnError]): The callback function to register.
                The function should have the following signature: (int, str).

        Behavior:
            The callback function will be invoked whenever an error occurs. 
            It receives two parameters:
            - int: The error code.
            - str: The error message.
        """
        self.__on_error = func

    async def connect_host(self, conn_req: str) -> str:
        """
        This function establishes a connection to the specified MQTT broker using the provided connection request when called.

        Args:
            conn_req (str): JSON string containing connection details. Example:
                {
                    "host": "bridge.iiflcapital.com",
                    "port": 9906,
                    "token": "your_token_here"
                }

        Returns:
            str: JSON string containing the connection response. Example:
                {
                    "status": 0,
                    "message": "Connection successful"
                }
        """
        try:
            if not conn_req:
                return json.dumps(ds.ConnectionResponse(101,"Request cannot be null").__dict__)

            req_data = json.loads(conn_req)
            clientID=self.__get_user_name(req_data["token"])
            
            valRes=self.__validate_Token(clientID,req_data["token"])
            if not valRes[0]=="Success":
                return json.dumps(ds.ConnectionResponse(1,str(valRes[1])).__dict__)

            self.__mqttClient.on_message = self.__on_message
            self.__mqttClient.on_disconnect = self.__on_disconnect
            self.__mqttClient.on_connect = self.__on_connect
            self.__mqttClient.on_subscribe = self.__on_subscribe
            self.__mqttClient.on_unsubscribe = self.__on_unsubscribe

            if self.is_host_connected:
                 return json.dumps(ds.ConnectionResponse(105,"Client is already connected").__dict__)
                
            self.__mqttClient.username_pw_set(
                username=clientID,
                password="OPENID~~" + req_data["token"] + "~"
            )
            self.__mqttClient.clean_session = True
            res = self.__mqttClient.connect(host=req_data["host"], port=req_data["port"],keepalive=20)

            if res.value == 0:
                self.__mqttClient.loop_start()
            return json.dumps(ds.ConnectionResponse(res.value,mqtt.error_string(res)).__dict__)
        except json.JSONDecodeError as e:
            if self.__on_error:
                self.__on_error(-1, f"Invalid JSON: {e}")
            return json.dumps(ds.ConnectionResponse(-1,f"Invalid JSON: {e}").__dict__)
        except KeyError as ex:
            if self.__on_error:
                self.__on_error(-1, f"The parameter '{ex.args[0]}' should not be empty")
            return json.dumps(ds.ConnectionResponse(-1,f"The parameter '{ex.args[0]}' should not be empty").__dict__)
        except Exception as ex:
            if ex.errno is not None and ex.errno==11001:
                self.__on_error(ex.errno, "Invalid host name")
                return json.dumps(ds.ConnectionResponse(ex.errno, "Invalid host name").__dict__)
            if self.__on_error:
                self.__on_error(-1, str(ex))
            return json.dumps(ds.ConnectionResponse(-1,str(ex)).__dict__)

    async def subscribe_feed(self, req: str) -> str:
        """
        Subscribes to the feed topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["nseeq/2285", "bsefo/1113375"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.mw_topic)

    async def subscribe_index(self, req: str) -> str:
        """
        Subscribes to the index topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["nsefo/999920019","bseeq/999907"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.index_topic)

    async def subscribe_open_interest(self, req: str) -> str:
        """
        Subscribes to the open interest topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["nseeq/2885","bsefo/1152436","mcxcomm/429816"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.oi_topic)

    async def subscribe_market_status(self, req: str) -> str:
        """
        Subscribes to the market status topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["nseeq", "mcxcomm"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.market_status)

    async def subscribe_lpp(self, req: str) -> str:
        """
        Subscribes to the LPP topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["nsefo/2885"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.lpp)

    async def subscribe_high_52week(self, req: str) -> str:
        """
        Subscribes to the high 52-week topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["nseefo", "bsefo"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.high_52_week)

    async def subscribe_low_52week(self, req: str) -> str:
        """
        Subscribes to the low 52-week topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["ncdexcomm", "nsecurr"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.low_52_week)

    async def subscribe_upper_circuit(self, req: str) -> str:
        """
        Subscribes to the upper circuit topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["bsecurr", "bsefo"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.upper_circuit)

    async def subscribe_lower_circuit(self, req: str) -> str:
        """
        Subscribes to the lower circuit topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["mcxcomm", "nsecomm"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.lower_circuit)

    async def subscribe_order_updates(self, req: str) -> str:
        """
        Subscribes to the order updates .

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["s0007"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.order_updates)
    
    async def subscribe_trade_updates(self, req: str) -> str:
        """
        Subscribes to the trade updates .

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["s0007"]
                }

        Returns:
            str: The subscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__subscribe(req, self.trade_updates)

    async def unsubscribe_feed(self, req: str) -> str:
        """
        Unsubscribes from the feed topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["nseeq/2285", "bsefo/1113375"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.mw_topic)

    async def unsubscribe_index(self, req: str) -> str:
        """
        Unsubscribes from the index topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["nsefo/999920019","bseeq/999907"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.index_topic)

    async def unsubscribe_open_interest(self, req: str) -> str:
        """
        Unsubscribes from the open interest topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["nseeq/2885","bsefo/1152436","mcxcomm/429816"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.oi_topic)

    async def unsubscribe_market_status(self, req: str) -> str:
        """
        Unsubscribes from the market status topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList":["nseeq", "mcxcomm"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.market_status)

    async def unsubscribe_lpp(self, req: str) -> str:
        """
        Unsubscribes from the LPP topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["nsefo/2885"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.lpp)

    async def unsubscribe_high_52week(self, req: str) -> str:
        """
        Unsubscribes from the high 52-week topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["nseefo", "bsefo"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.high_52_week)

    async def unsubscribe_low_52week(self, req: str) -> str:
        """
        Unsubscribes from the low 52-week topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["ncdexcomm", "nsecurr"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.low_52_week)

    async def unsubscribe_upper_circuit(self, req: str) -> str:
        """
        Unsubscribes from the upper circuit topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["bsecurr", "bsefo"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.upper_circuit)

    async def unsubscribe_lower_circuit(self, req: str) -> str:
        """
        Unsubscribes from the lower circuit topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["mcxcomm", "nsecomm"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.lower_circuit)

    async def unsubscribe_order_updates(self, req: str) -> str:
        """
        Unsubscribes from the order updates.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["s0007"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.order_updates)
    
    async def unsubscribe_trade_updates(self, req: str) -> str:
        """
        Unsubscribes from the trade updates.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["s0007"]
                }

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        return await self.__unsubscribe(req, self.trade_updates)

    async def __subscribe(self, req: str, topic_prefix: str) -> str:
        """
        Subscribes to the specified topic.

        Args:
            req (str): The subscription request in JSON format. Example:
                {
                    "subscriptionList": ["topic1", "topic2"]
                }
            topic_prefix (str): The prefix to be added to each topic.

        Returns:
            str: The subscription response in JSON form   at. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        try:
            if not req:
                return json.dumps(ds.SubscribeFeedResponse(101, "Request cannot be null", []).__dict__)

            res_data = ds.SubscribeFeedResponse(0, None, [])
            req_data = json.loads(req)
            valid_topics = []

            if req_data["subscriptionList"] and len(req_data["subscriptionList"]) <= 1024:
                self._build_subscription_topics(req_data, res_data, topic_prefix, valid_topics)
            else:
                return json.dumps(ds.SubscribeFeedResponse(102, "TopicList cannot be null and no. of topics should be less than 1024", []), default=lambda o: o.__dict__, indent=4)

            if valid_topics:
                res = self.__mqttClient.subscribe(valid_topics)
                if res[0].value == 0:
                    with self.lock:
                        self.__subscriptions[res[1]] = valid_topics
                    if len(res_data.failedTopics)>0:
                        res_data.message = "Subscription partially sent"
                    else:
                        res_data.message = mqtt.error_string(res[0])
                else:
                    res_data.message = mqtt.error_string(res[0])
                res_data.status = res[0].value
                return json.dumps(res_data.__dict__)
            else:
                res_data.status = 103
                res_data.message = "Subscripton failed"
                return json.dumps(res_data.__dict__)
        except json.JSONDecodeError as e:
            if self.__on_error:
                self.__on_error(-1, f"Invalid JSON: {e}")
            return json.dumps(ds.SubscribeFeedResponse(-1,f"Invalid JSON: {e}",[]).__dict__)
        except KeyError as ex:
            if self.__on_error:
                self.__on_error(-1, f"The parameter '{ex.args[0]}' should not be empty")
            return json.dumps(ds.SubscribeFeedResponse(-1,f"The parameter '{ex.args[0]}' should not be empty",[]).__dict__)
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))
            return json.dumps(ds.SubscribeFeedResponse(-1, str(ex), []).__dict__)

    async def __unsubscribe(self, req: str, topic_prefix: str) -> str:
        """
        Unsubscribes from the specified topic.

        Args:
            req (str): The unsubscription request in JSON format. Example:
                {
                    "unsubscriptionList": ["topic1", "topic2"]
                }
            topic_prefix (str): The prefix to be added to each topic.

        Returns:
            str: The unsubscription response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success",
                    "failedTopics": []
                }
        """
        try:
            if not req:
                return json.dumps(ds.UnsubscribeFeedResponse(101, "Request cannot be null", []).__dict__)

            res_data = ds.UnsubscribeFeedResponse(0, None, [])
            req_data = json.loads(req)
            valid_topics = []

            if req_data["unsubscriptionList"] and len(req_data["unsubscriptionList"]) <= 1024:
                self._build_unsubscribe_options(req_data, res_data, topic_prefix, valid_topics)
            else:
                return json.dumps(ds.UnsubscribeFeedResponse(102, "TopicList cannot be null and no. of topics should be less than 1024", []), default=lambda o: o.__dict__, indent=4)

            if valid_topics:
                res = self.__mqttClient.unsubscribe(valid_topics)
                if res[0].value==0:
                    res_data.status = res[0].value
                    if len(res_data.failedTopics)>0:
                        res_data.message = "Unsubscripton partially sent"
                    else:
                        res_data.message = mqtt.error_string(res[0])
                else:
                    res_data.message = mqtt.error_string(res[0])
                res_data.status = res[0].value
                return json.dumps(res_data.__dict__)
            else:
                res_data.status = 103
                res_data.message = "Unsubscription failed"
                return json.dumps(res_data.__dict__)
        except KeyError as ex:
            if self.__on_error:
                self.__on_error(-1, f"The parameter '{ex.args[0]}' should not be empty")
            return json.dumps(ds.UnsubscribeFeedResponse(-1,f"The parameter '{ex.args[0]}' should not be empty",[]).__dict__)
        except json.JSONDecodeError as e:
            if self.__on_error:
                self.__on_error(-1, f"Invalid JSON: {e}")
            return json.dumps(ds.UnsubscribeFeedResponse(-1,f"Invalid JSON: {e}",[]).__dict__)
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))
            return json.dumps(ds.UnsubscribeFeedResponse(-1, str(ex), []).__dict__)
        
    async def disconnect_host(self) -> str:
        """
        Disconnects the MQTT client from the host.

        Returns:
            str: The disconnection response in JSON format. Example:
                {
                    "status": 0,
                    "message": "Success"
                }
        """
        try:
            # await asyncio.sleep(1)
            res= self.__mqttClient.disconnect()
            self.__mqttClient.loop_stop()
            return json.dumps(ds.DisconnectResponse(res.value,mqtt.error_string(res)).__dict__)
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1,str(ex))
            return json.dumps(ds.DisconnectResponse(-1,str(ex)).__dict__)

    def _build_subscription_topics(self, req_data, res_data, topic_prefix, valid_topics):
        """
        Builds the list of valid subscription topics.

        Args:
            req_data (dict): The subscription request data.
            res_data (ds.SubscribeFeedResponse): The subscription response data.
            topic_prefix (str): The prefix to be added to each topic.
            valid_topics (list): The list to store valid topics.
        """
        try:
            pattern = r'^[0-9a-z/]+$'
            for topic in req_data["subscriptionList"]:
                if re.fullmatch(pattern, topic):
                    valid_topics.append((topic_prefix + topic, 0))
                else:
                    res_data.failedTopics.append(ds.SubscribeResult(104,"Invalid topic", topic).to_dict())
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))

    def _build_unsubscribe_options(self, req_data, res_data, topic_prefix, valid_topics):
        """
        Builds the list of valid unsubscription topics.

        Args:
            req_data (dict): The unsubscription request data.
            res_data (ds.UnsubscribeFeedResponse): The unsubscription response data.
            topic_prefix (str): The prefix to be added to each topic.
            valid_topics (list): The list to store valid topics.
        """
        try:
            pattern = r'^[0-9a-z/]+$'
            for topic in req_data["unsubscriptionList"]:
                if re.fullmatch(pattern, topic):
                    valid_topics.append(topic_prefix + topic)
                else:
                    res_data.failedTopics.append(ds.SubscribeResult(104,"Invalid topic", topic).to_dict())
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))

    def __validate_Token(self,clientID:str,token:str) -> tuple:
        """
        Validates the provided token.

        Args:
            clientID (str): The client ID.
            token (str): The token to be validated.

        Returns:
            tuple: A tuple containing the validation status and message.
        """
        try:
           req=ds.ValidateTokenRequest(clientID,token)
           jreq=json.dumps(req.__dict__)
           jres=requests.post(url=self.validateTokenUrl,data=jreq,verify=False)
           res = json.loads(jres.text)
           if jres.status_code==200:
               return (res["result"]["status"],res["result"]["message"])
           else:
               return (False,res.text)
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))
            return (False,str(ex))
     
    def __get_user_name(self,token):
        """
        Extracts the username from the provided token.

        Args:
            token (str): The token from which to extract the username.

        Returns:
            str: The extracted username.
        """
        try:
            token_parts = token.split('.')
            payload = token_parts[1]
            payload += '=' * ((4 - len(payload) % 4) % 4)  # Pad the payload to make it a multiple of 4
            json_payload = base64.urlsafe_b64decode(payload)
            jwt_payload = json.loads(json_payload)
            return jwt_payload["preferred_username"]
        except Exception as ex:
            return "tester"

    def __on_message(self, client, userdata, message):
        """
        Callback function to handle incoming messages.

        Args:
            client: The MQTT client instance.
            userdata: The private user data.
            message: The received message.
        """
        try:
            payload = message.payload
            split_topic = re.split(r'v1/', message.topic)
            self.count += 1

            if self.__topic_handler.get(split_topic[0] + "v1/"):
                self.__topic_handler.get(split_topic[0] + "v1/")(payload, split_topic[1])
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))

    def __on_disconnect(self, client, userdata, flags, rc, properties):
        """
        Callback function to handle disconnection events.

        Args:
            client: The MQTT client instance.
            userdata: The private user data.
            flags: The disconnection flags.
            rc: The reason code for disconnection.
            properties: The properties associated with the disconnection.
        """
        try:
            res = ds.CommonResponse(rc.value, rc.packetType,"DISCONNECT", str(rc))
            if self.__on_acknowledge_response:
                self.__on_acknowledge_response(json.dumps(res.__dict__))
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))

    def __on_connect(self, client, userdata, flags, rc, properties):
        """
        Callback function to handle connection events.

        Args:
            client: The MQTT client instance.
            userdata: The private user data.
            flags: The connection flags.
            rc: The reason code for connection.
            properties: The properties associated with the connection.
        """
        try:
            res = ds.CommonResponse(rc.value, rc.packetType, "CONNACK" ,str(rc))
            if self.__on_acknowledge_response:
                self.__on_acknowledge_response(json.dumps(res.__dict__))
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))

    def __on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        """
        Callback function to handle subscription events.

        Args:
            client: The MQTT client instance.
            userdata: The private user data.
            mid: The message ID.
            reason_code_list: The list of reason codes for the subscription.
            properties: The properties associated with the subscription.
        """
        try:
            ls = []
            for reason_code, topic in zip(reason_code_list, self.__subscriptions.get(mid, ("Unknown", 0))):
                if(reason_code.value==0):
                    res = ds.SubscribeResult(reason_code.value,"Granted", re.split(r'v1/', topic[0])[1])
                else:
                    res = ds.SubscribeResult(reason_code.value,"Not Granted", re.split(r'v1/', topic[0])[1])
                ls.append(res.to_dict())
            with self.lock:
                del self.__subscriptions[mid]

            res = ds.SubscribeAckResponse(reason_code_list[0].packetType,"SUBACK", ls)
            if self.__on_acknowledge_response:
                self.__on_acknowledge_response(json.dumps(res.__dict__))
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))

    def __on_unsubscribe(self, client, userdata, mid, reason_code_list, properties):
        """
        Callback function to handle unsubscription events.

        Args:
            client: The MQTT client instance.
            userdata: The private user data.
            mid: The message ID.
            reason_code_list: The list of reason codes for the unsubscription.
            properties: The properties associated with the unsubscription.
        """
        try:
            res = ds.UnsubscribeAckResponse(properties.packetType,"UNSUBACK",0, "Unsubscribed Successfully")
            if self.__on_acknowledge_response:
                self.__on_acknowledge_response(json.dumps(res.__dict__))
        except Exception as ex:
            if self.__on_error:
                self.__on_error(-1, str(ex))