class ConnectionRequest:
    def __init__(self, host: str, port: int,clientCode:str, token: str) -> None:
        """
        Initialize a ConnectionRequest.

        :param host: The host address.
        :param port: The port number.
        :param token: The authentication token.
        """
        self.host = host
        self.port = port
        self.token = token


class ConnectionResponse:
    def __init__(self, status: int , message: str) -> None:
        """
        Initialize a ConnectionResponse.

        :param status: The response status.
        :param message: The  response message.
        """
        self.status = status
        self.message = message


class SubscribeFeedRequest:
    def __init__(self, subscriptionList: list[str]) -> None:
        """
        Initialize a SubscribeFeedRequest.

        :param subscriptionList: List of topics to subscribe to.
        """
        self.subscriptionList = subscriptionList


class SubscribeResult:
    def __init__(self, resultCode: int, result:str ,topic: str) -> None:
        """
        Initialize a SubscribeResult.

        :param topic: The topic name.
        :param resultCode: The resultCode code.
        :param result: The result message.
        """
        self.resultCode = resultCode
        self.result=result
        self.topic = topic

    def to_dict(self) -> dict:
        """
        Convert the SubscribeResult to a dictionary.

        :return: Dictionary representation of the SubscribeResult.
        """
        return {
            "topic": self.topic,
            "resultCode": self.resultCode,
            "result":self.result
        }


class SubscribeAckResponse:
    def __init__(self, packetType: int,packetName :str, subscriptionResult: list[SubscribeResult]) -> None:
        """
        Initialize a SubscribeAckResponse.

        :param packetType: The packet type.
        :param packetName: The packet name.
        :param subscriptionResult: List of SubscribeResult objects.
        """
        self.packetType = packetType
        self.packetName=packetName
        self.subscriptionResult = subscriptionResult


class SubscribeFeedResponse:
    def __init__(self, status: int, message: str, failedTopics: list[SubscribeResult]) -> None:
        """
        Initialize a SubscribeFeedResponse.

        :param status: The status code.
        :param message: The response message.
        :param failedTopics: List of failed topics.
        """
        self.status = status
        self.message = message
        self.failedTopics = failedTopics


class UnsubscribeFeedRequest:
    def __init__(self, unsubscriptionList: list[str]) -> None:
        """
        Initialize an UnsubscribeFeedRequest.

        :param unsubscriptionList: List of topics to unsubscribe from.
        """
        self.unsubscriptionList = unsubscriptionList


class UnsubscribeAckResponse:
    def __init__(self, packetType: int,packetName :str, status: int, message:str) -> None:
        """
        Initialize an UnsubscribeAckResponse.

        :param packet_type: The packet type.
        :param packetName: The packet name.
        :param status: The response status.
        :param message: The response message.
        """
        self.packetType = packetType
        self.packetName=packetName
        self.status = status
        self.message=message


class UnsubscribeFeedResponse:
    def __init__(self, status: int, message: str, failedTopics: list[SubscribeResult]) -> None:
        """
        Initialize an UnsubscribeResponse.

        :param status: The status code.
        :param message: The response message.
        :param failedTopics: List of failedTopics topics.
        """
        self.status = status
        self.message = message
        self.failedTopics = failedTopics


class DisconnectResponse:
    def __init__(self, status: int, message: str) -> None:
        """
        Initialize a DisconnectResponse.

        :param status: The status code.
        :param message: The response message.
        """
        self.status = status
        self.message = message


class CommonResponse:
    def __init__(self, status: int, packet_type: int,packetName :str, message: str) -> None:
        """
        Initialize a CommonResponse.

        :param packet_type: The packet type.
        :param packetName: The packet name .
        :param status: The response status .
        :param message: The response message.
        """
        self.packetType = packet_type
        self.packetName=packetName
        self.status = status
        self.message = message

class ValidateTokenRequest:
    def __init__(self, userId:str, token:str)->None:        
        """
        Initialize a ValidateTokenRequest

        :param clientId: ClinetCode
        :param token: Access token
        """
        self.userId=userId
        self.token=token

