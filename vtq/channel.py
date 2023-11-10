class Channel:
    """any messsage queue, such as pub/sub system, or signal system

    E.g. it can be Blinker Signal, or ZeroMQ, etc.
    """

    def send(self, data):
        pass
