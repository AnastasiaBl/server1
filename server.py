import asyncio

metric = dict()


class ClientServerProtocol(asyncio.Protocol):
    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(self.make_process(data.decode('utf-8').strip('\r\n')).encode('utf-8'))
        resp = data.decode('utf-8')
        res = resp.strip('\r\n').split(' ')

    def make_process(self, comm):
        res = comm.split(' ')
        if res[0] == 'get':
            return self.get(res[1])
        elif res[0] == 'put':
            return self.put(res[1], res[2], res[3])
        else:
            error = 'error\nwrong command\n\n'
            return error

    @staticmethod
    def get(key):
        res = 'ok\n'
        if key == '*':
            for key, values in metric.items():
                for value in values:
                    res = res + key + ' ' + value[1] + ' ' + value[0] + '\n'
        else:
            if key in metric:
                for value in metric[key]:
                    res = res + key + ' ' + value[1] + ' ' + value[0] + '\n'

        return res + '\n'

    @staticmethod
    def put(key, value, timestamp):
        if key == '*':
            return 'error\nkey cannot contain *\n\n'
        if key not in metric:
            metric[key] = list()
        if not (timestamp, value) in metric[key]:
            metric[key].append((timestamp, value))
            metric[key].sort(key=lambda tup: tup[0])
        return 'ok\n\n'

def run_server(host,port):
    loop = asyncio.get_event_loop()
    coroutine = loop.create_server(ClientServerProtocol, host='127.0.0.1',port=8888)

    server = loop.run_until_complete(coroutine)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    run_server("127.0.0.1", 8888)






