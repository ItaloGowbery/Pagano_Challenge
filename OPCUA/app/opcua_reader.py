from opcua import ua, Server
import random
import time
import threading

# Configurações do Servidor
server = Server()
server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")

# Adicionando um namespace
uri = "http://examples.freeopcua.github.iohttp://examples.freeopcua.github.io"
idx = server.register_namespace(uri)

# Criando um objeto e variáveis no servidor
objects = server.get_objects_node()
myobj = objects.add_object(idx, "MyObject")

# Adicionando variáveis
counter = myobj.add_variable(idx, "Counter", 0)
random_val_30_60 = myobj.add_variable(idx, "RandomValue30to60", 0)
random_val_0_1000 = myobj.add_variable(idx, "RandomValue0to1000", 0)
random_val_0_5000 = myobj.add_variable(idx, "RandomValue0to5000", 0)

# Permitir a gravação nas variáveis
counter.set_writable()
random_val_30_60.set_writable()
random_val_0_1000.set_writable()
random_val_0_5000.set_writable()

# Função para atualizar os valores das tags periodicamente
def update_tags():
    count = 0
    while True:
        count += 1
        random_30_60 = random.randint(30, 60)
        random_0_1000 = random.randint(0, 1000)
        random_0_5000 = random.randint(0, 5000)

        print(f"Atualizando tags - Counter: {count}, Random 1: {random_30_60}, Random 2: {random_0_1000}, Random 3: {random_0_5000}")

        counter.set_value(count)
        random_val_30_60.set_value(random_30_60)
        random_val_0_1000.set_value(random_0_1000)
        random_val_0_5000.set_value(random_0_5000)

        time.sleep(1)

# Iniciar o servidor
server.start()
print("Servidor OPC UA iniciado na URL opc.tcp://0.0.0.0:4840/freeopcua/server/")

# Iniciar a atualização das tags em uma thread separada
thread = threading.Thread(target=update_tags)
thread.start()

try:
    # Manter o servidor rodando
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    # Parar o servidor em caso de interrupção
    print("Servidor OPC UA interrompido")
    server.stop()
