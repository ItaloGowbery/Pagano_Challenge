from opcua import ua, Server
import random
import time

# Configurar o servidor OPC UA
server = Server()
server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")
name = "OPCUA_SIMULATION_SERVER"
addspace = server.register_namespace(name)

# Objeto de nó
node = server.get_objects_node()

# Variáveis simuladas
myobj = node.add_object(addspace, "MyObject")
counter = myobj.add_variable(addspace, "Counter", 0)
random_30_60 = myobj.add_variable(addspace, "Random30_60", 0)
random_0_1000 = myobj.add_variable(addspace, "Random0_1000", 0)
random_0_5000 = myobj.add_variable(addspace, "Random0_5000", 0)

# Tornar as variáveis graváveis
counter.set_writable()
random_30_60.set_writable()
random_0_1000.set_writable()
random_0_5000.set_writable()

# Iniciar o servidor
server.start()
print("Servidor OPC UA iniciado em opc.tcp://0.0.0.0:4840/freeopcua/server/")

try:
    while True:
        counter.set_value(counter.get_value() + 1)
        random_30_60.set_value(random.randint(30, 60))
        random_0_1000.set_value(random.randint(0, 1000))
        random_0_5000.set_value(random.randint(0, 5000))
        time.sleep(5)
except KeyboardInterrupt:
    print("Interrupção pelo usuário")
finally:
    server.stop()
    print("Servidor OPC UA parado")
