import time
from opcua import Client


def NomeDoNo(x):
    x = str(x)

    if(x == 'ns=3;i=1002'):
        return 'counter'
    if(x == 'ns=3;i=1003'):
        return 'random'
    if(x == 'ns=3;i=1004'):
        return 'sawtooth'        
    if(x == 'ns=3;i=1005'):
        return 'sinusoid'
    if(x == 'ns=3;i=1006'):
        return 'square'
    if(x == 'ns=3;i=1007'):
        return 'triangle'

        
# URL do seu servidor OPC UA
url = "opc.tcp://DESKTOP-AE3RLGJ.mshome.net:53530/OPCUA/SimulationServer"

# Cria uma instância do cliente e conecta ao servidor
client = Client(url)
client.connect()

print("Conectado ao servidor OPC UA")

try:
    # Lista de NodeIds dos nós que queremos ler
    node_ids = [
        "ns=3;i=1002",
        "ns=3;i=1003",
        "ns=3;i=1004",
        "ns=3;i=1005",
        "ns=3;i=1006",
        "ns=3;i=1007"
    ]
    
    while True:
        print("Valores dos nós:")
        for node_id in node_ids:
            try:
                myvar = client.get_node(node_id)
                value = myvar.get_value()
                
                nome = NomeDoNo(node_id)
                print(f"Valor do nó {nome}: {value}")
            except Exception as e:
                print(f"Erro ao ler o nó {node_id}: {e}")
        
        # Espera 5 segundos antes de ler novamente
        time.sleep(1)
        print('')

finally:
    # Desconecta do servidor
    client.disconnect()
    print("Desconectado do servidor OPC UA")

