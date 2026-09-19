import asyncio
import math
import random

from asyncua import Server

URL = "opc.tcp://0.0.0.0:4840/freeopcua/server/"
NAMESPACE = "http://pipeline.local/simulator"
PERIOD = 1.0

TAGS = {
    "Temperature": lambda t: 25 + 5 * math.sin(t / 10) + random.uniform(-0.3, 0.3),
    "Pressure":    lambda t: 1.0 + 0.2 * math.cos(t / 7) + random.uniform(-0.02, 0.02),
    "FlowRate":    lambda t: max(0.0, 12 + 3 * math.sin(t / 4) + random.uniform(-0.5, 0.5)),
    "Level":       lambda t: 50 + 20 * math.sin(t / 25),
}


async def main():
    server = Server()
    await server.init()
    server.set_endpoint(URL)
    server.set_server_name("Pipeline Simulator")

    idx = await server.register_namespace(NAMESPACE)
    plant = await server.nodes.objects.add_object(idx, "Plant")

    nodes = {}
    for name in TAGS:
        var = await plant.add_variable(idx, name, 0.0)
        await var.set_writable()
        nodes[name] = var
        print(f"tag criada: {var.nodeid.to_string()}")

    async with server:
        print(f"servidor no ar em {URL}")
        t = 0.0
        while True:
            for name, fn in TAGS.items():
                await nodes[name].write_value(float(fn(t)))
            t += PERIOD
            await asyncio.sleep(PERIOD)


if __name__ == "__main__":
    asyncio.run(main())
