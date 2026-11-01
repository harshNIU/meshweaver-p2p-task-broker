"""Start a persistent DHT node for a terminal or multi-device demonstration."""

import argparse
import asyncio
import json

from dht import DHTNode


async def run(args):
    node = await DHTNode.start(
        args.host, args.port, advertise_host=args.advertise_host, refresh_interval=5
    )
    try:
        if args.bootstrap:
            host, port = args.bootstrap.rsplit(":", 1)
            await node.join((host, int(port)))
        print(json.dumps({"self": node.contact.to_dict()}), flush=True)
        while True:
            await node.lookup(node.node_id)
            print(
                json.dumps(
                    {
                        "peer_count": len(node.routing),
                        "peers": [c.to_dict() for c in node.routing.contacts()],
                    }
                ),
                flush=True,
            )
            await asyncio.sleep(5)
    finally:
        await node.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--advertise-host")
    parser.add_argument("--bootstrap", help="One seed, e.g. 127.0.0.1:9000")
    try:
        asyncio.run(run(parser.parse_args()))
    except KeyboardInterrupt:
        pass
