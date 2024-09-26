import aerospike
import time
from testcontainers.core.container import DockerContainer, Network
from testcontainers.core.waiting_utils import wait_for_logs
from pathlib import Path

AS_IMAGE="aerospike/aerospike-server:7.1.0.2"
AS_CONF_DIR=Path(__file__).parent / 'etc'

def get_as_container(name: str, network: Network, base_port: int) -> DockerContainer:
    d = DockerContainer(image=AS_IMAGE)
    d.with_name(name)
    d.with_volume_mapping(host=str(AS_CONF_DIR), container="/etc/aerospike")
    d.with_network(network)
    d.with_bind_ports(3000, base_port)
    d.with_bind_ports(3001, base_port + 1)
    d.with_bind_ports(3002, base_port + 2)
    d.with_bind_ports(3003, base_port + 3)
    return d


def main():
    with Network() as network:
        with get_as_container("as_1", network, 3000) as as_1:
            with get_as_container("as_2", network, 6000) as as_2:
                wait_for_logs(as_1, "CLUSTER-SIZE 1 CLUSTER-NAME docker")
                wait_for_logs(as_2, "CLUSTER-SIZE 1 CLUSTER-NAME docker")


                client = aerospike.client(config={
                    'hosts': [ ('127.0.0.1', 3000) ],
                }).connect()

                # write few records filling two write blocks
                test_key = ('test', 'test', 'key1')
                test_key_digest = aerospike.calc_digest(ns=test_key[0], set=test_key[1], key=test_key[2])
                print("Inserting few records")
                client.put(key=test_key, bins={'v': "a"*970*1024})
                for i in range(2, 10):
                    client.put(key=('test', 'test', f'key{i}'), bins={'v': f"{i}"*970*1024})

                # wait for records to be written to disk
                time.sleep(5)

                # extract 1st record location
                exit_code, output = as_1.exec(f"asinfo -v debug-record:namespace=test;keyd={test_key_digest.hex()}")
                print((exit_code, output.decode()[0:500]))
                if exit_code != 0:
                    raise Exception("Unable to dump record")

                rblock_id = -1
                file_name = None
                for meta in output.decode().split(','):
                    if meta.startswith('rblock-id='):
                        rblock_id = int(meta.split("=")[1])
                    if meta.startswith('file-name='):
                        file_name = meta.split("=")[1]
                if rblock_id < 0 or file_name is None:
                    raise Exception(f"Unable to resolve record location ({rblock_id=}, {file_name=})")

                input("Press a key to simulate record corruption")

                # Simulate disk corruption by zeroing record magic field
                record_offset = rblock_id << 4
                print(f"Overriding {test_key_digest.hex()} record magic at offset={record_offset} of {file_name}")
                exit_code, output = as_1.exec(f"dd if=/dev/zero of={file_name} seek={record_offset} bs=4 count=1 oflag=dsync,seek_bytes conv=notrunc")
                print((exit_code, output.decode()))

                time.sleep(1)

                # get ip of second node
                print("Forming cluster with second to trigger rebalancing")
                exit_code, output = as_2.exec("asinfo -v service")
                if exit_code != 0:
                    raise Exception("Unable to determine ip of second aerospike node")
                as_2_ip = output.strip().split(b":")[0].decode()

                # form cluster with second node
                # it will trigger migrations to balance records across the two nodes
                exit_code, output = as_1.exec(f"asinfo -v 'tip:host={as_2_ip};port=3002'")
                print((exit_code, output))
                input("Press a key to stop")

                # migration of the first record from first node to second node should be blocked
                # because first node is not able to load record from disk

                #
                # On first node (as_1), migration code is failing to record from disk (expected due to data corruption)
                #
                # WARNING (drv_ssd): (drv_ssd.c:1098) {test} read /opt/aerospike/data/test.dat: digest 1c4acea7d4566aef2bdf4057a5d86f8d3ac9f4de bad magic 0x0 offset 8388608
                # WARNING (drv_ssd): (drv_ssd.c:1251) {test} read /opt/aerospike/data/test.dat: digest 1c4acea7d4566aef2bdf4057a5d86f8d3ac9f4de failed read directly from device
                # WARNING (migrate): (migrate.c:895) unreadable digest 1c4acea7d4566aef2bdf4057a5d86f8d3ac9f4de
                #
                # But then as_1 is stuck in a retransmit loop that never stop, blocking all migrations (unexpected)
                # WARNING (migrate): (migrate.c:968) unreadable digest 1c4acea7d4566aef2bdf4057a5d86f8d3ac9f4de
                # WARNING (migrate): (migrate.c:968) unreadable digest 1c4acea7d4566aef2bdf4057a5d86f8d3ac9f4de
                # ...
                # WARNING (migrate): (migrate.c:968) unreadable digest 1c4acea7d4566aef2bdf4057a5d86f8d3ac9f4de

                #
                # as_2 will constantly reject the record and will never acknowledge insertion
                # WARNING (migrate): (migrate.c:1360) handle insert: got no record
                # WARNING (migrate): (migrate.c:1360) handle insert: got no record
                # ...
                # WARNING (migrate): (migrate.c:1360) handle insert: got no record

if __name__ == "__main__":
    main()
