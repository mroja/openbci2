
import os

receivers_number = 10


def run(cmd):
    print(cmd)
    os.system(cmd)

def get_peers(without):
    peers = []
    for i in range(receivers_number + 2):
        if i == without:
            continue
        else:
            peers.append('tcp://127.0.0.1:{}'.format(20000 + i))
    return ' '.join(peers)

run('xterm -e python3 broker.py --broker-bind tcp://127.0.0.1:10000 --bind tcp://127.0.0.1:20000 --peers ' + get_peers(0) + ' &')

run('xterm -e python3 test_sender_peer.py --id 1 --bind tcp://127.0.0.1:20001 --broker tcp://127.0.0.1:10000 --peers ' + get_peers(1) + ' &')

peer_id = 2
for i in range(receivers_number):
    bind = 'tcp://127.0.0.1:{}'.format(20000 + peer_id)    
    run('xterm -e python3 test_receiver_peer.py --id '+ str(peer_id) +' --bind ' + bind + ' --broker tcp://127.0.0.1:10000 --peers ' + get_peers(peer_id) + ' &')    
    peer_id += 1

