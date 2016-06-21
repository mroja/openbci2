
# Broker URL: tcp://127.0.0.1:10000

# Peer URLs:
# 0) tcp://127.0.0.1:20000 - broker
# 1) tcp://127.0.0.1:20001 - sender
# 2) tcp://127.0.0.1:20002 - receiver

xterm -e python3 broker.py --broker-bind tcp://127.0.0.1:10000 --bind tcp://127.0.0.1:20000 --peers tcp://127.0.0.1:20001 tcp://127.0.0.1:20002 &

xterm -e python3 test_sender_peer.py --id 1 --bind tcp://127.0.0.1:20001 --broker tcp://127.0.0.1:10000 --peers tcp://127.0.0.1:20000 tcp://127.0.0.1:20002 &

xterm -e python3 test_receiver_peer.py --id 2 --bind tcp://127.0.0.1:20002 --broker tcp://127.0.0.1:10000 --peers tcp://127.0.0.1:20000 tcp://127.0.0.1:20001 &

