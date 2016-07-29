
_LEGACY_MSG_TYPES_LIST = [
    {
        'type': 1,
        'name': 'PING',
        'comment': "I'm alive packet; it never carries any significant message."
    }, {
        'type': 2,
        'name': 'CONNECTION_WELCOME',
        'comment': "message interchange by peers just after connecting to each other"
    }, {
        'type': 3,
        'name': "BACKEND_FOR_PACKET_SEARCH",
        'comment': "message used by MX client in query() for finding a backend that would handle its request"
    }, {
        'type': 4,
        'name': "HEARTBIT",
        'comment': "packet to be sent by every peer on every channel when nothing sent through the given "
                   "channel for certain period of time"
    }, {
        'type': 5,
        'name': "DELIVERY_ERROR",
        'comment': "packet could not be delivered to one or more recipients"
    }, {
        'type': 99,
        'name': "MAX_MULTIPLEXER_META_PACKET",
        'comment': "this only defines a constant"
    },

    # PACKAGES 100 - 999 normal package
    # types 100-109 reserved
    {
        'type': 110,
        'name': "PYTHON_TEST_REQUEST"
    }, {
        'type': 111,
        'name': "PYTHON_TEST_RESPONSE"
    }, {
        'type': 112,
        'name': "PICKLE_RESPONSE"
    }, {
        'type': 113,
        'name': "REQUEST_RECEIVED",
        'comment': "packet sent by the backend immediatelly after receiving a request and stright to the "
                   "requesting peer"
    }, {
        'type': 114,
        'name': "BACKEND_ERROR",
        'comment': "packet sent by the backend when request handling function finishes and no packet response "
                   "packet is sent"
    }, {
        'type': 115,
        'name': "LOGS_STREAM",
        'comment': "payload is LogEntriesMessage"
    }, {
        # currently unused
        'type': 116,
        'name': "LOGS_STREAM_RESPONSE"
    }, {
        'type': 117,
        'name': "SEARCH_COLLECTED_LOGS_REQUEST",
        'comment': "payload is SearchCollectedLogs; logs are returned in LogEntriesMessage"
    }, {
        'type': 118,
        'name': "SEARCH_COLLECTED_LOGS_RESPONSE"
    },

    # types 119-125 reserved
    {
        'type': 126,
        'name': "REPLAY_EVENTS_REQUEST",
        'comment': "this is a no-response request"
    },

    # types 127-128 reserverd
    {
        'type': 129,
        'name': "AMPLIFIER_SIGNAL_MESSAGE"
    }, {
        'type': 130,
        'name': "FILTERED_SIGNAL_MESSAGE"
    }, {
        'type': 131,
        'name': "SIGNAL_CATCHER_REQUEST_MESSAGE"
    }, {
        'type': 132,
        'name': "SIGNAL_CATCHER_RESPONSE_MESSAGE"
    }, {
        'type': 133,
        'name': "DICT_GET_REQUEST_MESSAGE"
    }, {
        'type': 134,
        'name': "DICT_GET_RESPONSE_MESSAGE"
    }, {
        'type': 135,
        'name': "DICT_SET_MESSAGE"
    }, {
        'type': 136,
        'name': "DECISION_MESSAGE"
    }, {
        'type': 137,
        'name': "DIODE_MESSAGE"
    }, {
        'type': 138,
        'name': "DIODE_REQUEST"
    }, {
        'type': 139,
        'name': "DIODE_RESPONSE"
    }, {
        'type': 140,
        'name': "P300_DECISION_MESSAGE"
    }, {
        'type': 141,
        'name': "SSVEP_DECISION_MESSAGE"
    }, {
        'type': 142,
        'name': "SWITCH_MODE"
    }, {
        'type': 143,
        'name': "STREAMED_SIGNAL_MESSAGE"
    }, {
        'type': 144,
        'name': "SIGNAL_STREAMER_START"
    }, {
        'type': 145,
        'name': "SIGNAL_STREAMER_STOP"
    }, {
        'type': 146,
        'name': "SAMPLING_FREQUENCY"
    }, {
        'type': 147,
        'name': "CALIBRATION"
    }, {
        'type': 149,
        'name': "UGM_UPDATE_MESSAGE"
    }, {
        'type': 150,
        'name': "TAG"
    }, {
        'type': 151,
        'name': "DIODE_CONTROL_MESSAGE"
    }, {
        'type': 152,
        'name': "TAG_CATCHER_REQUEST_MESSAGE"
    }, {
        'type': 153,
        'name': "TAG_CATCHER_RESPONSE_MESSAGE"
    }, {
        'type': 154,
        'name': "BLINK_MESSAGE"
    }, {
        'type': 155,
        'name': "ACQUISITION_CONTROL_MESSAGE"
    }, {
        'type': 156,
        'name': "SIGNAL_SAVER_FINISHED"
    }, {
        'type': 157,
        'name': "INFO_SAVER_FINISHED"
    }, {
        'type': 158,
        'name': "TAG_SAVER_FINISHED"
    }, {
        'type': 159,
        'name': "BLINK_VECTOR_MESSAGE"
    }, {
        'type': 160,
        'name': "HAPTIC_CONTROL_MESSAGE"
    }, {
        'type': 209,
        'name': "ETR_SIGNAL_MESSAGE"
    }, {
        'type': 210,
        'name': "SYSTEM_CONFIGURATION"
    }, {
        'type': 211,
        'name': "UGM_ENGINE_MESSAGE"
    }, {
        'type': 212,
        'name': "UGM_CONTROL_MESSAGE"
    },

    # CONFIGURATION MESSAGES
    {
        # TODO get rid of this type
        'type': 213,
        'name': "CONFIG_MESSAGE"
    }, {
        # message contains ConfigParamsRequest message
        'type': 214,
        'name': "GET_CONFIG_PARAMS"
    }, {
        # message contains ConfigParams message
        'type': 215,
        'name': "CONFIG_PARAMS"
    }, {
        # message contains ConfigParams message
        'type': 216,
        'name': "REGISTER_PEER_CONFIG"
    }, {
        # message contains peer_id (string)
        'type': 217,
        'name': "PEER_REGISTERED"
    }, {
        # message contains ConfigParams message
        'type': 218,
        'name': "UPDATE_PARAMS"
    }, {
        # message contains ConfigParams message
        'type': 219,
        'name': "PARAMS_CHANGED"
    }, {
        # message contains peer_id (string)
        'type': 220,
        'name': "PEER_READY"
    }, {
        # PeerReadyQuery
        'type': 221,
        'name': "PEERS_READY_QUERY"
    }, {
        # PeerReadyStatus
        'type': 222,
        'name': "READY_STATUS"
    }, {
        # message contains PeerIdentity
        'type': 223,
        'name': "UNREGISTER_PEER_CONFIG"
    }, {
        # ConfigError
        'type': 224,
        'name': "CONFIG_ERROR"
    }, {
        # PeerIdentity
        'type': 225,
        'name': "PEER_READY_SIGNAL"
    }, {
        'type': 226,
        'name': "SHUTDOWN_REQUEST"
    }, {
        # cfg_templates.LauncherCommand
        'type': 227,
        'name': "LAUNCHER_COMMAND"
    }, {
        'type': 228,
        'name': "SWITCH_MESSAGE"
    }, {
        'type': 229,
        'name': "ROBOT_FEEDBACK_CONTROL"
    }, {
        'type': 231,
        'name': "P300_ANALYSIS_RESULTS"
    }, {
        'type': 232,
        'name': "ETR_ANALYSIS_RESULTS"
    }, {
        'type': 233,
        'name': "ETR_CALIBRATION_RESULTS"
    },

    # OBCI_LOGGING
    {
        'type': 234,
        'name': "OBCI_LOG_MESSAGE"
    }, {
        'type': 235,
        'name': "OBCI_LOG_DUMP_REQUEST"
    }, {
        'type': 236,
        'name': "OBCI_LOG_DUMP_RESPONSE"
    }, {
        'type': 240,
        'name': "TOBII_SIGNAL_SAVER_FINISHED"
    }, {
        'type': 241,
        'name': "TOBII_INFO_SAVER_FINISHED"
    }, {
        'type': 242,
        'name': "TOBII_TAG_SAVER_FINISHED"
    }, {
        'type': 243,
        'name': "TOBII_SIGNAL_MESSAGE"
    }, {
        'type': 250,
        'name': "WII_BOARD_SIGNAL_SAVER_FINISHED"
    }, {
        'type': 251,
        'name': "WII_BOARD_INFO_SAVER_FINISHED"
    }, {
        'type': 252,
        'name': "WII_BOARD_TAG_SAVER_FINISHED"
    }, {
        'type': 253,
        'name': "WII_BOARD_SIGNAL_MESSAGE"
    }, {
        'type': 254,
        'name': "WII_BOARD_ANALYSIS_RESULTS"
    }
]


LEGACY_MSG_TYPES = {}
for t in _LEGACY_MSG_TYPES_LIST:
    # legacy messages have code in 0xFFXX format,
    # where XX is the original code from OpenBCI 1
    code = 0xFF00 | t['type']
    LEGACY_MSG_TYPES[code] = 'LEGACY_' + t['name']


def print_legacy_msg_types():
    keys = sorted(LEGACY_MSG_TYPES.keys())
    for k in keys:
        print('{} = {}'.format(k, LEGACY_MSG_TYPES[k]))
