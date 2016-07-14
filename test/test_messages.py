
import pytest

from obci2.core.messages import (Message,
                                 NullMessageSerializer,
                                 StringMessageSerializer,
                                 JsonMessageSerializer)

Message.register_serializer('NULL', NullMessageSerializer)
Message.register_serializer('STRING', StringMessageSerializer)
Message.register_serializer('JSON', JsonMessageSerializer)


def check_round_trip(msg_type, subtype, payload):
    msg = Message(msg_type, subtype, payload)
    assert isinstance(msg.type, str)
    assert isinstance(msg.subtype, str)
    msg_serialized = msg.serialize()
    assert all(isinstance(x, bytes) for x in msg_serialized)
    msg_deserialized = Message.deserialize(msg_serialized)
    assert msg_deserialized.type == msg.type
    assert msg_deserialized.subtype == msg.subtype
    if (msg.data is None
            or isinstance(msg.data, bytes)
            or isinstance(msg.data, str)
            or isinstance(msg.data, dict)
            ):
        assert msg_deserialized.data == msg.data
    else:
        raise Exception("don't know how to compare playloads for equality")
    return True


def test_1():
    msg = Message('STRING', 123, 'abc')
    with pytest.raises(AttributeError):
        msg.type = 'NULL'
    assert msg.subtype == '123'
    msg.subtype = 321
    assert msg.subtype == '321'
    msg.data = 'cba'
    assert msg.data == 'cba'


def test_2():
    unicode_1 = 'ą, ć, ę, ł, ń, ó, ś, ź, ż, Ą, Ć, Ę, Ł, Ń, Ó, Ś, Ź, Ż'
    unicode_2 = 'Ё Ђ Ѓ Є Ѕ І Ї Ј Љ Њ Ћ Ќ Ў Џ А Б В Г Д Е Ж З И Й К Л М Н О П Р С Т У Ф Х Ц Ч Ш Щ Ъ Ы Ь Э Ю Я а б в г д е ж з и й к л м н о п р с т у ф х ц ч ш щ ъ ы ь э ю я ё ђ ѓ є ѕ і ї ј љ њ ћ ќ ў џ Ѡ ѡ Ѣ ѣ Ѥ ѥ Ѧ ѧ Ѩ ѩ Ѫ ѫ Ѭ ѭ Ѯ ѯ Ѱ ѱ Ѳ ѳ Ѵ ѵ Ѷ ѷ Ѹ ѹ Ѻ ѻ Ѽ ѽ Ѿ ѿ Ҁ ҁ ҂ ҃ ...'
    unicode_3 = '子曰：「學而時習之，不亦說乎？有朋自遠方來，不亦樂乎？人不知而不慍，不亦君子乎？」'
    assert check_round_trip('NULL', 0, None)
    assert check_round_trip('NULL', '0', None)
    assert check_round_trip('NULL', 123, None)
    assert check_round_trip('NULL', '123', None)

    assert check_round_trip('STRING', 0, '')
    assert check_round_trip('STRING', 0, ' ')
    assert check_round_trip('STRING', 0, 'abc')
    assert check_round_trip('STRING', 0, unicode_1)
    assert check_round_trip('STRING', 0, unicode_2)
    assert check_round_trip('STRING', 0, unicode_3)

    json = {
        'a': 1, 'b': 2, 'c': 'abc', 'd': 1.5,
        'f': {'a': 10, 'b': 11},
        'true': True, 'false': False,
        'null': None,
        'array_1': [1, 2, 3, 4],
        'array_1a': ['1', '2', '3', '4'],
        'array_2': [1.5, 2.5, 3.5, 4.5],
        'array_3': [{'a': 'a'}, {'b': 'b'}],
        'unicode_1': unicode_1,
        'unicode_2': unicode_2,
        'unicode_3': unicode_3
    }
    assert check_round_trip('JSON', 0, json)


if __name__ == '__main__':
    test_1()
    test_2()
