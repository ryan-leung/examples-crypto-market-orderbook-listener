#import msgpack
#import msgpack_numpy

#class Packer:
#    @staticmethod
#    def loads(data):
#        return msgpack.unpackb(data, object_hook=msgpack_numpy.decode, encoding="utf-8")
#
#   @staticmethod
#    def dumps(data):
#        return msgpack.packb(data, default=msgpack_numpy.encode, use_bin_type=True)
import pyarrow

get_topics = lambda x, y: "/%s/%s/orderbook" % (x, y)

class Packer:
    @staticmethod
    def loads(serialized):
        return pyarrow.deserialize(serialized)

    @staticmethod
    def dumps(deserialized):
        return pyarrow.serialize(deserialized).to_buffer()